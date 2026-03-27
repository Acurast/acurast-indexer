use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::{get, post},
    Json, Router,
};
use moka::future::Cache;
use serde::Deserialize;
use sqlx::{PgPool, Pool, Postgres};
use std::net::SocketAddr;
use std::time::Duration;
use subxt::{OnlineClient, PolkadotConfig};
use tokio_util::sync::CancellationToken;
use tower_http::{
    cors::CorsLayer,
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};

use serde::Serialize;

use crate::task_monitor::{QueueMetrics, TASK_REGISTRY};

use crate::{
    routes,
    rpc_server::{RpcError, RpcResult},
};

/// Convert a serializable value to JSON, returning an RPC error on failure
fn to_json<T: Serialize>(value: T) -> RpcResult<serde_json::Value> {
    serde_json::to_value(value).map_err(|e| RpcError::internal_error(e.to_string()))
}

#[derive(Clone)]
pub struct AppState {
    pub db_pool: PgPool,
    pub client: OnlineClient<PolkadotConfig>,
    pub query_timeout: Duration,
    /// Cache for count queries (key: serialized params, value: count)
    /// TTL: 30 seconds, max 1000 entries
    pub count_cache: Cache<String, i64>,
}

pub async fn run(
    db_pool: Pool<Postgres>,
    client: OnlineClient<PolkadotConfig>,
    cancel_token: CancellationToken,
) -> Result<(), anyhow::Error> {
    let settings = crate::config::settings();
    let address =
        format!("{}:{}", settings.server.host, settings.server.port).parse::<SocketAddr>()?;
    // Create count cache with 30 second TTL and max 1000 entries
    let count_cache: Cache<String, i64> = Cache::builder()
        .time_to_live(Duration::from_secs(30))
        .max_capacity(1000)
        .build();

    let state = AppState {
        db_pool,
        client,
        query_timeout: Duration::from_secs(settings.server.query_timeout_seconds),
        count_cache,
    };

    let cors_layer = CorsLayer::permissive();

    // JSON-RPC endpoint with batch support
    let rpc_routes = Router::new()
        .route("/rpc", post(handle_rpc))
        .layer(middleware::from_fn(check_api_key))
        .with_state(state.clone());

    // REST-style catch-all that translates to RPC calls
    // e.g., GET /api/v1/getCommitments?limit=10 or GET /api/v1/get_commitments?limit=10
    let rest_routes = Router::new()
        .route("/{method}", get(handle_rest_to_rpc))
        .layer(middleware::from_fn(check_api_key))
        .with_state(state.clone());

    let health_routes = Router::new().route("/health", get(routes::health::health));

    // Task monitoring endpoint (no auth required, polled by UI)
    let task_routes = Router::new()
        .route("/tasks", get(get_tasks))
        .route("/queue-metrics", get(get_queue_metrics));

    // Serve React frontend from frontend/dist
    // Falls back to index.html for SPA client-side routing
    let frontend_service = ServeDir::new("frontend/dist")
        .not_found_service(ServeFile::new("frontend/dist/index.html"));

    let app = Router::new()
        .nest(
            "/api/v1",
            Router::new()
                .merge(rpc_routes)
                .merge(health_routes)
                .merge(task_routes)
                .merge(rest_routes),
        )
        .fallback_service(frontend_service)
        .layer(cors_layer)
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    tracing::info!("Listening on http://{address}");
    tracing::info!("Dashboard: http://{address}/");
    tracing::info!("JSON-RPC endpoint: http://{address}/api/v1/rpc");
    let listener = tokio::net::TcpListener::bind(&address).await?;
    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(cancel_token.cancelled_owned())
        .await?;

    Ok(())
}

/// JSON-RPC request/response handling with batch support
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum BatchOrSingle {
    // Batch must come first - serde tries variants in order with untagged,
    // and Value can deserialize from anything including arrays
    Batch(Vec<serde_json::Value>),
    Single(serde_json::Value),
}

async fn handle_rpc(
    State(rpc_server): State<AppState>,
    Json(body): Json<BatchOrSingle>,
) -> Json<serde_json::Value> {
    match body {
        BatchOrSingle::Batch(reqs) => {
            let mut responses = Vec::new();
            for req in reqs {
                let response = process_single_request(req, &rpc_server).await;
                responses.push(response);
            }
            Json(serde_json::json!(responses))
        }
        BatchOrSingle::Single(req) => {
            let response = process_single_request(req, &rpc_server).await;
            Json(response)
        }
    }
}

async fn process_single_request(
    req: serde_json::Value,
    rpc_server: &AppState,
) -> serde_json::Value {
    // Parse the JSON-RPC request
    let method = req.get("method").and_then(|m| m.as_str()).unwrap_or("");
    let params = req.get("params").cloned().unwrap_or(serde_json::json!({}));
    let id = req.get("id").cloned().unwrap_or(serde_json::json!(null));

    // Route to appropriate method
    let result = match method {
        "getBlock" => {
            let hash: String = serde_json::from_value(params).unwrap_or_default();
            rpc_server.get_block(hash).await.and_then(to_json)
        }
        "getBlocks" => {
            let params: crate::rpc_server::GetBlocksParams =
                serde_json::from_value(params).unwrap_or_default();
            rpc_server.get_blocks(params).await.and_then(to_json)
        }
        "getBlocksCount" => {
            let params: crate::rpc_server::GetBlocksCountParams =
                serde_json::from_value(params).unwrap_or_default();
            rpc_server.get_blocks_count(params).await.and_then(to_json)
        }
        "getExtrinsic" => {
            match serde_json::from_value::<crate::rpc_server::GetExtrinsicParams>(params) {
                Ok(p) => rpc_server
                    .get_extrinsic(p.block_number, p.index, p.events.unwrap_or(false))
                    .await
                    .and_then(to_json),
                Err(e) => Err(RpcError::from(e)),
            }
        }
        "getExtrinsicByHash" => {
            let params: crate::rpc_server::GetExtrinsicByHashParams =
                serde_json::from_value(params).unwrap_or_default();
            rpc_server
                .get_extrinsic_by_hash(params.tx_hash, params.events.unwrap_or(false))
                .await
                .and_then(to_json)
        }
        "getExtrinsics" => {
            let params: crate::rpc_server::GetExtrinsicsParams =
                serde_json::from_value(params).unwrap_or_default();
            rpc_server.get_extrinsics(params).await.and_then(to_json)
        }
        "getExtrinsicsCount" => {
            let params: crate::rpc_server::GetExtrinsicsCountParams =
                serde_json::from_value(params).unwrap_or_default();
            rpc_server
                .get_extrinsics_count(params)
                .await
                .and_then(to_json)
        }
        "getExtrinsicMetadata" => rpc_server.get_extrinsic_metadata().await.and_then(to_json),
        "getEventMetadata" => rpc_server.get_event_metadata().await.and_then(to_json),
        "getSpecVersion" => {
            match serde_json::from_value::<crate::rpc_server::GetSpecVersionParams>(params) {
                Ok(p) => rpc_server.get_spec_version(p).await.and_then(to_json),
                Err(e) => Err(RpcError::invalid_params(e.to_string())),
            }
        }
        "getExtrinsicAddresses" => {
            let params: crate::rpc_server::GetExtrinsicAddressesParams =
                serde_json::from_value(params).unwrap_or_default();
            rpc_server
                .get_extrinsic_addresses(params)
                .await
                .and_then(to_json)
        }
        "getEvent" => match serde_json::from_value(params) {
            Ok(p) => rpc_server.get_event(p).await.and_then(to_json),
            Err(e) => Err(RpcError::from(e)),
        },
        "getEvents" => match serde_json::from_value(params) {
            Ok(p) => rpc_server.get_events(p).await.and_then(to_json),
            Err(e) => Err(RpcError::from(e)),
        },
        "getJobs" => {
            let params: crate::rpc_server::GetJobsParams =
                serde_json::from_value(params).unwrap_or_default();
            rpc_server.get_jobs(params).await.and_then(to_json)
        }
        "getStorageSnapshots" => {
            let params: crate::rpc_server::GetStorageSnapshotsParams =
                serde_json::from_value(params).unwrap_or_default();
            rpc_server
                .get_storage_snapshots(params)
                .await
                .and_then(to_json)
        }
        "getEpochs" => {
            let params: crate::rpc_server::GetEpochsParams =
                serde_json::from_value(params).unwrap_or_default();
            rpc_server.get_epochs(params).await.and_then(to_json)
        }
        "getProcessorsCountByEpoch" => {
            match serde_json::from_value::<crate::rpc_server::GetProcessorsCountByEpochParams>(
                params,
            ) {
                Ok(p) => rpc_server
                    .get_processors_count_by_epoch(p)
                    .await
                    .and_then(to_json),
                Err(e) => Err(RpcError::from(e)),
            }
        }
        "getMetricsByManager" => {
            let params: crate::rpc_server::GetEpochMetricsParams =
                serde_json::from_value(params).unwrap_or_default();
            rpc_server
                .get_metrics_by_manager(params)
                .await
                .and_then(to_json)
        }
        "getMetricsByProcessor" => {
            let params: crate::rpc_server::GetProcessorMetricsParams =
                serde_json::from_value(params).unwrap_or_default();
            rpc_server
                .get_metrics_by_processor(params)
                .await
                .and_then(to_json)
        }
        "getCommitments" => {
            let params: crate::rpc_server::GetCommitmentsParams =
                serde_json::from_value(params).unwrap_or_default();
            rpc_server.get_commitments(params).await.and_then(to_json)
        }
        _ => Err(RpcError::method_not_found(method)),
    };

    // Build JSON-RPC response
    match result {
        Ok(result) => serde_json::json!({
            "jsonrpc": "2.0",
            "result": result,
            "id": id
        }),
        Err(err) => serde_json::json!({
            "jsonrpc": "2.0",
            "error": {
                "code": err.code(),
                "message": err.message()
            },
            "id": id
        }),
    }
}

async fn check_api_key(req: Request<Body>, next: Next) -> Result<Response, StatusCode> {
    let auth = &crate::config::settings().auth;
    // Check for the presence of the "API-Key" header
    if let Some(api_key) = req.headers().get("API-Key") {
        if api_key == &auth.api_key {
            return Ok(next.run(req).await);
        }
    }

    // Return 401 Unauthorized if the header is missing or invalid
    Err(StatusCode::UNAUTHORIZED)
}

/// HTTP endpoint for task monitoring (polled by UI every 10 seconds)
async fn get_tasks() -> Json<Vec<crate::task_monitor::TaskInfo>> {
    Json(TASK_REGISTRY.get_all())
}

/// HTTP endpoint for queue metrics (polled by UI)
async fn get_queue_metrics() -> Json<QueueMetrics> {
    Json(TASK_REGISTRY.get_queue_metrics())
}

/// Convert snake_case to camelCase
fn snake_to_camel(s: &str) -> String {
    let mut result = String::new();
    let mut capitalize_next = false;
    for c in s.chars() {
        if c == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(c.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            result.push(c);
        }
    }
    result
}

/// REST-style catch-all handler that translates GET requests to RPC calls
/// e.g., GET /api/v1/get_commitments?limit=10 -> RPC getCommitments with {"limit": 10}
async fn handle_rest_to_rpc(
    State(rpc_server): State<AppState>,
    Path(method_path): Path<String>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Json<serde_json::Value> {
    // Convert snake_case to camelCase for method name
    let method = snake_to_camel(&method_path);

    // Convert query params to JSON object, attempting to parse values as appropriate types
    let params_json: serde_json::Value = {
        let mut map = serde_json::Map::new();
        for (key, value) in params {
            // Try to parse as JSON first (handles numbers, booleans, null, objects, arrays)
            let parsed = serde_json::from_str(&value).unwrap_or_else(|_| {
                // If parsing fails, treat as string
                serde_json::Value::String(value)
            });
            map.insert(key, parsed);
        }
        serde_json::Value::Object(map)
    };

    // Build the RPC request
    let rpc_request = serde_json::json!({
        "method": method,
        "params": params_json,
        "id": 1
    });

    // Process via existing RPC handler
    let response = process_single_request(rpc_request, &rpc_server).await;

    // Extract just the result or error (strip JSON-RPC envelope for REST-style response)
    if let Some(result) = response.get("result") {
        Json(result.clone())
    } else if let Some(error) = response.get("error") {
        Json(serde_json::json!({ "error": error }))
    } else {
        Json(response)
    }
}
