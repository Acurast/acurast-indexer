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
    rpc_server::{validate_params, ProcessorChurnResponse, RpcError, RpcResult},
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
    /// Cache for runtime metadata responses keyed by spec_version.
    /// Metadata is immutable per spec_version, so no TTL is needed; this
    /// avoids a live archive-node call (hundreds of KB-MB) per request.
    pub metadata_cache: Cache<i32, serde_json::Value>,
    /// Cache for `getProcessorChurn`, which cannot use `count_cache` because its
    /// value is a struct rather than an `i64`. The default (full-range) call is
    /// what the dashboard sends, and it recomputed from scratch on every request.
    pub churn_cache: Cache<String, ProcessorChurnResponse>,
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

    // The `active` half only moves as the churn collector advances (60 s passes)
    // and `onboarded` only when a new processor onboards, so a few minutes of
    // staleness is well inside tolerance. Few distinct (from, to) ranges are ever
    // requested, hence the small capacity.
    let churn_cache: Cache<String, ProcessorChurnResponse> = Cache::builder()
        .time_to_live(Duration::from_secs(300))
        .max_capacity(256)
        .build();

    // Metadata is immutable per spec_version; entries never expire.
    let metadata_cache: Cache<i32, serde_json::Value> =
        Cache::builder().max_capacity(10_000).build();

    let state = AppState {
        db_pool,
        client,
        query_timeout: Duration::from_secs(settings.server.query_timeout_seconds),
        count_cache,
        metadata_cache,
        churn_cache,
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

    // Apply TraceLayer only to routes that need tracing (not health endpoint)
    let traced_routes = Router::new()
        .merge(rpc_routes)
        .merge(task_routes)
        .merge(rest_routes)
        .layer(TraceLayer::new_for_http());

    let app = Router::new()
        .nest(
            "/api/v1",
            Router::new().merge(traced_routes).merge(health_routes),
        )
        .fallback_service(frontend_service)
        .layer(cors_layer)
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

/// Maximum number of requests allowed in a single JSON-RPC batch.
/// Without a cap, one HTTP request (within the 2 MiB body limit) can carry
/// tens of thousands of sub-requests, each holding a pooled DB connection.
const MAX_BATCH_SIZE: usize = 100;

async fn handle_rpc(
    State(rpc_server): State<AppState>,
    Json(body): Json<BatchOrSingle>,
) -> Json<serde_json::Value> {
    match body {
        BatchOrSingle::Batch(reqs) => {
            if reqs.len() > MAX_BATCH_SIZE {
                return Json(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": null,
                    "error": RpcError::invalid_params(format!(
                        "Batch size {} exceeds maximum of {}",
                        reqs.len(),
                        MAX_BATCH_SIZE
                    )),
                }));
            }
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

    // Decode the params object into the target struct, surfacing every
    // validation error at once via `validate_params`. The closure form lets
    // each match-arm stay a single expression while still using `?`.
    let result: RpcResult<serde_json::Value> = (async {
        match method {
            "getBlock" => {
                let hash: String = validate_params(params)?;
                rpc_server.get_block(hash).await.and_then(to_json)
            }
            "getBlocks" => {
                let params: crate::rpc_server::GetBlocksParams = validate_params(params)?;
                rpc_server.get_blocks(params).await.and_then(to_json)
            }
            "getBlocksCount" => {
                let params: crate::rpc_server::GetBlocksCountParams = validate_params(params)?;
                rpc_server.get_blocks_count(params).await.and_then(to_json)
            }
            "getExtrinsic" => {
                let p: crate::rpc_server::GetExtrinsicParams = validate_params(params)?;
                rpc_server
                    .get_extrinsic(p.block_number, p.index, p.events.unwrap_or(false))
                    .await
                    .and_then(to_json)
            }
            "getExtrinsicByHash" => {
                let params: crate::rpc_server::GetExtrinsicByHashParams = validate_params(params)?;
                rpc_server
                    .get_extrinsic_by_hash(params.tx_hash, params.events.unwrap_or(false))
                    .await
                    .and_then(to_json)
            }
            "getExtrinsics" => {
                let params: crate::rpc_server::GetExtrinsicsParams = validate_params(params)?;
                rpc_server.get_extrinsics(params).await.and_then(to_json)
            }
            "getExtrinsicsCount" => {
                let params: crate::rpc_server::GetExtrinsicsCountParams = validate_params(params)?;
                rpc_server
                    .get_extrinsics_count(params)
                    .await
                    .and_then(to_json)
            }
            "getExtrinsicMetadata" => rpc_server.get_extrinsic_metadata().await.and_then(to_json),
            "getEventMetadata" => rpc_server.get_event_metadata().await.and_then(to_json),
            "getSpecVersion" => {
                let p: crate::rpc_server::GetSpecVersionParams = validate_params(params)?;
                rpc_server.get_spec_version(p).await.and_then(to_json)
            }
            "getExtrinsicAddresses" => {
                let params: crate::rpc_server::GetExtrinsicAddressesParams =
                    validate_params(params)?;
                rpc_server
                    .get_extrinsic_addresses(params)
                    .await
                    .and_then(to_json)
            }
            "getEvent" => {
                let p: crate::rpc_server::GetEventParams = validate_params(params)?;
                rpc_server.get_event(p).await.and_then(to_json)
            }
            "getEvents" => {
                let p: crate::rpc_server::GetEventsParams = validate_params(params)?;
                rpc_server.get_events(p).await.and_then(to_json)
            }
            "getEventsCount" => {
                let p: crate::rpc_server::GetEventsCountParams = validate_params(params)?;
                rpc_server.get_events_count(p).await.and_then(to_json)
            }
            "getJobs" => {
                let params: crate::rpc_server::GetJobsParams = validate_params(params)?;
                rpc_server.get_jobs(params).await.and_then(to_json)
            }
            "getStorageSnapshots" => {
                let params: crate::rpc_server::GetStorageSnapshotsParams = validate_params(params)?;
                rpc_server
                    .get_storage_snapshots(params)
                    .await
                    .and_then(to_json)
            }
            "getEpochs" => {
                let params: crate::rpc_server::GetEpochsParams = validate_params(params)?;
                rpc_server.get_epochs(params).await.and_then(to_json)
            }
            "getProcessorsCountByEpoch" => {
                let p: crate::rpc_server::GetProcessorsCountByEpochParams =
                    validate_params(params)?;
                rpc_server
                    .get_processors_count_by_epoch(p)
                    .await
                    .and_then(to_json)
            }
            "getProcessorChurn" => {
                let p: crate::rpc_server::GetProcessorChurnParams = validate_params(params)?;
                rpc_server.get_processor_churn(p).await.and_then(to_json)
            }
            "getMetricsByManager" => {
                let params: crate::rpc_server::GetEpochMetricsParams = validate_params(params)?;
                rpc_server
                    .get_metrics_by_manager(params)
                    .await
                    .and_then(to_json)
            }
            "getMetricsByProcessor" => {
                let params: crate::rpc_server::GetProcessorMetricsParams = validate_params(params)?;
                rpc_server
                    .get_metrics_by_processor(params)
                    .await
                    .and_then(to_json)
            }
            "getCommitments" => {
                let params: crate::rpc_server::GetCommitmentsParams = validate_params(params)?;
                rpc_server.get_commitments(params).await.and_then(to_json)
            }
            "getDeployments" => {
                let params: crate::rpc_server::GetDeploymentsParams = validate_params(params)?;
                rpc_server.get_deployments(params).await.and_then(to_json)
            }
            "getBaseRewards" => {
                let params: crate::rpc_server::GetBaseRewardsParams = validate_params(params)?;
                rpc_server.get_base_rewards(params).await.and_then(to_json)
            }
            "getAccounts" => {
                let p: crate::rpc_server::GetAccountsParams = validate_params(params)?;
                rpc_server.get_accounts(p).await.and_then(to_json)
            }
            "getAccountsCount" => {
                let p: crate::rpc_server::GetAccountsCountParams = validate_params(params)?;
                rpc_server.get_accounts_count(p).await.and_then(to_json)
            }
            "getEpochTotals" => {
                let p: crate::rpc_server::GetEpochTotalsParams = validate_params(params)?;
                rpc_server.get_epoch_totals(p).await.and_then(to_json)
            }
            _ => Err(RpcError::method_not_found(method)),
        }
    })
    .await;

    // Build JSON-RPC response
    match result {
        Ok(result) => serde_json::json!({
            "jsonrpc": "2.0",
            "result": result,
            "id": id
        }),
        Err(err) => {
            let mut error_obj = serde_json::json!({
                "code": err.code(),
                "message": err.message(),
            });
            if let Some(data) = err.data() {
                error_obj["data"] = data.clone();
            }
            serde_json::json!({
                "jsonrpc": "2.0",
                "error": error_obj,
                "id": id
            })
        }
    }
}

/// Constant-time byte comparison so the API-key check does not short-circuit
/// on the first mismatching byte (timing side channel).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

async fn check_api_key(req: Request<Body>, next: Next) -> Result<Response, StatusCode> {
    let auth = &crate::config::settings().auth;
    // Check for the presence of the "API-Key" header
    if let Some(api_key) = req.headers().get("API-Key") {
        if constant_time_eq(api_key.as_bytes(), auth.api_key.as_bytes()) {
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
