use axum::{extract::State, http::StatusCode, response::Json};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::entities::EpochIndexPhase;
use crate::health_state::HEALTH_STATE;
use crate::server::AppState;
use crate::task_monitor::TASK_REGISTRY;

#[derive(Serialize, Deserialize)]
pub struct BlockInfo {
    pub block_number: i64,
    pub block_time: String,
    pub delay_in_seconds: i64,
}

#[derive(Serialize, Deserialize)]
pub struct EpochPhaseInfo {
    pub count: i64,
    pub min: Option<i64>,
    pub max: Option<i64>,
}

#[derive(Serialize, Deserialize)]
pub struct HealthResponse {
    pub live: bool,
    pub ready: bool,
    pub synced: bool,
    pub latest_indexed_block: BlockInfo,
    pub current_block: Option<u32>,
    pub finalized_block: Option<u32>,
    /// Epoch stats by phase (key is phase number as string for JSON compatibility)
    pub epochs: HashMap<String, EpochPhaseInfo>,
    pub tasks: HashMap<String, bool>,
}

pub async fn health(
    State(state): State<AppState>,
) -> Result<(StatusCode, Json<HealthResponse>), (StatusCode, String)> {
    // Get latest block from blocks table
    let latest_block = sqlx::query!(
        r#"
        SELECT block_number, block_time
        FROM blocks
        ORDER BY block_number DESC
        LIMIT 1
        "#
    )
    .fetch_optional(&state.db_pool)
    .await
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to fetch latest block: {}", e),
        )
    })?;

    let latest_indexed_block = match latest_block {
        Some(block) => {
            let delay_in_seconds = Utc::now()
                .signed_duration_since(block.block_time)
                .num_seconds();
            BlockInfo {
                block_number: block.block_number,
                block_time: block.block_time.to_rfc3339(),
                delay_in_seconds,
            }
        }
        None => {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "No blocks indexed yet".to_string(),
            ))
        }
    };

    // Get epoch stats by phase
    let epoch_stats = sqlx::query!(
        r#"
        SELECT
            phase,
            COUNT(*) as count,
            MIN(epoch) as min_epoch,
            MAX(epoch) as max_epoch
        FROM epochs
        GROUP BY phase
        ORDER BY phase
        "#
    )
    .fetch_all(&state.db_pool)
    .await
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to fetch epoch stats: {}", e),
        )
    })?;

    // Build epochs map from query results
    let mut epochs: HashMap<String, EpochPhaseInfo> = HashMap::new();
    for stat in epoch_stats {
        let phase_info = EpochPhaseInfo {
            count: stat.count.unwrap_or(0),
            min: stat.min_epoch,
            max: stat.max_epoch,
        };
        epochs.insert(stat.phase.to_string(), phase_info);
    }

    // Check if latest indexed block is not older than 3 minutes
    let block_time_fresh = if let Ok(parsed_time) =
        chrono::DateTime::parse_from_rfc3339(&latest_indexed_block.block_time)
    {
        let age = Utc::now().signed_duration_since(parsed_time.with_timezone(&Utc));
        age.num_seconds() <= 180
    } else {
        false
    };

    // Calculate synced flag:
    // - min for final phase (EpochIndexPhase::MAX) is equal or below index_from_epoch config
    // - final phase max is no more than 1 behind phase0.max
    // - latest_indexed_block is not older than 3 minutes
    let index_from_epoch = crate::config::settings().indexer.index_from_epoch;
    let phase0 = epochs.get("0");
    let final_phase = epochs.get(&EpochIndexPhase::MAX.to_string());

    let synced = if let (Some(p0), Some(pmax)) = (phase0, final_phase) {
        let sufficiently_back_indexed = pmax.min <= Some(index_from_epoch);
        let final_phase_caught_up = match (p0.max, pmax.max) {
            (Some(p0_max), Some(pmax_max)) => p0_max - pmax_max <= 1,
            _ => false,
        };

        sufficiently_back_indexed && final_phase_caught_up && block_time_fresh
    } else {
        false
    };

    // Get current block from RPC (best effort, don't fail health check)
    let current_block = state
        .client
        .blocks()
        .at_latest()
        .await
        .ok()
        .map(|block| block.number());

    // Get finalized block from RPC (best effort, don't fail health check)
    let finalized_block = match state.client.backend().latest_finalized_block_ref().await {
        Ok(block_ref) => state
            .client
            .blocks()
            .at(block_ref.hash())
            .await
            .ok()
            .map(|block| block.number()),
        Err(_) => None,
    };

    // Get task status from registry
    let all_tasks = TASK_REGISTRY.get_all();
    let now_millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let tasks: HashMap<String, bool> = all_tasks
        .iter()
        .map(|task| {
            let is_running = task.ended_at.is_none();
            (task.name.clone(), is_running)
        })
        .collect();

    // Liveness check:
    // - Is the finalized routine making progress (within 5 minutes)?
    // - Is the app NOT in a fatal/unrecoverable state?
    let finalized_progressing = all_tasks
        .iter()
        .find(|t| t.name == "Queue finalized")
        .map(|t| {
            let elapsed_seconds = (now_millis.saturating_sub(t.current_work.last_updated)) / 1000;
            elapsed_seconds < 300 // 5 minutes
        })
        .unwrap_or(false);
    let no_fatal_error = !HEALTH_STATE.has_fatal_error();
    let live = finalized_progressing && no_fatal_error;

    // Readiness check:
    // - Database connection healthy (we already queried it above, so it's working)
    // - RPC node reachable (check if we got current_block or finalized_block)
    // - Not shutting down
    let db_healthy = true; // If we got here, DB queries succeeded
    let rpc_reachable = current_block.is_some() || finalized_block.is_some();
    let not_shutting_down = !HEALTH_STATE.is_shutting_down();
    let ready = db_healthy && rpc_reachable && not_shutting_down;

    let response = HealthResponse {
        live,
        ready,
        synced,
        latest_indexed_block,
        current_block,
        finalized_block,
        epochs,
        tasks,
    };

    let status = if synced {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    Ok((status, Json(response)))
}
