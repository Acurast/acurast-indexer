#[subxt::subxt(runtime_metadata_path = "./canary.scale")]
pub mod acurast {}

pub mod block_processing;
pub mod block_queuing;
pub mod config;
pub mod data_extraction;
pub mod db_timeout;
pub mod entities;
pub mod epoch_indexing;
mod errors;
pub mod event_indexing;
pub mod extrinsic_indexing;
pub mod health_state;
pub mod metadata;
pub mod phase_work;
mod response;
pub mod routes;
pub mod rpc_server;
mod server;
pub mod spec_version_backfill;
pub mod storage_indexing;
pub mod task_monitor;
pub mod transformation;
pub mod utils;

pub use errors::AppError;
pub use health_state::HEALTH_STATE;
pub use phase_work::PhaseWorkItem;
pub use server::run;
pub use task_monitor::{
    CurrentWork, QueueMetrics, QueueType, QueueTypeMetrics, TaskGuard, TaskInfo, TaskRegistry,
    TASK_REGISTRY,
};
