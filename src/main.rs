use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use acurast_indexer::block_processing::process_blocks;
use acurast_indexer::block_queuing::{
    on_finalized, queue_backwards, queue_gaps, queue_parents_of, queue_reprocess_blocks,
};
use acurast_indexer::config::get_config;
use acurast_indexer::entities::{
    Block, EpochIndexPhase, EventRow, EventsIndexPhase, ExtrinsicsIndexPhase,
};
use acurast_indexer::epoch_indexing::{queue_epochs_phase, wait_epoch_events_ready};
use acurast_indexer::event_indexing::{process_event_phase, queue_events_phase};
use acurast_indexer::extrinsic_indexing::{
    process_extrinsic_extract_addresses, queue_extrinsics_phase,
};
use acurast_indexer::phase_work::{
    phase_work_queues, PhaseWorkItem, PhaseWorkReceivers, PhaseWorkSenders,
};
use acurast_indexer::task_monitor::{QueueType, TaskGuard, TASK_REGISTRY};
use acurast_indexer::utils::connect_node;
use acurast_indexer::AppError;
use acurast_indexer::HEALTH_STATE;
use clap::{Parser, Subcommand};
use sqlx::query_as;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool, Pool, Postgres,
};
use std::num::NonZeroU32;
use subxt::{utils::H256, OnlineClient, PolkadotConfig};
use tokio::{signal, spawn, sync::mpsc, try_join};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, trace};
use tracing::{error, info, subscriber::set_global_default, warn};
use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_log::LogTracer;
use tracing_subscriber::{prelude::*, EnvFilter, Registry};

#[derive(Parser)]
#[command(name = "acurast-indexer")]
#[command(author, version, about = "The acurast indexer and API", long_about = None)]
struct Cli {
    #[arg(short, long, env, default_value = "local")]
    environment: String,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the indexer & API.
    Run {
        #[arg(long, num_args = 0..)]
        queue_before: Vec<String>,
    },
    GetBlockNumber {
        #[arg(long)]
        hash: String,
    },
}

impl Default for Commands {
    fn default() -> Self {
        Self::Run {
            queue_before: vec![],
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    dotenvy::dotenv().ok();
    LogTracer::init().expect("Failed to set logger");
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let formatting_layer = BunyanFormattingLayer::new("app".into(), std::io::stdout);
    let subscriber = Registry::default()
        .with(env_filter)
        .with(JsonStorageLayer)
        .with(formatting_layer);
    set_global_default(subscriber).expect("setting default subscriber failed");

    let cli = Cli::parse();

    // Initialize global configuration from environment
    let settings = get_config(&cli.environment)?;
    let (client, _) = connect_node(settings.indexer.archive_nodes[0].clone())
        .await
        .expect("Failed to connect to node");
    acurast_indexer::config::init_globals(settings.clone(), client.clone()).await?;

    match &cli.command.unwrap_or_default() {
        Commands::GetBlockNumber { hash } => {
            get_block_number(hash.to_owned()).await?;
            return Ok(());
        }
        Commands::Run { queue_before } => {
            // graceful shutdown
            let (_shutdown_send, mut shutdown_recv) = mpsc::unbounded_channel::<()>();
            let token = CancellationToken::new();
            let server_token = CancellationToken::new();

            // Create three separate connection pools
            let api_pool =
                get_db_pool_with_limit(settings.server.num_db_connections, 2, "API").await?;
            let phase_pool =
                get_db_pool_with_limit(settings.indexer.num_db_conn_phases, 10, "phase workers")
                    .await?;
            let index_pool = get_db_pool_with_limit(
                settings.indexer.num_workers_backwards
                    + settings.indexer.num_workers_gaps
                    + settings.indexer.num_workers_finalized
                    + 3
                    + 1,
                10,
                "backwards workers",
            )
            .await?;

            // Run migrations on API pool
            sqlx::migrate!().run(&api_pool).await?;

            // Backfill metadata for spec_versions (best-effort, don't fail startup on error)
            info!("Starting metadata backfill for spec_versions...");
            match acurast_indexer::spec_version_backfill::backfill_spec_version_metadata(
                &api_pool, &client,
            )
            .await
            {
                Ok(count) => {
                    info!(
                        "Metadata backfill completed: {} spec_versions updated",
                        count
                    );
                }
                Err(e) => {
                    warn!("Metadata backfill failed (non-fatal): {:?}", e);
                }
            }

            // Validate API key is configured before starting server
            if settings.auth.api_key.is_empty() {
                anyhow::bail!("API key not configured. Set API_KEY environment variable.");
            }

            let indexer = spawn(acurast_indexer::run(
                api_pool.clone(),
                client.clone(),
                server_token.clone(),
            ));

            let tasks = spawn(start_tasks(
                client,
                phase_pool,
                index_pool,
                queue_before.to_owned(),
                token.clone(),
            ));

            // wait for shutdown signal
            tokio::select! {
                _ = signal::ctrl_c() => {},
                _ = shutdown_recv.recv() => {},
            }

            HEALTH_STATE.set_shutting_down();
            token.cancel();
            info!("Shutdown signal received, stopping in 5 seconds...");
            // useful to test shutdown of tasks while still reporting status of task monitor
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            server_token.cancel();
            info!("Graceful shutdown...");
            try_join!(indexer, tasks)?;

            return Ok(());
        }
    }
}

pub async fn get_block_number(hash: String) -> Result<(), anyhow::Error> {
    let settings = acurast_indexer::config::settings();
    let (api, _) = connect_node(settings.indexer.archive_nodes[0].clone()).await?;

    let block = api
        .blocks()
        .at(H256::from_slice(&hex::decode(hash.clone()).unwrap()))
        .await?;
    let parent = api.blocks().at(block.header().parent_hash).await?;
    let grandparent = api.blocks().at(parent.header().parent_hash).await?;
    println!(
        "parent {:?} {:?} ---> {:?} {:?} ---> {:?} {:?}",
        grandparent.reference().hash(),
        grandparent.number(),
        parent.reference().hash(),
        parent.number(),
        block.hash(),
        block.number()
    );

    Ok(())
}

pub async fn get_db_pool_with_limit(
    max_connections: u32,
    acquire_timeout: u64,
    pool_name: &str,
) -> Result<PgPool, anyhow::Error> {
    let db = &acurast_indexer::config::settings().database;
    let options = PgConnectOptions::new()
        .host(&db.host)
        .port(db.port)
        .username(&db.username)
        .password(&db.password)
        .database(&db.database);
    info!("Connecting to Postgres ({})...", pool_name);
    debug!(
        "Connecting to Postgres ({}) with options: {:?}",
        pool_name, options
    );
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(std::time::Duration::from_secs(acquire_timeout))
        .connect_lazy_with(options);
    Ok(pool)
}

async fn start_tasks(
    client: OnlineClient<PolkadotConfig>,
    phase_pool: Pool<Postgres>,
    index_pool: Pool<Postgres>,
    queue_before: Vec<String>,
    cancel_token: CancellationToken,
) -> Result<(), anyhow::Error> {
    let settings = &acurast_indexer::config::settings().indexer;
    let tracker = TaskTracker::new();

    // Create a pool of clients for phase workers
    let num_conn = settings.num_conn_phases.max(1) as usize;
    info!(
        "Creating {} RPC connections for {} phase workers",
        num_conn, settings.num_workers_phases
    );
    let mut phase_clients: Vec<OnlineClient<PolkadotConfig>> = Vec::with_capacity(num_conn);
    for i in 0..num_conn {
        let (conn_client, _) = connect_node(settings.archive_nodes[0].clone()).await?;
        info!("Created phase connection {}/{}", i + 1, num_conn);
        phase_clients.push(conn_client);
    }

    // Unified phase processing with priority queues
    // Epochs > Events > Extrinsics (to avoid extrinsics piling up before events are processed)
    let (tx_phase, rx_phase) = phase_work_queues();

    // Channel for epoch insertion notifications (for utilization recalculation)
    // Sends (epoch_number, block_hash) when a new epoch is discovered at its start block
    let (epoch_tx, epoch_rx) = tokio::sync::mpsc::channel::<(u32, String)>(100);

    // Oneshot channels to capture the first finalized block number
    // One for backwards indexing, one for gap detection, one for commitment initial sync, one for event queuers
    let (tx_first_finalized_backwards, rx_first_finalized_backwards) =
        tokio::sync::oneshot::channel::<(u32, H256)>();
    let (tx_first_finalized_gaps, rx_first_finalized_gaps) =
        tokio::sync::oneshot::channel::<(u32, H256)>();
    let (tx_first_finalized_commitments, rx_first_finalized_commitments) =
        tokio::sync::oneshot::channel::<(u32, H256)>();
    let (tx_first_finalized_events, rx_first_finalized_events) =
        tokio::sync::oneshot::channel::<(u32, H256)>();

    // Shared finalized block number for pruning checks (updated periodically)
    let latest_finalized = Arc::new(AtomicU32::new(0));

    // the main work distribution channel, multi-producer (we use only one) and multi-consumer
    let (tx_finalized, rx_finalized) = async_channel::unbounded();
    if settings.index_finalized {
        for i in 0..settings.num_workers_finalized {
            info!("Spawn worker {:?}", i);
            tracker.spawn(process_blocks(
                "finalized".to_string(),
                i,
                rx_finalized.clone(),
                index_pool.clone(),
                tx_phase.clone(),
                Some(epoch_tx.clone()),
                cancel_token.clone(),
            ));
        }
    }
    tracker.spawn(on_finalized(
        tx_finalized.clone(),
        cancel_token.clone(),
        vec![
            tx_first_finalized_backwards,
            tx_first_finalized_gaps,
            tx_first_finalized_commitments,
            tx_first_finalized_events,
        ],
        latest_finalized.clone(),
    ));

    // Find lowest block known in DB and fall back to the first block processed in finalized if DB has no blocks (therefore this is done AFTER starting finalized tracking task!)
    let lowest_hash = if let Some(b) = query_as!(
        Block,
        "SELECT block_number, hash, block_time FROM blocks ORDER BY block_number ASC LIMIT 1"
    )
    .fetch_optional(&index_pool)
    .await
    .map_err(|e| AppError::InternalError(e.into()))?
    {
        H256::from_slice(&hex::decode(&b.hash).unwrap())
    } else {
        tokio::select! {
            biased;

            _ = cancel_token.cancelled() => {
                return Ok(());
            }
            result = rx_first_finalized_backwards => {
                match result {
                    Ok((block_num, block_ref)) => {
                        info!("Using first finalized block {}, {} as starting point for backwards indexing", block_num, block_ref);
                        block_ref
                    }
                    Err(_) => {
                        panic!("Failed to receive first finalized block");
                    }
                }
            }
        }
    };
    let lowest_done = client.blocks().at(lowest_hash).await?;
    let lowest_done_block = lowest_done.number() as u32;

    let (tx_before, rx_before) = async_channel::unbounded();

    // Check if we have reprocess blocks from config or queue_before from CLI
    let has_reprocess_blocks = !settings.reprocess.blocks.is_empty();
    let has_queue_before = !queue_before.is_empty();

    if has_reprocess_blocks || has_queue_before {
        info!("Spawn worker for priority blocks (reprocess/individual)");
        tracker.spawn(process_blocks(
            "priority".to_string(),
            0,
            rx_before.clone(),
            index_pool.clone(),
            tx_phase.clone(),
            Some(epoch_tx.clone()),
            cancel_token.clone(),
        ));

        // Queue reprocess blocks from config first
        if has_reprocess_blocks {
            let reprocess_blocks = settings.reprocess.blocks.clone();
            info!(
                "Queueing {} blocks from reprocess config",
                reprocess_blocks.len()
            );
            tracker.spawn(queue_reprocess_blocks(
                tx_before.clone(),
                reprocess_blocks,
                cancel_token.clone(),
            ));
        }

        // Then queue parents from CLI args
        if has_queue_before {
            tracker.spawn(queue_parents_of(
                tx_before.clone(),
                queue_before,
                cancel_token.clone(),
            ));
        }
    }

    let (tx_past, rx_past) = async_channel::unbounded();
    if settings.index_backwards {
        for i in 0..settings.num_workers_backwards {
            info!("Spawn worker {:?}", i);
            tracker.spawn(process_blocks(
                "backwards".to_string(),
                i,
                rx_past.clone(),
                index_pool.clone(),
                tx_phase.clone(),
                Some(epoch_tx.clone()),
                cancel_token.clone(),
            ));
        }
        // Queue gaps first, then backwards (gaps have priority)
        let tx_past_queuer = tx_past.clone();
        let backpressure = tx_phase.extrinsic.clone();
        let index_pool_queuer = index_pool.clone();
        let cancel_token_queuer = cancel_token.clone();
        tracker.spawn(async move {
            queue_gaps(
                tx_past_queuer.clone(),
                backpressure.clone(),
                index_pool_queuer.clone(),
                cancel_token_queuer.clone(),
                lowest_done_block,
                rx_first_finalized_gaps,
            )
            .await;
            queue_backwards(
                tx_past_queuer,
                backpressure,
                index_pool_queuer,
                lowest_done,
                cancel_token_queuer,
            )
            .await;
        });
    }

    if settings.index_phases {
        // Spawn unified phase workers (each worker gets a client from the pool)
        for i in 0..settings.num_workers_phases {
            let conn_idx = i as usize % num_conn;
            info!("Spawn phase worker {:?} (using connection {})", i, conn_idx);
            tracker.spawn(process_phases(
                i,
                rx_phase.clone(),
                tx_phase.clone(),
                phase_pool.clone(),
                phase_clients[conn_idx].clone(),
                cancel_token.clone(),
                latest_finalized.clone(),
            ));
        }
    }

    // Spawn storage snapshot pruning task (runs periodically)
    tracker.spawn(storage_pruning_task(
        phase_pool.clone(),
        client.clone(),
        cancel_token.clone(),
    ));

    // Spawn queue metrics monitor task (reads channel lengths every second)
    tracker.spawn(queue_metrics_monitor_task(
        tx_phase.clone(),
        cancel_token.clone(),
    ));

    // Spawn commitment processing task (runs periodically)
    tracker.spawn(commitment_processing_task(
        phase_pool.clone(),
        client.clone(),
        cancel_token.clone(),
        rx_first_finalized_commitments,
        epoch_rx,
    ));

    // Reprocess specific events if configured
    if !settings.reprocess.events.is_empty() {
        info!(
            "Reprocessing {} events from config",
            settings.reprocess.events.len(),
        );
        for reprocess_event in &settings.reprocess.events {
            let target_phase = reprocess_event.phase as i32;
            let event_id = format!(
                "{}-{}.{}",
                reprocess_event.block_number,
                reprocess_event.extrinsic_index,
                reprocess_event.index
            );

            // Update phase in database
            match sqlx::query(
                "UPDATE events SET phase = $1 WHERE block_number = $2 AND extrinsic_index = $3 AND index = $4",
            )
            .bind(target_phase)
            .bind(reprocess_event.block_number)
            .bind(reprocess_event.extrinsic_index)
            .bind(reprocess_event.index)
            .execute(&phase_pool)
            .await
            {
                Ok(result) => {
                    if result.rows_affected() > 0 {
                        info!(
                            "Set event {} to phase {} for reprocessing",
                            event_id, target_phase
                        );
                    } else {
                        warn!(
                            "Event {} not found in database, skipping reprocess",
                            event_id
                        );
                    }
                }
                Err(e) => {
                    error!(
                        "Failed to update event {} for reprocessing: {:?}",
                        event_id, e
                    );
                }
            }

            // Directly queue the event for processing
            let event_result: Result<Option<EventRow>, sqlx::Error> = sqlx::query_as(
                "SELECT block_number, extrinsic_index, index, pallet, variant, data, phase, error, block_number, block_time FROM events WHERE block_number = $1 AND extrinsic_index = $2 AND index = $3",
            )
            .bind(reprocess_event.block_number)
            .bind(reprocess_event.extrinsic_index)
            .bind(reprocess_event.index)
            .fetch_optional(&phase_pool)
            .await;

            match event_result {
                Ok(Some(event)) => {
                    tokio::select! {
                        biased;
                        _ = cancel_token.cancelled() => {
                            break;
                        },
                        result = tx_phase.send_event(event) => {
                            if let Err(e) = result {
                                error!("Failed to queue event {}: {:?}", event_id, e);
                            } else {
                                info!("Queued event {} for processing", event_id);
                            }
                        }
                    }
                }
                Ok(None) => {
                    warn!("Event {} not found for queuing", event_id);
                }
                Err(e) => {
                    error!("Failed to fetch event {}: {:?}", event_id, e);
                }
            }
        }
    }

    if settings.index_phases {
        // Queue extrinsics - single queue that finds all extrinsics in any phase < max_phase
        tracker.spawn(queue_extrinsics_phase(
            tx_phase.extrinsic.clone(),
            phase_pool.clone(),
            cancel_token.clone(),
            settings.index_from_block,
        ));

        // Queue events - spawn parallel queuers to speed up queuing
        // Each queuer handles a portion of the block range from index_from_block to first_finalized
        // The last queuer continues indefinitely (follows finalized)
        let num_event_queuers = settings.num_event_queuers.max(1);
        if num_event_queuers == 1 {
            // Single queuer - simple case, no block range splitting
            tracker.spawn(queue_events_phase(
                tx_phase.event.clone(),
                phase_pool.clone(),
                cancel_token.clone(),
                0, // queuer_id
                settings.index_from_block,
                None, // no upper limit - follows finalized
            ));
        } else {
            // Multiple queuers - wait for first finalized, then split the range
            let tx_event = tx_phase.event.clone();
            let pool = phase_pool.clone();
            let token = cancel_token.clone();
            let index_from = settings.index_from_block;
            tracker.spawn(async move {
                // Wait for first finalized block to determine range
                let first_finalized = tokio::select! {
                    biased;
                    _ = token.cancelled() => {
                        info!("Event queuers cancelled before receiving first finalized block");
                        return Ok(());
                    }
                    result = rx_first_finalized_events => {
                        match result {
                            Ok((block_num, _)) => block_num,
                            Err(_) => {
                                error!("Failed to receive first finalized block for event queuers");
                                return Ok(());
                            }
                        }
                    }
                };

                info!(
                    "Spawning {} event queuers for block range [{}, {}]",
                    num_event_queuers, index_from, first_finalized
                );

                // Calculate block range per queuer
                let total_blocks = first_finalized.saturating_sub(index_from);
                let blocks_per_queuer = total_blocks / num_event_queuers;

                let inner_tracker = TaskTracker::new();

                for i in 0..num_event_queuers {
                    let from_block = index_from + (i * blocks_per_queuer);
                    // Last queuer has no upper limit (follows finalized)
                    // Other queuers have a fixed upper limit
                    let to_block = if i == num_event_queuers - 1 {
                        None
                    } else {
                        Some(index_from + ((i + 1) * blocks_per_queuer))
                    };

                    info!(
                        "Event queuer #{}: block range [{}, {:?})",
                        i, from_block, to_block
                    );

                    inner_tracker.spawn(queue_events_phase(
                        tx_event.clone(),
                        pool.clone(),
                        token.clone(),
                        i,
                        from_block,
                        to_block,
                    ));
                }

                inner_tracker.close();
                inner_tracker.wait().await;
                Ok::<(), anyhow::Error>(())
            });
        }

        // Queue epochs that are past Raw phase but not yet fully processed (on restart)
        // Queries all intermediate phases (>= 1 and < max_phase_for(Epoch))
        tracker.spawn(queue_epochs_phase(
            tx_phase.epoch.clone(),
            phase_pool.clone(),
            cancel_token.clone(),
            settings.index_from_block,
        ));

        // Queue epochs - waits for events to be fully indexed before processing Raw epochs
        tracker.spawn(wait_epoch_events_ready(
            phase_pool.clone(),
            cancel_token.clone(),
            settings.index_from_block,
        ));
    }

    // Once we spawned everything, we close the tracker.
    tracker.close();

    // Wait for everything to finish.
    tracker.wait().await;

    drop(tx_finalized);
    drop(tx_before);
    drop(tx_past);
    drop(tx_phase);

    Ok(())
}

/// Periodic task to monitor queue sizes and update pending counts
async fn queue_metrics_monitor_task(
    tx_phase: PhaseWorkSenders,
    cancel_token: CancellationToken,
) -> Result<(), anyhow::Error> {
    let task_id = TASK_REGISTRY.start("Queue metrics monitor", None);
    const POLL_INTERVAL_SECS: u64 = 1;

    info!(
        "Starting queue metrics monitor (interval: {}s)",
        POLL_INTERVAL_SECS
    );

    loop {
        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => {
                info!("Queue metrics monitor task cancelled");
                TASK_REGISTRY.end(task_id);
                break;
            }
            _ = tokio::time::sleep(Duration::from_secs(POLL_INTERVAL_SECS)) => {
                // Read channel lengths directly
                let event_len = tx_phase.event.len() as i64;
                let extrinsic_len = tx_phase.extrinsic.len() as i64;
                let epoch_len = tx_phase.epoch.len() as i64;

                TASK_REGISTRY.set_pending_count(QueueType::Event, event_len);
                TASK_REGISTRY.set_pending_count(QueueType::Extrinsic, extrinsic_len);
                TASK_REGISTRY.set_pending_count(QueueType::Epoch, epoch_len);

                trace!(
                    "Queue metrics updated: events={}, extrinsics={}, epochs={}",
                    event_len, extrinsic_len, epoch_len
                );
            }
        }
    }

    Ok(())
}

/// Periodic task to update the finalized block cache and prune old storage snapshots
async fn storage_pruning_task(
    db_pool: Pool<Postgres>,
    client: OnlineClient<PolkadotConfig>,
    cancel_token: CancellationToken,
) -> Result<(), anyhow::Error> {
    let task_id = TASK_REGISTRY.start("Storage pruning", None);

    const PRUNE_INTERVAL_SECS: u64 = 10; // Run every 5 minutes
    const BATCH_SIZE: i64 = 1000; // Delete in batches to avoid long locks

    info!(
        "Starting storage pruning task (interval: {}s, batch: {})",
        PRUNE_INTERVAL_SECS, BATCH_SIZE
    );

    loop {
        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => {
                info!("Storage pruning task cancelled");
                TASK_REGISTRY.end(task_id);
                break;
            }
            _ = tokio::time::sleep(Duration::from_secs(PRUNE_INTERVAL_SECS)) => {
                // Update finalized block cache
                match client.backend().latest_finalized_block_ref().await {
                    Ok(block_ref) => {
                        match client.blocks().at(block_ref.hash()).await {
                            Ok(block) => {
                                let block_number = block.number() as u64;
                                // Run pruning (uses all rules for pruning check)
                                match acurast_indexer::storage_indexing::prune_storage_snapshots(
                                    &db_pool,
                                    acurast_indexer::config::storage_rules().all(),
                                    block_number,
                                    BATCH_SIZE,
                                ).await {
                                    Ok(deleted) => {
                                        if deleted > 0 {
                                            info!("Pruned {} total storage snapshots", deleted);
                                        }
                                    }
                                    Err(e) => {
                                        error!("Failed to prune storage snapshots: {:?}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Failed to get finalized block details: {:?}", e);
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to get finalized block ref: {:?}", e);
                    }
                }
            }
        }
    }

    Ok(())
}

/// Periodic task to process commitment snapshots into the commitments table
async fn commitment_processing_task(
    db_pool: Pool<Postgres>,
    client: OnlineClient<PolkadotConfig>,
    cancel_token: CancellationToken,
    rx_first_finalized: tokio::sync::oneshot::Receiver<(u32, H256)>,
    mut epoch_rx: tokio::sync::mpsc::Receiver<(u32, String)>,
) -> Result<(), anyhow::Error> {
    let task_id = TASK_REGISTRY.start("Commitment processing", None);

    const PROCESS_INTERVAL_SECS: u64 = 30; // Run every 30 seconds
    const BATCH_SIZE: i64 = 100; // Process 100 commitments per batch

    info!("Starting commitment processing task, waiting for first finalized block...");

    // Wait for the first finalized block before starting
    let (first_finalized_block, first_finalized_hash) = tokio::select! {
        biased;
        _ = cancel_token.cancelled() => {
            info!("Commitment processing task cancelled before initial sync");
            TASK_REGISTRY.end(task_id);
            return Ok(());
        }
        result = rx_first_finalized => {
            match result {
                Ok((block_num, block_hash)) => {
                    info!("Commitment processing: received first finalized block {} ({})", block_num, block_hash);
                    (block_num as i64, block_hash)
                }
                Err(_) => {
                    error!("Failed to receive first finalized block for commitment processing");
                    TASK_REGISTRY.end(task_id);
                    return Ok(());
                }
            }
        }
    };

    // Check if we need to rescan based on latest epoch
    let scan_progress_block: Option<i64> =
        sqlx::query_scalar("SELECT block_number FROM _index_progress WHERE id = 'commitment_scan'")
            .fetch_optional(&db_pool)
            .await
            .ok()
            .flatten();

    // Get the latest epoch's start block (highest epoch number is the current ongoing epoch)
    let latest_epoch_start: Option<i64> =
        sqlx::query_scalar("SELECT epoch_start FROM epochs ORDER BY epoch DESC LIMIT 1")
            .fetch_optional(&db_pool)
            .await
            .ok()
            .flatten();

    let needs_rescan = match (scan_progress_block, latest_epoch_start) {
        (Some(scanned_block), Some(epoch_start)) => scanned_block < epoch_start,
        (None, Some(_)) => true,  // Never scanned before, use epoch start
        (None, None) => true,     // Never scanned, no epoch data - use first finalized
        (Some(_), None) => false, // Already scanned, but no epoch data yet
    };

    if needs_rescan {
        // Determine which block to scan at
        let scan_hash = if let Some(epoch_start_block) = latest_epoch_start {
            // Use epoch start block - need to fetch hash from database
            match sqlx::query_scalar::<_, String>("SELECT hash FROM blocks WHERE block_number = $1")
                .bind(epoch_start_block)
                .fetch_optional(&db_pool)
                .await
            {
                Ok(Some(hash_hex)) => match hex::decode(&hash_hex) {
                    Ok(hash_bytes) if hash_bytes.len() == 32 => {
                        let mut hash = [0u8; 32];
                        hash.copy_from_slice(&hash_bytes);
                        Some((epoch_start_block, H256::from(hash)))
                    }
                    Ok(_) => {
                        error!(
                            "Invalid block hash length for epoch start block {}",
                            epoch_start_block
                        );
                        None
                    }
                    Err(e) => {
                        error!(
                            "Failed to decode block hash for epoch start block {}: {:?}",
                            epoch_start_block, e
                        );
                        None
                    }
                },
                Ok(None) => {
                    warn!(
                        "Epoch start block {} not found in database",
                        epoch_start_block
                    );
                    None
                }
                Err(e) => {
                    error!("Failed to query epoch start block hash: {:?}", e);
                    None
                }
            }
        } else {
            // No epoch data, use first finalized block
            Some((first_finalized_block, first_finalized_hash))
        };

        if let Some((block_num, block_hash)) = scan_hash {
            info!(
                "Starting commitment scan at block {} (last scan: {:?}, latest epoch start: {:?})",
                block_num, scan_progress_block, latest_epoch_start
            );
            TASK_REGISTRY.set_detail(task_id, format!("scanning at block {}...", block_num));

            match acurast_indexer::storage_indexing::scan_all_commitments_at_block(
                &db_pool,
                &client,
                block_hash,
                Some(task_id),
                &cancel_token,
            )
            .await
            {
                Ok(true) => {
                    info!("Commitment scan complete at block {}", block_num);
                    // Record successful completion
                    if let Err(e) = sqlx::query(
                        "INSERT INTO _index_progress (id, block_number, completed_at) VALUES ('commitment_scan', $1, NOW())
                         ON CONFLICT (id) DO UPDATE SET block_number = EXCLUDED.block_number, completed_at = NOW()",
                    )
                    .bind(block_num)
                    .execute(&db_pool)
                    .await
                    {
                        error!("Failed to record commitment scan progress: {:?}", e);
                    }
                }
                Ok(false) => {
                    info!("Commitment scan was cancelled or skipped");
                    TASK_REGISTRY.set_detail(task_id, "scan incomplete".to_string());
                }
                Err(e) => {
                    error!("Failed commitment scan: {:?}", e);
                    TASK_REGISTRY.set_detail(task_id, format!("scan failed: {:?}", e));
                }
            }
        } else {
            warn!("Could not determine block to scan at, will retry on next startup");
            TASK_REGISTRY.set_detail(task_id, "scan skipped: no block available".to_string());
        }
    } else {
        info!(
            "Skipping commitment scan: already up-to-date (last scan: block {}, latest epoch start: {:?})",
            scan_progress_block.unwrap(), latest_epoch_start
        );
        TASK_REGISTRY.set_detail(
            task_id,
            format!(
                "up-to-date (scanned at block {})",
                scan_progress_block.unwrap()
            ),
        );
    }

    // Determine the starting block for incremental processing:
    // Use max block_number from commitments table, or fall back to first_finalized_block
    let min_block_for_incremental =
        sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(block_number) FROM commitments")
            .fetch_one(&db_pool)
            .await
            .ok()
            .flatten()
            .unwrap_or(first_finalized_block);

    info!(
        "Starting incremental commitment processing (interval: {}s, batch: {}, min_block: {})",
        PROCESS_INTERVAL_SECS, BATCH_SIZE, min_block_for_incremental
    );
    TASK_REGISTRY.set_detail(task_id, "idle".to_string());

    // Incremental processing loop: only process snapshots after min_block_for_incremental
    let mut total_processed: u64 = 0;
    loop {
        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => {
                info!("Commitment processing task cancelled");
                TASK_REGISTRY.end(task_id);
                break;
            }
            Some((epoch, block_hash_hex)) = epoch_rx.recv() => {
                info!("Received new epoch {} at block {}, triggering full commitment rescan", epoch, block_hash_hex);
                TASK_REGISTRY.set_detail(task_id, format!("rescanning for epoch {}...", epoch));

                // Convert hex string to H256
                match hex::decode(&block_hash_hex) {
                    Ok(hash_bytes) if hash_bytes.len() == 32 => {
                        let mut hash = [0u8; 32];
                        hash.copy_from_slice(&hash_bytes);
                        let block_hash = H256::from(hash);

                        // Trigger full rescan at this epoch's start block
                        match acurast_indexer::storage_indexing::scan_all_commitments_at_block(
                            &db_pool,
                            &client,
                            block_hash,
                            Some(task_id),
                            &cancel_token,
                        )
                        .await
                        {
                            Ok(true) => {
                                info!("Full commitment rescan complete for epoch {}", epoch);
                                TASK_REGISTRY.set_detail(task_id, format!("idle (total: {}, last rescan: epoch {})", total_processed, epoch));
                            }
                            Ok(false) => {
                                warn!("Commitment rescan for epoch {} was cancelled or skipped", epoch);
                                TASK_REGISTRY.set_detail(task_id, format!("rescan incomplete (epoch {})", epoch));
                            }
                            Err(e) => {
                                error!("Failed to rescan commitments for epoch {}: {:?}", epoch, e);
                                TASK_REGISTRY.set_detail(task_id, format!("rescan failed: {:?}", e));
                            }
                        }
                    }
                    Ok(_) => {
                        error!("Invalid block hash length for epoch {}: {}", epoch, block_hash_hex);
                    }
                    Err(e) => {
                        error!("Failed to decode block hash for epoch {}: {:?}", epoch, e);
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(PROCESS_INTERVAL_SECS)) => {
                // Find unprocessed commitment snapshots at or after min_block_for_incremental
                let snapshot_ids = match acurast_indexer::storage_indexing::find_unprocessed_commitment_snapshots(
                    &db_pool,
                    Some(min_block_for_incremental),
                    BATCH_SIZE,
                ).await {
                    Ok(ids) => ids,
                    Err(e) => {
                        error!("Failed to find unprocessed commitment snapshots: {:?}", e);
                        continue;
                    }
                };

                if snapshot_ids.is_empty() {
                    TASK_REGISTRY.set_detail(task_id, format!("idle (total: {})", total_processed));
                    continue;
                }

                // Process the found snapshot IDs
                TASK_REGISTRY.set_detail(task_id, format!("processing {} snapshots...", snapshot_ids.len()));
                match acurast_indexer::storage_indexing::process_commitment_snapshot_ids(
                    &db_pool,
                    &client,
                    &snapshot_ids,
                ).await {
                    Ok(processed) => {
                        total_processed += processed;
                        if processed > 0 {
                            info!("Processed {} commitments", processed);
                        }
                        TASK_REGISTRY.set_detail(task_id, format!("idle (total: {})", total_processed));
                    }
                    Err(e) => {
                        error!("Failed to process commitment snapshots: {:?}", e);
                        TASK_REGISTRY.set_detail(task_id, format!("error: {:?}", e));
                    }
                }
            }
        }
    }

    Ok(())
}

#[tracing::instrument(
      skip_all,
      fields(
          worker = format!("phase-{:?}", worker_id),
      )
  )]
/// Unified phase worker that can process any phase type
async fn process_phases(
    worker_id: u32,
    receiver: PhaseWorkReceivers,
    tx: PhaseWorkSenders,
    db_pool: Pool<Postgres>,
    client: OnlineClient<PolkadotConfig>,
    cancel_token: CancellationToken,
    latest_finalized: Arc<AtomicU32>,
) -> Result<(), anyhow::Error> {
    let mut task = TaskGuard::new("Phase worker", Some(worker_id));

    'outer: while !cancel_token.is_cancelled() {
        // Use priority-aware receive (epochs > events > extrinsics)
        let work_item = match receiver.recv().await {
            Some(item) => item,
            None => {
                warn!("all queues closed");
                break 'outer;
            }
        };

        // Determine queue type for throughput tracking
        let queue_type = match &work_item {
            PhaseWorkItem::Extrinsic(_) => QueueType::Extrinsic,
            PhaseWorkItem::Event(_) => QueueType::Event,
            PhaseWorkItem::Epoch(_) => QueueType::Epoch,
        };

        let res = match work_item {
            PhaseWorkItem::Extrinsic(extrinsic) => {
                trace!(
                    "Process phase {:?} of extrinsic {:?}",
                    extrinsic.phase,
                    extrinsic.id()
                );
                task.set_extrinsic(extrinsic.id(), extrinsic.phase as i32);
                match extrinsic.phase {
                    ExtrinsicsIndexPhase::Raw => {
                        // Phase 0: Extract addresses from extrinsic
                        process_extrinsic_extract_addresses(
                            worker_id,
                            extrinsic,
                            acurast_indexer::config::extrinsic_transformations(),
                            acurast_indexer::config::pallet_method_map(),
                            &db_pool,
                            &tx.extrinsic,
                            &cancel_token,
                        )
                        .await
                    }
                    ExtrinsicsIndexPhase::AddressExtracted => {
                        // Phase 1: Index storage based on extrinsic-triggered rules
                        acurast_indexer::storage_indexing::process_extrinsic_storage_indexing(
                            worker_id, extrinsic, &db_pool, &client,
                        )
                        .await
                    }
                    ExtrinsicsIndexPhase::StorageIndexed => {
                        // Final phase, no re-queuing
                        warn!(
                            "Received extrinsic in StorageIndexing phase (final), skipping: {:?}",
                            extrinsic.id()
                        );
                        Ok(())
                    }
                }
            }
            PhaseWorkItem::Event(event) => {
                // Process all phases in one go (no re-queuing between phases)
                process_event_all_phases(
                    worker_id,
                    event,
                    &mut task,
                    &db_pool,
                    &client,
                    &latest_finalized,
                )
                .await
            }
            PhaseWorkItem::Epoch(epoch) => {
                trace!("Process phase {:?} of epoch {:?}", epoch.phase, epoch.epoch);
                task.set_epoch(epoch.epoch);
                let max_epoch_phase = acurast_indexer::config::storage_rules()
                    .max_phase_for(acurast_indexer::storage_indexing::TriggerKind::Epoch);

                match epoch.phase {
                    EpochIndexPhase::Raw => {
                        // Phase 0: Should not receive Raw epochs here
                        warn!("Received epoch in Raw phase, skipping: {:?}", epoch.epoch);
                        Ok(())
                    }
                    EpochIndexPhase::EventsReady => {
                        // Phase 1: All events in this epoch are fully indexed
                        // Process hardcoded manager indexing (advances to StorageIndexed2)
                        acurast_indexer::storage_indexing::process_epoch_storage_indexing(
                            worker_id,
                            epoch.clone(),
                            &db_pool,
                            &client,
                            &cancel_token,
                        )
                        .await?;

                        // Get finalized block for pruning threshold check
                        let finalized_block = {
                            let cached = latest_finalized.load(Ordering::Relaxed);
                            if cached > 0 {
                                Some(cached)
                            } else {
                                None
                            }
                        };

                        // Process phase 2 storage rules
                        acurast_indexer::storage_indexing::process_epoch_storage_rules_indexing(
                            worker_id,
                            epoch.clone(),
                            EpochIndexPhase::StorageIndexed2,
                            &db_pool,
                            &client,
                            finalized_block,
                        )
                        .await?;

                        // Re-queue for next phase if there are more rules
                        if max_epoch_phase > 2 {
                            let mut epoch_for_next = epoch;
                            epoch_for_next.phase = EpochIndexPhase::StorageIndexed2;
                            tx.epoch.send(epoch_for_next).await.ok();
                        }
                        Ok(())
                    }
                    EpochIndexPhase::StorageIndexed2 => {
                        // Get finalized block for pruning threshold check
                        let finalized_block = {
                            let cached = latest_finalized.load(Ordering::Relaxed);
                            if cached > 0 {
                                Some(cached)
                            } else {
                                None
                            }
                        };

                        // Phase 2->3: Process phase 3 storage rules
                        acurast_indexer::storage_indexing::process_epoch_storage_rules_indexing(
                            worker_id,
                            epoch.clone(),
                            EpochIndexPhase::StorageIndexed3,
                            &db_pool,
                            &client,
                            finalized_block,
                        )
                        .await?;

                        // Re-queue for next phase if there are more rules
                        if max_epoch_phase > 3 {
                            let mut epoch_for_next = epoch;
                            epoch_for_next.phase = EpochIndexPhase::StorageIndexed3;
                            tx.epoch.send(epoch_for_next).await.ok();
                        }
                        Ok(())
                    }
                    EpochIndexPhase::StorageIndexed3 => {
                        // Get finalized block for pruning threshold check
                        let finalized_block = {
                            let cached = latest_finalized.load(Ordering::Relaxed);
                            if cached > 0 {
                                Some(cached)
                            } else {
                                None
                            }
                        };

                        // Phase 3->4: Process phase 4 storage rules
                        acurast_indexer::storage_indexing::process_epoch_storage_rules_indexing(
                            worker_id,
                            epoch,
                            EpochIndexPhase::StorageIndexed4,
                            &db_pool,
                            &client,
                            finalized_block,
                        )
                        .await
                    }
                    EpochIndexPhase::StorageIndexed4 => {
                        // Final phase, no re-queuing
                        Ok(())
                    }
                }
            }
        };

        match res {
            Ok(_) => {
                // Record successful processing for throughput tracking
                TASK_REGISTRY.record_processed(queue_type);
            }
            Err(e) => {
                error!("Failed to process item: {:?}", e);
                task.record_error(&e);
            }
        }
    }

    task.complete();
    Ok(())
}

/// Process an event through all its phases in one go (no re-queuing between phases).
/// This reduces latency for multi-phase events by keeping them in the same worker.
async fn process_event_all_phases(
    worker_id: u32,
    mut event: EventRow,
    task: &mut TaskGuard,
    db_pool: &Pool<Postgres>,
    client: &OnlineClient<PolkadotConfig>,
    latest_finalized: &Arc<AtomicU32>,
) -> Result<(), anyhow::Error> {
    let storage_rules = acurast_indexer::config::storage_rules();
    let max_phase =
        storage_rules.max_phase_for(acurast_indexer::storage_indexing::TriggerKind::Event);

    // Start from the event's current phase
    let mut current_phase_num: u32 = match event.phase {
        EventsIndexPhase::Created => 1,
        EventsIndexPhase::JobsExtracted => 2,
        EventsIndexPhase::StorageIndexed2 => 3,
        EventsIndexPhase::StorageIndexed3 => 4,
        EventsIndexPhase::StorageIndexed4 => 5,
        EventsIndexPhase::StorageIndexed5 => return Ok(()), // Already done
    };

    loop {
        trace!(
            "Process phase {} of event {:?}",
            current_phase_num,
            event.id()
        );
        task.set_event(event.id(), current_phase_num as i32);

        // Process the current phase
        match current_phase_num {
            1 => {
                // Phase 1: Extract jobs from events
                let success = process_event_phase(
                    worker_id,
                    event.clone(),
                    acurast_indexer::config::event_transformations(),
                    db_pool,
                )
                .await?;

                if !success {
                    // Error recorded in DB, stop processing
                    return Ok(());
                }
            }
            2 | 3 | 4 | 5 => {
                // Phases 2-5: Process storage indexing rules
                let finalized_block = {
                    let cached = latest_finalized.load(Ordering::Relaxed);
                    if cached > 0 {
                        Some(cached)
                    } else {
                        None
                    }
                };

                acurast_indexer::storage_indexing::process_events_storage_indexing(
                    worker_id,
                    event.clone(),
                    current_phase_num,
                    db_pool,
                    client,
                    finalized_block,
                )
                .await?;
            }
            _ => break, // Beyond max phase
        }

        // Find the next phase with rules for this event
        let mut next_phase_with_rules: Option<u32> = None;
        for phase in (current_phase_num + 1)..=max_phase {
            if storage_rules.has_event_rule_at_phase(
                event.pallet as i32,
                event.variant as i32,
                phase,
            ) {
                next_phase_with_rules = Some(phase);
                break;
            }
        }

        // Determine target DB phase and whether to continue
        let target_db_phase = match next_phase_with_rules {
            Some(phase) => phase - 1,
            None => max_phase,
        };

        // Update the database phase
        sqlx::query(
            "UPDATE events SET phase = $1, error = NULL WHERE block_number = $2 AND extrinsic_index = $3 AND index = $4",
        )
        .bind(target_db_phase as i32)
        .bind(event.block_number)
        .bind(event.extrinsic_index)
        .bind(event.index)
        .execute(db_pool)
        .await?;

        // Continue to next phase if there are more rules, otherwise we're done
        match next_phase_with_rules {
            Some(next_phase) => {
                current_phase_num = next_phase;
                event.phase = target_db_phase.into();
            }
            None => break, // All phases complete
        }
    }

    Ok(())
}
