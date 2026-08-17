use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use acurast_indexer::block_processing::process_blocks;
use acurast_indexer::block_queuing::{on_finalized, queue_backwards, queue_gaps, queue_parents_of};
use acurast_indexer::config::get_config;
use acurast_indexer::entities::{
    Block, EpochIndexPhase, EventRow, EventsIndexPhase, ExtrinsicsIndexPhase,
};
use acurast_indexer::epoch_indexing::{queue_epochs_phase, wait_epoch_events_ready};
use acurast_indexer::event_indexing::{
    events_phase_update_channel, events_phase_update_flusher, process_event_phase,
    queue_events_phase, EventPhaseUpdate, EventPhaseUpdateSender,
};
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
            // API pool: cap server-side query time so a timed-out RPC handler
            // can't leave a scan running and pinning snapshots.
            //
            // The server-side limit must be SHORTER than the client-side one
            // (`query_timeout_seconds`, applied by `db_timeout::with_timeout`).
            // It used to be `+ 5`, i.e. longer: when the client timeout fired at
            // 10 s the tokio future was dropped but the backend kept burning I/O
            // for 5 more seconds, and sqlx had to discard the connection because
            // it was left mid-result. Letting Postgres cancel first turns that
            // into a clean 57014 error with the connection returned to the pool
            // and no work outliving the request.
            let api_statement_timeout = format!(
                "{}s",
                settings
                    .server
                    .query_timeout_seconds
                    .saturating_sub(1)
                    .max(1)
            );
            let api_pool = get_db_pool_with_limit(
                settings.server.num_db_connections,
                2,
                "API",
                Some(&api_statement_timeout),
            )
            .await?;
            let phase_pool = get_db_pool_with_limit(
                settings.indexer.num_db_conn_phases,
                10,
                "phase workers",
                None,
            )
            .await?;
            let index_pool = get_db_pool_with_limit(
                settings.indexer.num_workers_backwards
                    + settings.indexer.num_workers_gaps
                    + settings.indexer.num_workers_finalized
                    + 3
                    + 1,
                10,
                "backwards workers",
                None,
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
                anyhow::bail!(
                    "API key not configured. Set ACURAST_INDEXER__AUTH__API_KEY environment variable."
                );
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
            let (indexer_result, tasks_result) = try_join!(indexer, tasks)?;
            indexer_result?;
            tasks_result?;

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
    statement_timeout: Option<&str>,
) -> Result<PgPool, anyhow::Error> {
    let db = &acurast_indexer::config::settings().database;
    let mut options = PgConnectOptions::new()
        .host(&db.host)
        .port(db.port)
        .username(&db.username)
        .password(&db.password)
        .database(&db.database);
    // Server-side timeouts so a client disconnect can't leave a query running.
    // Set only on pools serving short queries (e.g. the API); indexer pools
    // legitimately run long bulk operations.
    if let Some(timeout) = statement_timeout {
        options = options.options([
            ("statement_timeout", timeout),
            ("idle_in_transaction_session_timeout", timeout),
        ]);
    }
    info!("Connecting to Postgres ({})...", pool_name);
    // Never Debug-print `options`: PgConnectOptions' Debug impl includes the
    // password unredacted. Log only non-sensitive connection details.
    debug!(
        "Connecting to Postgres ({}) at {}:{}/{} as {}",
        pool_name, db.host, db.port, db.database, db.username
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
    // Each phase connection is paired with its node URL so that phase workers
    // can attribute RPC calls to the correct archive node in metrics.
    let phase_clients: Vec<(OnlineClient<PolkadotConfig>, String)> = {
        let mut set = tokio::task::JoinSet::new();
        for i in 0..num_conn {
            let node_url = settings.archive_nodes[i % settings.archive_nodes.len()].clone();
            let url_for_return = node_url.clone();
            set.spawn(async move {
                connect_node(node_url)
                    .await
                    .map(|(c, _)| (c, url_for_return))
            });
        }
        let mut clients = Vec::with_capacity(num_conn);
        while let Some(result) = set.join_next().await {
            let (client, url) = result??;
            clients.push((client, url));
        }
        info!("Created {} phase connections", clients.len());
        clients
    };

    // Unified phase processing with priority queues
    // Epochs > Events > Extrinsics (to avoid extrinsics piling up before events are processed)
    let (tx_phase, rx_phase) = phase_work_queues();

    // Serializes the epoch-totals step (`AccountsMaterialized ->
    // EpochTotalsComputed`) process-wide. That step rewrites the shared
    // per-account vesting cohort in `accounts`; two epochs running it at once
    // deadlock in Postgres. The queuer gates to (near) one at a time — only the
    // latest epoch computes totals — and the worker holds this lock across the
    // whole run as the hard guarantee that the write never overlaps. Replaces
    // the former `pg_advisory_xact_lock`.
    let epoch_totals_lock = std::sync::Arc::new(tokio::sync::Mutex::new(()));

    // Channel for epoch insertion notifications (for utilization recalculation)
    // Sends (epoch_number, epoch_start_block, block_hash) when a new epoch is discovered at its start block
    let (epoch_tx, epoch_rx) = tokio::sync::mpsc::channel::<(u32, u32, String)>(100);

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
        epoch_tx,
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

    if !queue_before.is_empty() {
        info!("Spawn worker for priority blocks");
        tracker.spawn(process_blocks(
            "priority".to_string(),
            0,
            rx_before.clone(),
            index_pool.clone(),
            tx_phase.clone(),
            cancel_token.clone(),
        ));

        tracker.spawn(queue_parents_of(
            tx_before.clone(),
            queue_before,
            cancel_token.clone(),
        ));
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
                cancel_token.clone(),
            ));
        }
        // Spawn gap queuer and backwards queuer as separate tasks
        // (they were previously sequential, but queue_gaps never returns)
        let tx_past_gaps = tx_past.clone();
        let backpressure_gaps = tx_phase.extrinsic.clone();
        let index_pool_gaps = index_pool.clone();
        let cancel_token_gaps = cancel_token.clone();
        tracker.spawn(queue_gaps(
            tx_past_gaps,
            backpressure_gaps,
            index_pool_gaps,
            cancel_token_gaps,
            lowest_done_block,
            rx_first_finalized_gaps,
        ));

        let tx_past_backwards = tx_past.clone();
        let backpressure_backwards = tx_phase.extrinsic.clone();
        let index_pool_backwards = index_pool.clone();
        let cancel_token_backwards = cancel_token.clone();
        tracker.spawn(queue_backwards(
            tx_past_backwards,
            backpressure_backwards,
            index_pool_backwards,
            lowest_done,
            cancel_token_backwards,
        ));
    }

    // Batched-update channel for event phase advances. Created up here so the
    // flusher and every worker share the same channel; dropping the master
    // sender below lets the flusher exit cleanly during shutdown.
    let (events_phase_tx, events_phase_rx) = events_phase_update_channel();

    if settings.index_phases {
        // Spawn the events phase-update flusher before the workers so it's
        // ready to receive the moment a worker advances an event.
        tracker.spawn(events_phase_update_flusher(
            events_phase_rx,
            phase_pool.clone(),
            cancel_token.clone(),
        ));

        // Spawn unified phase workers (each worker gets a client from the pool)
        for i in 0..settings.num_workers_phases {
            let conn_idx = i as usize % num_conn;
            let (phase_client, phase_node_url) = phase_clients[conn_idx].clone();
            info!(
                "Spawn phase worker {:?} (using connection {} @ {})",
                i, conn_idx, phase_node_url
            );
            let rx = rx_phase.clone();
            let tx = tx_phase.clone();
            let pool = phase_pool.clone();
            let cancel = cancel_token.clone();
            let latest_finalized = latest_finalized.clone();
            let events_phase_tx = events_phase_tx.clone();
            let epoch_totals_lock = epoch_totals_lock.clone();
            tracker.spawn(acurast_indexer::utils::with_node_url(
                phase_node_url,
                async move {
                    process_phases(
                        i,
                        rx,
                        tx,
                        pool,
                        phase_client,
                        cancel,
                        latest_finalized,
                        events_phase_tx,
                        epoch_totals_lock,
                    )
                    .await
                },
            ));
        }
    }

    // Spawn storage snapshot pruning task (runs periodically)
    tracker.spawn(storage_pruning_task(
        phase_pool.clone(),
        client.clone(),
        cancel_token.clone(),
    ));

    // Spawn queue metrics monitor task (samples channel lengths every
    // second and runs a phase-distribution sample every ~10s).
    let block_channels_for_monitor: Vec<(
        String,
        async_channel::Sender<subxt::blocks::BlockRef<H256>>,
    )> = vec![
        ("finalized".to_string(), tx_finalized.clone()),
        ("priority".to_string(), tx_before.clone()),
        ("backwards".to_string(), tx_past.clone()),
    ];
    tracker.spawn(queue_metrics_monitor_task(
        tx_phase.clone(),
        block_channels_for_monitor,
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

    // Spawn attestation processing task (runs periodically)
    tracker.spawn(attestation_processing_task(
        phase_pool.clone(),
        cancel_token.clone(),
    ));

    // Spawn processor-churn bucket collector (standalone, eventually-correct;
    // walks indexed blocks and fills `processor_active_bucket`, off the API path).
    tracker.spawn(processor_churn_collect_task(
        phase_pool.clone(),
        cancel_token.clone(),
    ));

    if settings.index_phases {
        // Phase queuers skip blocks below `index_phases_from_block` (in addition
        // to the indexer's own `index_from_block` lower bound). Together with
        // the in-memory enqueue gate in `bulk_insert`, this means below-threshold
        // rows are simply never queued for phase processing — without us having
        // to write a "skip" sentinel into the `phase` column that could go stale
        // if MAX is bumped later.
        let phase_index_from = settings
            .index_from_block
            .max(settings.index_phases_from_block);

        // Queue extrinsics - single queue that finds all extrinsics in any phase < max_phase
        tracker.spawn(queue_extrinsics_phase(
            tx_phase.extrinsic.clone(),
            phase_pool.clone(),
            cancel_token.clone(),
            phase_index_from,
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
                phase_index_from,
                None, // no upper limit - follows finalized
            ));
        } else {
            // Multiple queuers - wait for first finalized, then split the range
            let tx_event = tx_phase.event.clone();
            let pool = phase_pool.clone();
            let token = cancel_token.clone();
            let index_from = phase_index_from;
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
        // Queries all intermediate phases (>= 1 and < EpochIndexPhase::MAX)
        tracker.spawn(queue_epochs_phase(
            tx_phase.epoch.clone(),
            phase_pool.clone(),
            cancel_token.clone(),
            phase_index_from,
            epoch_totals_lock.clone(),
        ));

        // Queue epochs - waits for events to be fully indexed before processing Raw epochs
        tracker.spawn(wait_epoch_events_ready(
            phase_pool.clone(),
            cancel_token.clone(),
            phase_index_from,
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
    drop(events_phase_tx);

    Ok(())
}

/// Periodic task to monitor queue sizes and update pending counts
async fn queue_metrics_monitor_task(
    tx_phase: PhaseWorkSenders,
    block_channels: Vec<(String, async_channel::Sender<subxt::blocks::BlockRef<H256>>)>,
    cancel_token: CancellationToken,
) -> Result<(), anyhow::Error> {
    let task_id = TASK_REGISTRY.start("Queue metrics monitor", None);
    const POLL_INTERVAL_SECS: u64 = 1;

    info!(
        "Starting queue metrics monitor (channel poll: {}s)",
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
                // Phase work channels (extrinsic / event / epoch).
                let event_len = tx_phase.event.len() as i64;
                let extrinsic_len = tx_phase.extrinsic.len() as i64;
                let epoch_len = tx_phase.epoch.len() as i64;
                TASK_REGISTRY.set_pending_count(QueueType::Event, event_len);
                TASK_REGISTRY.set_pending_count(QueueType::Extrinsic, extrinsic_len);
                TASK_REGISTRY.set_pending_count(QueueType::Epoch, epoch_len);

                // Block-distribution channels (finalized / priority / backwards).
                for (name, sender) in &block_channels {
                    TASK_REGISTRY.set_block_channel_pending(name, sender.len() as u64);
                }

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
                                // Run storage-location pruning. Pruning is now keyed
                                // exclusively by `(pallet, storage_location)` —
                                // configured via `indexer.storage_pruning`.
                                match acurast_indexer::storage_indexing::prune_storage_snapshots_by_location(
                                    &db_pool,
                                    &acurast_indexer::config::settings().indexer.storage_pruning,
                                    block_number,
                                    BATCH_SIZE,
                                ).await {
                                    Ok(deleted) => {
                                        if deleted > 0 {
                                            info!("Pruned {} storage snapshots", deleted);
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
    mut epoch_rx: tokio::sync::mpsc::Receiver<(u32, u32, String)>,
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
    let min_block_for_incremental = if let Some(scan_progress_block) = sqlx::query_scalar::<_, i64>(
        "SELECT block_number FROM _index_progress WHERE id = 'commitment_scan'",
    )
    .fetch_optional(&db_pool)
    .await
    .ok()
    .flatten()
    {
        info!(
            "Skipping commitment scan: already up-to-date at block {}",
            scan_progress_block
        );
        TASK_REGISTRY.set_detail(
            task_id,
            format!("up-to-date (scanned at block {})", scan_progress_block),
        );
        scan_progress_block + 1
    } else {
        info!(
            "Starting commitment scan at block {}",
            first_finalized_block
        );
        TASK_REGISTRY.set_detail(
            task_id,
            format!("scanning at block {}...", first_finalized_block),
        );

        match acurast_indexer::storage_indexing::scan_all_commitments_at_block(
            &db_pool,
            &client,
            first_finalized_hash,
            Some(task_id),
            &cancel_token,
        )
        .await
        {
            Ok(true) => {
                info!(
                    "Commitment scan complete at block {}",
                    first_finalized_block
                );
                // Record successful completion
                if let Err(e) = sqlx::query(
                        "INSERT INTO _index_progress (id, block_number, completed_at) VALUES ('commitment_scan', $1, NOW())
                         ON CONFLICT (id) DO UPDATE SET block_number = EXCLUDED.block_number, completed_at = NOW()",
                    )
                    .bind(first_finalized_block)
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
        first_finalized_block + 1
    };

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
            Some((epoch, epoch_start, block_hash_hex)) = epoch_rx.recv() => {
                info!("Received new epoch {} at block {}, triggering full commitment rescan", epoch, epoch_start);
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
                                // Record successful completion
                                if let Err(e) = sqlx::query(
                                    "INSERT INTO _index_progress (id, block_number, completed_at) VALUES ('commitment_scan', $1, NOW())
                                     ON CONFLICT (id) DO UPDATE SET block_number = EXCLUDED.block_number, completed_at = NOW()",
                                )
                                .bind(epoch_start as i64)
                                .execute(&db_pool)
                                .await
                                {
                                    error!("Failed to record commitment scan progress: {:?}", e);
                                }
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

/// Periodic task to classify processor attestations (Core/Lite, iOS/Android)
/// from `Acurast.StoredAttestation` snapshots into the `accounts` table.
///
/// Unlike commitment processing, this needs no chain access (the snapshot's
/// `data` column already holds the fully decoded attestation JSON) and no
/// epoch-triggered rescans (attestations aren't epoch-scoped values), so it's
/// a plain incremental poll. The same query also naturally sweeps every
/// pre-existing `StoredAttestation` snapshot on first run — no separate
/// backfill step is needed.
async fn attestation_processing_task(
    db_pool: Pool<Postgres>,
    cancel_token: CancellationToken,
) -> Result<(), anyhow::Error> {
    let task_id = TASK_REGISTRY.start("Attestation processing", None);

    const PROCESS_INTERVAL_SECS: u64 = 30;
    const BATCH_SIZE: i64 = 100;

    info!(
        "Starting attestation processing task (interval: {}s, batch: {})",
        PROCESS_INTERVAL_SECS, BATCH_SIZE
    );
    TASK_REGISTRY.set_detail(task_id, "idle".to_string());

    let mut total_processed: u64 = 0;
    loop {
        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => {
                info!("Attestation processing task cancelled");
                TASK_REGISTRY.end(task_id);
                break;
            }
            _ = tokio::time::sleep(Duration::from_secs(PROCESS_INTERVAL_SECS)) => {
                let snapshot_ids = match acurast_indexer::storage_indexing::find_unprocessed_attestation_snapshots(
                    &db_pool,
                    BATCH_SIZE,
                ).await {
                    Ok(ids) => ids,
                    Err(e) => {
                        error!("Failed to find unprocessed attestation snapshots: {:?}", e);
                        continue;
                    }
                };

                if snapshot_ids.is_empty() {
                    TASK_REGISTRY.set_detail(task_id, format!("idle (total: {})", total_processed));
                    continue;
                }

                TASK_REGISTRY.set_detail(task_id, format!("processing {} snapshots...", snapshot_ids.len()));
                match acurast_indexer::storage_indexing::process_attestation_snapshot_ids(
                    &db_pool,
                    &snapshot_ids,
                ).await {
                    Ok(processed) => {
                        total_processed += processed;
                        if processed > 0 {
                            info!("Processed {} attestations", processed);
                        }
                        TASK_REGISTRY.set_detail(task_id, format!("idle (total: {})", total_processed));
                    }
                    Err(e) => {
                        error!("Failed to process attestation snapshots: {:?}", e);
                        TASK_REGISTRY.set_detail(task_id, format!("error: {:?}", e));
                    }
                }
            }
        }
    }

    Ok(())
}

/// Standalone, eventually-correct collector for the processor-churn buckets
/// (`processor_active_bucket`), fully decoupled from the epoch pipeline. Each pass
/// scans indexed heartbeats above a persisted "sealed" frontier into their
/// calendar quarter/year bucket (idempotent), then advances the frontier over the
/// gap-free prefix. On first run (`sealed = 0`) it fills all history; afterwards
/// it follows the tip. See `processor_churn.rs`.
async fn processor_churn_collect_task(
    db_pool: Pool<Postgres>,
    cancel_token: CancellationToken,
) -> Result<(), anyhow::Error> {
    let task_id = TASK_REGISTRY.start("Processor churn collect", None);
    const INTERVAL_SECS: u64 = 60;

    info!(
        "Starting processor churn collect task (interval: {}s)",
        INTERVAL_SECS
    );
    TASK_REGISTRY.set_detail(task_id, "starting".to_string());

    // Persisted sealed frontier (block_number); missing -> 0 (scan from genesis).
    let mut sealed: i64 =
        sqlx::query_scalar("SELECT block_number FROM _index_progress WHERE id = 'churn_bucket'")
            .fetch_optional(&db_pool)
            .await
            .ok()
            .flatten()
            .unwrap_or(0);

    // Initial pass immediately, then every INTERVAL_SECS.
    run_processor_churn_collect_pass(&db_pool, &mut sealed, task_id).await;

    loop {
        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => {
                info!("Processor churn collect task cancelled");
                TASK_REGISTRY.end(task_id);
                break;
            }
            _ = tokio::time::sleep(Duration::from_secs(INTERVAL_SECS)) => {
                run_processor_churn_collect_pass(&db_pool, &mut sealed, task_id).await;
            }
        }
    }

    Ok(())
}

/// One collection pass: scan `(sealed, tip]` (idempotent — keeps the current
/// bucket fresh and re-captures backwards-filled blocks above the frontier), then
/// advance the sealed frontier over the contiguous prefix. Errors are logged and
/// skipped; the task never aborts.
async fn run_processor_churn_collect_pass(
    db_pool: &Pool<Postgres>,
    sealed: &mut i64,
    task_id: u64,
) {
    let tip: Option<i64> = match sqlx::query_scalar("SELECT max(block_number) FROM blocks")
        .fetch_one(db_pool)
        .await
    {
        Ok(v) => v,
        Err(e) => {
            error!("processor churn: failed to read tip: {:?}", e);
            return;
        }
    };
    let Some(tip) = tip else {
        TASK_REGISTRY.set_detail(task_id, "no blocks yet".to_string());
        return;
    };
    if tip <= *sealed {
        TASK_REGISTRY.set_detail(task_id, format!("up-to-date (sealed {})", *sealed));
        return;
    }

    TASK_REGISTRY.set_detail(task_id, format!("scanning ({}, {}]", *sealed, tip));
    if let Err(e) =
        acurast_indexer::processor_churn::collect_active_processors_for_range(db_pool, *sealed, tip)
            .await
    {
        error!(
            "processor churn: collect ({}, {}] failed: {:?}",
            *sealed, tip, e
        );
        return;
    }

    // Advance the sealed frontier over the gap-free prefix so it is never
    // re-scanned; anything above a hole is re-scanned next pass.
    match acurast_indexer::processor_churn::contiguous_frontier(db_pool, *sealed).await {
        Ok(frontier) if frontier > *sealed => {
            if let Err(e) = sqlx::query(
                "INSERT INTO _index_progress (id, block_number, completed_at) \
                 VALUES ('churn_bucket', $1, NOW()) \
                 ON CONFLICT (id) DO UPDATE SET block_number = EXCLUDED.block_number, completed_at = NOW()",
            )
            .bind(frontier)
            .execute(db_pool)
            .await
            {
                error!("processor churn: persist frontier {} failed: {:?}", frontier, e);
            } else {
                *sealed = frontier;
            }
        }
        Ok(_) => {}
        Err(e) => error!("processor churn: frontier query failed: {:?}", e),
    }
    TASK_REGISTRY.set_detail(task_id, format!("idle (sealed {})", *sealed));
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
    events_phase_tx: EventPhaseUpdateSender,
    epoch_totals_lock: Arc<tokio::sync::Mutex<()>>,
) -> Result<(), anyhow::Error> {
    let mut task = TaskGuard::new("Phase worker", Some(worker_id));

    'outer: loop {
        // Use priority-aware receive (epochs > events > extrinsics)
        let work_item = tokio::select! {
            biased;
            _ = cancel_token.cancelled() => {
                debug!("Phase worker {} cancelled", worker_id);
                break 'outer;
            }
            item = receiver.recv() => {
                match item {
                    Some(item) => item,
                    None => {
                        warn!("all queues closed");
                        break 'outer;
                    }
                }
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
                    &events_phase_tx,
                )
                .await
            }
            PhaseWorkItem::Epoch(epoch) => {
                trace!("Process phase {:?} of epoch {:?}", epoch.phase, epoch.epoch);
                task.set_epoch(epoch.epoch);

                match epoch.phase {
                    EpochIndexPhase::Raw => {
                        // Phase 0: Should not receive Raw epochs here
                        warn!("Received epoch in Raw phase, skipping: {:?}", epoch.epoch);
                        Ok(())
                    }
                    EpochIndexPhase::EventsReady => {
                        // Phase 1: All events in this epoch are fully indexed.
                        // NOTE: return the Result (do NOT `?`) so an epoch error is
                        // caught by the `match res` handler below and the worker
                        // keeps running — the epoch is re-queued and retried.
                        // A bare `?` here propagates out of the worker fn and kills
                        // the task ("task ended unexpectedly").
                        async {
                            // Hardcoded manager indexing (advances to StorageIndexed2)
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
                                epoch,
                                EpochIndexPhase::StorageIndexed2,
                                &db_pool,
                                &client,
                                finalized_block,
                            )
                            .await
                        }
                        .await
                    }
                    EpochIndexPhase::StorageIndexed2 => {
                        // Phase 2: run phase-3 epoch storage rules (e.g. known_accounts).
                        // Bumping the max epoch phase re-queues historically-indexed
                        // epochs through this arm, giving automatic backfill.
                        let finalized_block = {
                            let cached = latest_finalized.load(Ordering::Relaxed);
                            if cached > 0 {
                                Some(cached)
                            } else {
                                None
                            }
                        };

                        // Return the Result (no `?`) so errors are caught below
                        // and the worker survives — see EventsReady note.
                        acurast_indexer::storage_indexing::process_epoch_storage_rules_indexing(
                            worker_id,
                            epoch,
                            EpochIndexPhase::StorageIndexed3,
                            &db_pool,
                            &client,
                            finalized_block,
                        )
                        .await
                    }
                    EpochIndexPhase::StorageIndexed3 => {
                        // Phase 3: materialize accounts from the epoch's snapshots.
                        // Runs after the phase-3 storage rules (e.g. known_accounts)
                        // have landed their snapshots, so the materializer sees a
                        // complete view. Advances the epoch to AccountsMaterialized.
                        acurast_indexer::storage_indexing::process_epoch_accounts_materialization(
                            worker_id, epoch, &db_pool,
                        )
                        .await
                    }
                    EpochIndexPhase::AccountsMaterialized => {
                        // Phase 4: compute per-epoch network-wide totals (vesting,
                        // token-claim, staked, delegated) by iterating chain
                        // storage directly at the epoch's end block.
                        // Advances the epoch to EpochTotalsComputed.
                        //
                        // Hold the process-wide lock across the whole run: this
                        // step rewrites the shared per-account vesting cohort in
                        // `accounts`, which deadlocks if two epochs run it
                        // concurrently. Serial execution replaces the former
                        // pg_advisory_xact_lock. The queuer only ever queues the
                        // latest epoch here, so this rarely contends.
                        let _totals_guard = epoch_totals_lock.lock().await;
                        acurast_indexer::storage_indexing::process_epoch_totals(
                            worker_id, epoch, &db_pool, &client,
                        )
                        .await
                    }
                    EpochIndexPhase::EpochTotalsComputed => {
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
    events_phase_tx: &EventPhaseUpdateSender,
) -> Result<(), anyhow::Error> {
    // Walk every event through every phase up to MAX. There is no
    // rule-based skipping: hardcoded phase hooks (e.g. processor flagging in
    // phase 4) must run regardless of whether the event has a config storage
    // rule, and phases with no matching rule are cheap in-memory no-ops.
    let final_phase = EventsIndexPhase::MAX as u32;

    // Start from the event's current phase
    let mut current_phase_num: u32 = match event.phase {
        EventsIndexPhase::Created => 1,
        EventsIndexPhase::JobsExtracted => 2,
        EventsIndexPhase::StorageIndexed2 => 3,
        EventsIndexPhase::StorageIndexed3 => 4,
        EventsIndexPhase::StorageIndexed4 => return Ok(()), // Already done
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

                // Process deployment events (JobRegistrationStoredV2, JobRegistrationRemoved)
                // Acurast pallet = 40, variant 9 = StoredV2, variant 1 = Removed
                const ACURAST_PALLET: i32 = 40;
                const JOB_REGISTRATION_STORED_V2: i32 = 9;
                const JOB_REGISTRATION_REMOVED: i32 = 1;

                if event.pallet == ACURAST_PALLET {
                    if let Some(ref data) = event.data {
                        let result = match event.variant {
                            JOB_REGISTRATION_STORED_V2 => {
                                // Get block hash for storage lookup
                                let block_hash: Option<(String,)> = sqlx::query_as(
                                    "SELECT hash FROM blocks WHERE block_number = $1",
                                )
                                .bind(event.block_number)
                                .fetch_optional(db_pool)
                                .await?;

                                match block_hash {
                                    Some((hash,)) => {
                                        acurast_indexer::storage_indexing::process_job_registration_stored(
                                            db_pool,
                                            client,
                                            data,
                                            event.block_number,
                                            &hash,
                                            event.block_time,
                                        )
                                        .await
                                    }
                                    None => {
                                        warn!(
                                            "Block hash not found for block {}, skipping deployment processing",
                                            event.block_number
                                        );
                                        Ok(false)
                                    }
                                }
                            }
                            JOB_REGISTRATION_REMOVED => {
                                acurast_indexer::storage_indexing::process_job_registration_removed(
                                    db_pool,
                                    data,
                                    event.block_number,
                                    event.block_time,
                                )
                                .await
                            }
                            _ => Ok(false),
                        };

                        if let Err(e) = result {
                            warn!(
                                "Failed to process deployment event at block {}: {:?}",
                                event.block_number, e
                            );
                        }
                    }
                }
            }
            2 | 3 | 4 => {
                // Phase 2: primary storage indexing (rules with `phase: 2`, the default).
                // Phase 3: follow-up storage indexing (rules with `phase: 3`).
                // Phase 4: follow-up storage indexing (rules with `phase: 4`) plus
                // hardcoded event-derived denormalization hooks (see below).
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

                // Phase 4 also hosts hardcoded event-derived side effects.
                // Placed here (not phase 1) so a production MAX bump from 3->4
                // sweeps the existing backlog through this pass with no event
                // reset: phase-3 rows advance to 4 and run this hook once.
                if current_phase_num == 4 {
                    // Flag processor accounts from ProcessorManager events
                    // (ProcessorPaired / ProcessorHeartbeatWithVersion /
                    // ProcessorPairedV2 — processor account at data[0]). The
                    // pallet/variant matching lives in a pure, unit-tested
                    // helper so the mainnet-specific variant numbers can't
                    // silently drift out of sync again.
                    if let Some(ref data) = event.data {
                        if let Some(who) =
                            acurast_indexer::storage_indexing::processor_account_from_event(
                                event.pallet,
                                event.variant,
                                data,
                            )
                        {
                            if let Err(e) = acurast_indexer::storage_indexing::flag_processor(
                                db_pool,
                                who,
                                event.block_number,
                                event.block_time,
                            )
                            .await
                            {
                                warn!(
                                    "Failed to flag processor account {} at block {}: {:?}",
                                    who, event.block_number, e
                                );
                            }
                        }
                    }
                }
            }
            _ => break, // Beyond max phase
        }

        // Sequential advance: the step we just ran (`current_phase_num`) maps
        // 1:1 onto the DB phase it completes (step 1 -> JobsExtracted, ...,
        // step 4 -> StorageIndexed4). Persist it, then step to the next phase.
        let completed_db_phase = current_phase_num.min(final_phase);

        // Forward-only phase advance handed off to the batching flusher.
        // The flusher coalesces conflicting updates (higher phase wins) and
        // its UPDATE includes `WHERE e.phase < u.new_phase` so a stale entry
        // can never regress the row's persisted phase or clobber a later-phase
        // error. See extrinsic_indexing.rs for the original single-row
        // rationale.
        events_phase_tx
            .send(EventPhaseUpdate {
                block_number: event.block_number,
                index: event.index,
                new_phase: completed_db_phase as i32,
            })
            .await
            .map_err(|e| anyhow::anyhow!("events phase batcher gone: {}", e))?;

        if current_phase_num >= final_phase {
            break; // All phases complete
        }
        current_phase_num += 1;
        event.phase = completed_db_phase.into();
    }

    Ok(())
}
