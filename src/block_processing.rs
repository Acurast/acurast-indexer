//! Block processing module.
//!
//! This module handles processing blocks from the chain:
//! - Extracting extrinsics and events
//! - Bulk inserting into the database
//! - Metadata version handling

use std::iter;

use anyhow::anyhow;
use async_channel::Receiver;
use backoff::{future::retry_notify, Error as BackoffError, ExponentialBackoff as Backoff};
use chrono::{DateTime, Utc};
use parity_scale_codec::{Decode, Encode};
use scale_value::{Composite, ValueDef};
use serde_json::value::{RawValue, Value};
pub use serde_json::Value as JsonValue;
use sqlx::{Pool, Postgres};
use std::time::Duration;
use subxt::backend::rpc::reconnecting_rpc_client::RpcClient;
use subxt::backend::{Backend, BackendExt};
use subxt::Metadata;
use subxt::{blocks::BlockRef, utils::H256, OnlineClient, PolkadotConfig};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::entities::{
    Block, EventRow, EventsIndexPhase, ExtrinsicRow, ExtrinsicsIndexPhase, SpecVersionChange,
};
use crate::phase_work::PhaseWorkSenders;
use crate::transformation::ValueWrapper;
use crate::utils::connect_node;

/// Fetch metadata at a specific block hash, returns raw SCALE bytes
async fn fetch_metadata_at_block(
    client: &OnlineClient<PolkadotConfig>,
    block_hash: H256,
) -> Result<Vec<u8>, anyhow::Error> {
    // Call the Metadata_metadata_at_version runtime API directly to get raw SCALE bytes
    let version: u32 = 15;
    let version_encoded = version.encode();

    let raw_result = client
        .backend()
        .call(
            "Metadata_metadata_at_version",
            Some(&version_encoded),
            block_hash,
        )
        .await?;

    // The result is SCALE-encoded Option<OpaqueMetadata>
    // Decode it to extract the metadata bytes
    match Option::<Vec<u8>>::decode(&mut &raw_result[..]) {
        Ok(Some(metadata_bytes)) => Ok(metadata_bytes),
        Ok(None) => Err(anyhow!("Metadata v15 not available at this block")),
        Err(e) => Err(anyhow!("Failed to decode metadata result: {}", e)),
    }
}

pub async fn bulk_insert(
    worker_category: String,
    worker: u32,
    col_block_number: &mut Vec<i64>,
    col_index: &mut Vec<i32>,
    col_pallet: &mut Vec<i32>,
    col_method: &mut Vec<i32>,
    col_data: &mut Vec<JsonValue>,
    col_tx_hash: &mut Vec<String>,
    col_account_id: &mut Vec<String>,
    col_block_time: &mut Vec<DateTime<Utc>>,
    // Event columns for direct insertion to events table
    col_event_extrinsic_block_number: &mut Vec<i64>,
    col_event_extrinsic_index: &mut Vec<i32>,
    col_event_index: &mut Vec<i32>,
    col_event_pallet: &mut Vec<i32>,
    col_event_variant: &mut Vec<i32>,
    col_event_data: &mut Vec<JsonValue>,
    col_event_block_time: &mut Vec<DateTime<Utc>>,
    col_event_phase: &mut Vec<i32>,
    blocks: &mut Vec<Block>,
    spec_version_changes: &mut Vec<SpecVersionChange>,
    db_pool: &Pool<Postgres>,
    tx: &PhaseWorkSenders,
    client: &OnlineClient<PolkadotConfig>,
    epoch_tx: Option<&tokio::sync::mpsc::Sender<(u32, String)>>,
    cancel_token: &CancellationToken,
) -> Result<(), anyhow::Error> {
    let phase: Vec<i32> =
        iter::repeat_n(ExtrinsicsIndexPhase::Raw as i32, col_index.len()).collect();

    // Start a database transaction for all inserts
    let mut db_tx = db_pool.begin().await?;

    // bulk insert extrinsics
    sqlx::query(
        "
            INSERT INTO extrinsics(block_number, index, pallet, method, data, tx_hash, account_id, block_time, phase)
            SELECT * FROM UNNEST($1::bigint[], $2::integer[], $3::integer[], $4::integer[], $5::jsonb[], $6::text[], $7::text[], $8::timestamptz[], $9::integer[])
            ON CONFLICT (block_number, index) DO NOTHING
        ",
    )
        .bind(&col_block_number[..])
        .bind(&col_index[..])
        .bind(&col_pallet[..])
        .bind(&col_method[..])
        .bind(&col_data[..])
        .bind(&col_tx_hash[..])
        .bind(&col_account_id[..])
        .bind(&col_block_time[..])
        .bind(&phase[..])
        .execute(&mut *db_tx)
        .await?;

    // bulk insert events with computed phase
    if !col_event_index.is_empty() {
        sqlx::query(
            "
                INSERT INTO events(block_number, extrinsic_index, index, pallet, variant, data, block_time, phase)
                SELECT * FROM UNNEST($1::bigint[], $2::integer[], $3::integer[], $4::integer[], $5::integer[], $6::jsonb[], $7::timestamptz[], $8::integer[])
                ON CONFLICT (block_number, extrinsic_index, index) DO UPDATE SET
                pallet = EXCLUDED.pallet,
                variant = EXCLUDED.variant,
                data = EXCLUDED.data,
                block_number = EXCLUDED.block_number,
                block_time = EXCLUDED.block_time,
                phase = EXCLUDED.phase;
            ",
        )
        .bind(&col_event_extrinsic_block_number[..])
        .bind(&col_event_extrinsic_index[..])
        .bind(&col_event_index[..])
        .bind(&col_event_pallet[..])
        .bind(&col_event_variant[..])
        .bind(&col_event_data[..])
        .bind(&col_event_block_time[..])
        .bind(&col_event_phase[..])
        .execute(&mut *db_tx)
        .await?;
    }

    debug!(
        worker = format!("{}-{:?}", worker_category, worker),
        "Wrote {:?} extrinsics down to block {:?}",
        col_index.len(),
        blocks.last().unwrap().block_number
    );

    for p in blocks.iter() {
        // Index epoch at this block (uses its own transaction internally)
        if let Err(e) =
            crate::epoch_indexing::index_epoch_at_block(p, db_pool, client, epoch_tx).await
        {
            warn!(
                worker = format!("{}-{:?}", worker_category, worker),
                "Failed to index epoch at block {}: {:?}", p.block_number, e
            );
        }

        // NOTE: the very last thing is to insert the block, so we reprocess the block if any insert statements abort
        sqlx::query!(
            "INSERT INTO blocks (block_number, hash, block_time) VALUES ($1, $2, $3) ON CONFLICT (block_number) DO NOTHING",
            p.block_number,
            p.hash,
            p.block_time
        )
        .execute(&mut *db_tx)
        .await?;
        debug!(
            worker = format!("{}-{:?}", worker_category, worker),
            "Write block progress for {:?} {:?}", p.block_number, p.hash,
        );
    }

    for p in spec_version_changes.iter() {
        // Fetch metadata for this spec version at the block where it was first introduced
        let block_hash = H256::from_slice(
            &hex::decode(&p.block_hash)
                .map_err(|e| anyhow!("Failed to decode block hash: {}", e))?,
        );

        // Fetch metadata v15 at this block hash
        let metadata_bytes: Option<Vec<u8>> =
            match fetch_metadata_at_block(client, block_hash).await {
                Ok(bytes) => Some(bytes),
                Err(e) => {
                    tracing::warn!(
                        "Failed to fetch metadata for spec_version {} at block {}: {}",
                        p.spec_version,
                        p.block_hash,
                        e
                    );
                    None
                }
            };

        sqlx::query!(
            "
                INSERT INTO spec_versions (spec_version, block_number, block_time, metadata)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (spec_version) DO UPDATE
                SET block_number = LEAST(EXCLUDED.block_number, spec_versions.block_number),
                    block_time = CASE WHEN EXCLUDED.block_number < spec_versions.block_number THEN EXCLUDED.block_time ELSE spec_versions.block_time END,
                    metadata = COALESCE(spec_versions.metadata, EXCLUDED.metadata)
            ",
            p.spec_version,
            p.block_number,
            p.block_time,
            metadata_bytes.as_deref(),
        )
        .execute(&mut *db_tx)
        .await?;
        debug!(
            worker = format!("{}-{:?}", worker_category, worker),
            "Write spec version changes for {:?}: spec_version {:?} with metadata: {}",
            p.block_number,
            p.spec_version,
            if metadata_bytes.is_some() {
                "yes"
            } else {
                "no"
            }
        );
    }

    // Commit the transaction
    db_tx.commit().await?;

    // Queue the newly inserted extrinsics for the first extrinsic phase
    for i in 0..col_index.len() {
        if cancel_token.is_cancelled() {
            break;
        }
        let extrinsic = ExtrinsicRow {
            block_number: col_block_number[i],
            index: col_index[i],
            pallet: col_pallet[i],
            method: col_method[i],
            data: Some(col_data[i].clone()),
            tx_hash: col_tx_hash[i].clone(),
            account_id: col_account_id[i].clone(),
            block_time: col_block_time[i],
            phase: ExtrinsicsIndexPhase::Raw,
        };
        if ExtrinsicsIndexPhase::MAX > 0 {
            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => break,
                result = tx.extrinsic.send(extrinsic) => {
                    result.map_err(|e| anyhow!(format!("Failed to queue new extrinsic: {:?}", e)))?;
                }
            }
        }
    }

    // Queue only events that need processing (phase == 0)
    // Events at max_phase (no rules) are already fully processed
    for i in 0..col_event_index.len() {
        if cancel_token.is_cancelled() {
            break;
        }
        // Skip events that don't need processing (already at max_phase)
        if col_event_phase[i] != EventsIndexPhase::Created as i32 {
            continue;
        }
        let event = EventRow {
            block_number: col_event_extrinsic_block_number[i],
            extrinsic_index: col_event_extrinsic_index[i],
            index: col_event_index[i],
            pallet: col_event_pallet[i],
            variant: col_event_variant[i],
            data: Some(col_event_data[i].clone()),
            phase: EventsIndexPhase::Created,
            error: None,
            block_time: col_event_block_time[i],
            pallet_name: None,
            method_name: None,
        };
        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => break,
            result = tx.event.send(event) => {
                result.map_err(|e| anyhow!(format!("Failed to queue new event: {:?}", e)))?;
            }
        }
    }

    blocks.clear();
    spec_version_changes.clear();

    col_index.clear();
    col_pallet.clear();
    col_method.clear();
    col_data.clear();
    col_tx_hash.clear();
    col_account_id.clear();
    col_block_number.clear();
    col_block_time.clear();
    // Clear event columns
    col_event_extrinsic_block_number.clear();
    col_event_extrinsic_index.clear();
    col_event_index.clear();
    col_event_pallet.clear();
    col_event_variant.clear();
    col_event_data.clear();
    col_event_block_time.clear();
    col_event_phase.clear();

    Ok(())
}

/// Fetch the metadata from the node at `block_hash` using the latest stable metadata format version.
async fn fetch_latest_stable_metadata(
    backend: &dyn Backend<PolkadotConfig>,
    block_hash: H256,
) -> Result<Metadata, subxt::Error> {
    // This is the latest stable metadata that subxt can utilize.
    const V15_METADATA_VERSION: u32 = 15;

    // Try to fetch the metadata version.
    if let Ok(bytes) = backend
        .metadata_at_version(V15_METADATA_VERSION, block_hash)
        .await
    {
        return Ok(bytes);
    }

    // If that fails, fetch the metadata V14 using the old API.
    backend.legacy_metadata(block_hash).await
}

#[tracing::instrument(
    skip_all,
    fields(
        worker = format!("{}-{:?}", worker_category, worker),
        block_hash = b.hash().to_string(),
    )
)]
async fn process_block(
    b: BlockRef<H256>,
    worker_category: String,
    worker: u32,
    col_block_number: &mut Vec<i64>,
    col_index: &mut Vec<i32>,
    col_pallet: &mut Vec<i32>,
    col_method: &mut Vec<i32>,
    col_data: &mut Vec<JsonValue>,
    col_tx_hash: &mut Vec<String>,
    col_account_id: &mut Vec<String>,
    col_block_time: &mut Vec<DateTime<Utc>>,
    // Event columns for direct insertion to events table
    col_event_extrinsic_block_number: &mut Vec<i64>,
    col_event_extrinsic_index: &mut Vec<i32>,
    col_event_index: &mut Vec<i32>,
    col_event_pallet: &mut Vec<i32>,
    col_event_variant: &mut Vec<i32>,
    col_event_data: &mut Vec<JsonValue>,
    col_event_block_time: &mut Vec<DateTime<Utc>>,
    col_event_phase: &mut Vec<i32>,
    blocks: &mut Vec<Block>,
    spec_version_changes: &mut Vec<SpecVersionChange>,
    client: &OnlineClient<PolkadotConfig>,
    rpc_client: &RpcClient,
    current_metadata_spec_version: &mut Option<u32>,
) -> Result<(), anyhow::Error> {
    debug!("Process block",);

    let json_string = serde_json::to_string(&vec![&b.hash()])?;

    // Now parse it as RawValue
    let param: Box<RawValue> = RawValue::from_string(json_string)?;
    let runtime_version_at = rpc_client
        .request("state_getRuntimeVersion".to_owned(), Some(param))
        .await?;
    let parsed: Value = serde_json::from_str(runtime_version_at.get()).unwrap();
    let spec_version_at = parsed.get("specVersion").unwrap().as_u64().unwrap() as u32;

    // only load metadata if changed
    let update_metadata = if let Some(v) = current_metadata_spec_version {
        *v != spec_version_at
    } else {
        true
    };
    if update_metadata {
        let metadata = fetch_latest_stable_metadata(client.backend(), b.hash()).await?;
        client.set_metadata(metadata);
        *current_metadata_spec_version = Some(spec_version_at);

        info!(
            "Updated client metadata at block {:?} to spec version {:?}",
            b.hash(),
            spec_version_at
        );
    }

    let block: subxt::blocks::Block<PolkadotConfig, OnlineClient<PolkadotConfig>> =
        client.blocks().at(b.clone()).await?;

    let block_time: u64 = block
        .storage()
        .fetch(&subxt::dynamic::storage("Timestamp", "Now", vec![]))
        .await?
        .ok_or(anyhow!("Got empty timestamp"))?
        .as_type()?;

    debug!(
        "Process block {:?} ({:?}) @{} at spec version {:?}",
        block.number(),
        b.hash(),
        block_time,
        spec_version_at
    );

    let extrinsics = block.extrinsics().await?;

    for ext in extrinsics.iter() {
        // we have to do all potentially erroring parsing first to not write partial columns to the col_X vectors
        let wrapped = ValueWrapper::from(scale_value::Value {
            value: ValueDef::Composite(Composite::unnamed(
                ext.field_values().expect("field_values err").into_values(),
            )),
            context: 0,
        });
        let ext_json = serde_json::to_value(wrapped).map_err(|e| {
            anyhow!(
                "extrinsic {:?} failed with {:?}: {:?}",
                hex::encode(ext.hash()),
                e,
                ext.field_values(),
            )
        })?;
        let block_number_i64 = block.number() as i64;
        let ext_index_i32 = ext.index() as i32;
        let block_time_dt = DateTime::from_timestamp_millis(block_time as i64).unwrap();

        // Process events and push to event columns
        for raw_event in ext.events().await?.iter() {
            if let Ok(e) = raw_event {
                let wrapped = ValueWrapper::from(scale_value::Value {
                    value: ValueDef::Composite(Composite::unnamed(
                        e.field_values().expect("field_values err").into_values(),
                    )),
                    context: 0,
                });
                let event_json = serde_json::to_value(wrapped).map_err(|err| {
                    anyhow!(
                        "extrinsic {:?} -> event {:?} failed with {:?}: {:?}",
                        hex::encode(ext.hash()),
                        e.index(),
                        err,
                        e.field_values(),
                    )
                })?;

                // Push to event columns for direct insertion to events table
                col_event_extrinsic_block_number.push(block_number_i64);
                col_event_extrinsic_index.push(ext_index_i32);
                col_event_index.push(e.index() as i32);
                let pallet = e.pallet_index() as i32;
                let variant = e.variant_index() as i32;
                col_event_pallet.push(pallet);
                col_event_variant.push(variant);
                col_event_data.push(event_json);
                col_event_block_time.push(block_time_dt);
                // Compute phase: if event needs processing, start at 0; otherwise skip to max
                let phase = if crate::config::event_needs_processing(pallet, variant) {
                    EventsIndexPhase::Created as i32
                } else {
                    EventsIndexPhase::MAX as i32
                };
                col_event_phase.push(phase);
            }
        }

        col_block_number.push(block_number_i64);
        col_index.push(ext_index_i32);
        col_pallet.push(ext.pallet_index() as i32);
        col_method.push(ext.variant_index() as i32);
        col_data.push(ext_json);
        col_tx_hash.push(hex::encode(ext.hash()));
        col_account_id.push(
            ext.address_bytes()
                .map(|a| {
                    // Pad to 64 hex chars (32 bytes) in case address is shorter
                    let bytes = &a[1..];
                    format!("{:0>64}", hex::encode(bytes))
                })
                .unwrap_or_default(),
        );
        col_block_time.push(block_time_dt);
    }

    let block_time_dt = DateTime::from_timestamp_millis(block_time as i64).unwrap();
    blocks.push(Block {
        block_number: block.number() as i64,
        hash: hex::encode(block.hash().as_bytes()),
        block_time: block_time_dt,
    });

    if update_metadata {
        // store the change, even if other workers might already detect the change, we clean this up later
        spec_version_changes.push(SpecVersionChange {
            block_number: block.number() as i64,
            block_time: block_time_dt,
            spec_version: spec_version_at as i32,
            block_hash: hex::encode(block.hash().as_bytes()),
        });
    }

    Ok::<(), anyhow::Error>(())
}

pub async fn process_blocks(
    worker_category: String,
    worker: u32,
    receiver: Receiver<BlockRef<H256>>,
    db_pool: Pool<Postgres>,
    tx_phase: PhaseWorkSenders,
    epoch_tx: Option<tokio::sync::mpsc::Sender<(u32, String)>>,
    cancel_token: CancellationToken,
) {
    use crate::task_monitor::TASK_REGISTRY;

    // Register this worker task
    let task_id = TASK_REGISTRY.start(
        format!("Block processor ({})", worker_category),
        Some(worker),
    );

    let result = retry_notify(
        Backoff::default(),
        || {
            let worker_category = worker_category.clone();
            let receiver = receiver.clone();
            let cancel_token = cancel_token.clone();
            let db_pool = db_pool.clone();
            let tx_phase = tx_phase.clone();
            let epoch_tx = epoch_tx.clone();
            async move {
                if cancel_token.is_cancelled() {
                    return Err(BackoffError::permanent(anyhow!("Cancelled")));
                }

                match process_blocks_(
                    worker_category,
                    worker,
                    receiver,
                    db_pool,
                    tx_phase,
                    epoch_tx,
                    cancel_token,
                    task_id,
                )
                .await
                {
                    Ok(_) => Ok(()),
                    Err(e) => Err(BackoffError::transient(e)),
                }
            }
        },
        |err, dur| {
            error!(
                "[{:?}] Backing off for process_blocks due to error: {}",
                dur, err
            );
        },
    )
    .await;

    // Report error and end task
    if let Err(ref e) = result {
        error!("process_blocks stopped permanently: {}", e);
        TASK_REGISTRY.set_detail(task_id, format!("ERROR: {}", e));
    }
    TASK_REGISTRY.end(task_id);
}

/// Index blocks from receiver queue.
async fn process_blocks_(
    worker_category: String,
    worker: u32,
    receiver: Receiver<BlockRef<H256>>,
    db_pool: Pool<Postgres>,
    tx_phase: PhaseWorkSenders,
    epoch_tx: Option<tokio::sync::mpsc::Sender<(u32, String)>>,
    cancel_token: CancellationToken,
    task_id: u64,
) -> Result<(), anyhow::Error> {
    use crate::task_monitor::TASK_REGISTRY;
    let settings = &crate::config::settings().indexer;
    let (client, rpc_client) = connect_node(
        settings.archive_nodes[worker as usize % settings.archive_nodes.len()].clone(),
    )
    .await?;

    // this solution currently requires each column to be its own vector
    // in the future we're aiming to allow binding iterators directly as arrays
    // so you can take a vector of structs and bind iterators mapping to each field
    let mut col_block_number: Vec<i64> = vec![];
    let mut col_index: Vec<i32> = vec![];
    let mut col_pallet: Vec<i32> = vec![];
    let mut col_method: Vec<i32> = vec![];
    let mut col_data: Vec<Value> = vec![];
    let mut col_tx_hash: Vec<String> = vec![];
    let mut col_account_id: Vec<String> = vec![];
    let mut col_block_time: Vec<DateTime<Utc>> = vec![];
    // Event columns for direct insertion to events table
    let mut col_event_extrinsic_block_number: Vec<i64> = vec![];
    let mut col_event_extrinsic_index: Vec<i32> = vec![];
    let mut col_event_index: Vec<i32> = vec![];
    let mut col_event_pallet: Vec<i32> = vec![];
    let mut col_event_variant: Vec<i32> = vec![];
    let mut col_event_data: Vec<JsonValue> = vec![];
    let mut col_event_block_time: Vec<DateTime<Utc>> = vec![];
    let mut col_event_phase: Vec<i32> = vec![];

    let mut blocks: Vec<Block> = vec![];

    let mut spec_version_changes: Vec<SpecVersionChange> = vec![];

    let mut last_insert: Instant = Instant::now();

    let mut current_metadata_spec_version: Option<u32> = None;

    while !cancel_token.is_cancelled() {
        let timeout = Duration::from_secs(20).saturating_sub(last_insert.elapsed());

        // next_block is None exactly when the time-based insert triggered (in second select-clause)
        debug!(
            worker = format!("{}-{:?}", worker_category, worker),
            "Before selecting, I had {:?} blocks",
            blocks.len()
        );
        let mut next_block: Option<BlockRef<H256>> = None;
        tokio::select! {
            biased;

            _ = cancel_token.cancelled() => break,
            maybe_msg = receiver.recv() => {
                if let Ok(msg) = maybe_msg {
                    next_block = Some(msg)
                } else {
                    warn!(worker = format!("{}-{:?}", worker_category, worker), "queue closed");
                    // bulk insert remaining items if any
                    if col_index.len() > 0                    {
                        if let Err(e) = bulk_insert(worker_category.clone(),worker,
                            &mut col_block_number,
                            &mut col_index,
                            &mut col_pallet,
                            &mut col_method,
                            &mut col_data,
                            &mut col_tx_hash,
                            &mut col_account_id,
                            &mut col_block_time,
                            &mut col_event_extrinsic_block_number,
                            &mut col_event_extrinsic_index,
                            &mut col_event_index,
                            &mut col_event_pallet,
                            &mut col_event_variant,
                            &mut col_event_data,
                            &mut col_event_block_time,
                            &mut col_event_phase,
                            &mut blocks,
                            &mut spec_version_changes,
                            &db_pool,
                            &tx_phase,
                            &client,
                            epoch_tx.as_ref(),
                            &cancel_token,
                        ).await {
                            error!("Skipped blocks because bulk insertion failed: {:?}", e);
                        }
                    }
                    break;
                }
            },
            // Trigger insert if 20 seconds passed and there's data
            _ = tokio::time::sleep(timeout), if !col_index.is_empty() => {
                debug!(worker = format!("{}-{:?}", worker_category, worker), "Time-based insert triggered");
            }
        };

        if col_index.len() >= settings.max_blocks_per_bulk_insert
            || (next_block.is_none() && col_index.len() > 0)
        {
            if let Err(e) = bulk_insert(
                worker_category.clone(),
                worker,
                &mut col_block_number,
                &mut col_index,
                &mut col_pallet,
                &mut col_method,
                &mut col_data,
                &mut col_tx_hash,
                &mut col_account_id,
                &mut col_block_time,
                &mut col_event_extrinsic_block_number,
                &mut col_event_extrinsic_index,
                &mut col_event_index,
                &mut col_event_pallet,
                &mut col_event_variant,
                &mut col_event_data,
                &mut col_event_block_time,
                &mut col_event_phase,
                &mut blocks,
                &mut spec_version_changes,
                &db_pool,
                &tx_phase,
                &client,
                epoch_tx.as_ref(),
                &cancel_token,
            )
            .await
            {
                error!("Skipped blocks because bulk insertion failed: {:?}", e);
            }
            last_insert = Instant::now();
        }

        if let Some(b) = next_block {
            // Update task monitor with current block being processed
            if let Ok(block) = client.blocks().at(b.hash()).await {
                TASK_REGISTRY.set_block(task_id, block.number() as u32);
            }
            if let Err(e) = process_block(
                b,
                worker_category.clone(),
                worker,
                &mut col_block_number,
                &mut col_index,
                &mut col_pallet,
                &mut col_method,
                &mut col_data,
                &mut col_tx_hash,
                &mut col_account_id,
                &mut col_block_time,
                &mut col_event_extrinsic_block_number,
                &mut col_event_extrinsic_index,
                &mut col_event_index,
                &mut col_event_pallet,
                &mut col_event_variant,
                &mut col_event_data,
                &mut col_event_block_time,
                &mut col_event_phase,
                &mut blocks,
                &mut spec_version_changes,
                &client,
                &rpc_client,
                &mut current_metadata_spec_version,
            )
            .await
            {
                error!("Skipped block because processing failed: {:?}", e);
            }
        }
    }

    Ok(())
}
