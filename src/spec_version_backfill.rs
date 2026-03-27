///! Backfill metadata for existing spec_versions
///!
///! This module provides functionality to fetch and store metadata for spec_versions
///! that were indexed before the metadata column was added.
use anyhow::anyhow;
use parity_scale_codec::{Decode, Encode};
use sqlx::{Pool, Postgres};
use subxt::{utils::H256, OnlineClient, PolkadotConfig};
use tracing::{info, warn};

/// Backfill metadata for all spec_versions that don't have it yet
pub async fn backfill_spec_version_metadata(
    db_pool: &Pool<Postgres>,
    client: &OnlineClient<PolkadotConfig>,
) -> Result<usize, anyhow::Error> {
    info!("Starting spec_version metadata backfill");

    // Fetch all spec_versions without metadata, joined with blocks to get block hash
    let rows = sqlx::query!(
        r#"
        SELECT sv.spec_version, sv.block_number, b.hash as block_hash
        FROM spec_versions sv
        JOIN blocks b ON sv.block_number = b.block_number
        WHERE sv.metadata IS NULL
        ORDER BY sv.spec_version ASC
        "#
    )
    .fetch_all(db_pool)
    .await?;

    info!(
        "Found {} spec_versions without metadata to backfill",
        rows.len()
    );

    let mut backfilled_count = 0;

    for row in &rows {
        let spec_version = row.spec_version;
        let block_number = row.block_number;
        let block_hash = &row.block_hash;

        info!(
            "Fetching metadata for spec_version {} at block {} (hash: {})",
            spec_version, block_number, block_hash
        );

        // Parse block hash
        let block_hash_bytes = hex::decode(&block_hash)
            .map_err(|e| anyhow!("Failed to decode block hash {}: {}", block_hash, e))?;
        let block_hash_h256 = H256::from_slice(&block_hash_bytes);

        // Call the Metadata_metadata_at_version runtime API directly to get raw SCALE bytes
        // The version parameter needs to be SCALE-encoded
        let version: u32 = 15;
        let version_encoded = version.encode();

        let call_result = client
            .backend()
            .call(
                "Metadata_metadata_at_version",
                Some(&version_encoded),
                block_hash_h256,
            )
            .await;

        match call_result {
            Ok(raw_result) => {
                // The result is SCALE-encoded Option<OpaqueMetadata>
                // Decode it to check if metadata exists
                match Option::<Vec<u8>>::decode(&mut &raw_result[..]) {
                    Ok(Some(metadata_bytes)) => {
                        // Store raw SCALE bytes directly
                        sqlx::query!(
                            "UPDATE spec_versions SET metadata = $1 WHERE spec_version = $2",
                            &metadata_bytes[..],
                            spec_version
                        )
                        .execute(db_pool)
                        .await?;

                        backfilled_count += 1;
                        info!(
                            "Successfully backfilled metadata for spec_version {} ({} bytes)",
                            spec_version,
                            metadata_bytes.len()
                        );
                    }
                    Ok(None) => {
                        warn!(
                            "Metadata v15 not available for spec_version {} at block {}",
                            spec_version, block_hash
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to decode metadata result for spec_version {} at block {}: {}",
                            spec_version, block_hash, e
                        );
                    }
                }
            }
            Err(e) => {
                warn!(
                    "Failed to fetch metadata for spec_version {} at block {}: {}",
                    spec_version, block_hash, e
                );
            }
        }
    }

    info!(
        "Spec_version metadata backfill completed: {} out of {} backfilled",
        backfilled_count,
        rows.len()
    );

    Ok(backfilled_count)
}
