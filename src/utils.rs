use std::str::FromStr;
use subxt::backend::rpc::reconnecting_rpc_client::{ExponentialBackoff, RpcClient};
use subxt::utils::AccountId32;
use subxt::{OnlineClient, PolkadotConfig};

pub fn strip_hex_prefix(s: &str) -> String {
    if s.starts_with("0x") {
        s[2..].to_string() // Remove the first 2 characters ("0x")
    } else {
        s.to_string() // Return the string as is if "0x" is not found
    }
}

/// Normalize an address to lowercase hex without prefix.
/// Accepts both hex (with or without 0x prefix) and SS58 formats.
pub fn normalize_address(s: &str) -> String {
    let trimmed = s.trim();

    // Try hex first (with or without 0x prefix)
    let hex_str = strip_hex_prefix(trimmed);
    if hex_str.len() == 64 && hex::decode(&hex_str).is_ok() {
        return hex_str.to_lowercase();
    }

    // Try SS58 decoding
    if let Ok(account_id) = AccountId32::from_str(trimmed) {
        return hex::encode(account_id.0);
    }

    // Fallback: return stripped hex as-is
    hex_str.to_lowercase()
}

/// Normalize an address to lowercase hex with 0x prefix.
/// Accepts both hex (with or without 0x prefix) and SS58 formats.
/// Use this for comparing against addresses stored with 0x prefix (e.g., commitments).
pub fn normalize_address_with_prefix(s: &str) -> String {
    let hex_without_prefix = normalize_address(s);
    format!("0x{}", hex_without_prefix)
}

pub fn ensure_hex_prefix(s: &str) -> String {
    if s.starts_with("0x") {
        s.to_string() // Return the string as is if "0x" is found
    } else {
        format!("0x{}", s)
    }
}

pub async fn connect_node(
    url: impl AsRef<str>,
) -> Result<(OnlineClient<PolkadotConfig>, RpcClient), anyhow::Error> {
    let client: RpcClient = RpcClient::builder()
        .retry_policy(
            ExponentialBackoff::from_millis(100)
                .max_delay(std::time::Duration::from_secs(10))
                .take(3),
        )
        .build(&url.as_ref())
        .await?;
    Ok((
        OnlineClient::<PolkadotConfig>::from_rpc_client(client.clone()).await?,
        client,
    ))
}
