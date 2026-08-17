use std::future::Future;
use std::str::FromStr;
use subxt::backend::rpc::reconnecting_rpc_client::{ExponentialBackoff, RpcClient};
use subxt::utils::AccountId32;
use subxt::{OnlineClient, PolkadotConfig};

tokio::task_local! {
    /// The archive node URL associated with the currently running task.
    /// Set this at the entry point of each node-bound worker via `with_node_url`,
    /// then call `record_node_rpc_call()` at major RPC call sites to attribute
    /// the call to the right node in the metrics dashboard.
    pub static NODE_URL: String;
}

/// Run a future with the given archive node URL set as the task-local context.
/// Any code inside the future can read the URL via `NODE_URL.try_with(...)`.
pub async fn with_node_url<F, R>(url: String, fut: F) -> R
where
    F: Future<Output = R>,
{
    NODE_URL.scope(url, fut).await
}

/// Record an RPC call against the node URL bound to the current task.
/// No-op if there is no URL bound (e.g. tests, ad-hoc bin tools).
pub fn record_node_rpc_call() {
    let _ = NODE_URL.try_with(|url| {
        crate::task_monitor::TASK_REGISTRY.record_node_call(url);
    });
}

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

/// Connect to an archive node and return its URL alongside the clients.
/// The returned URL is used to attribute RPC calls to a specific node
/// in the metrics dashboard.
pub async fn connect_node_with_url(
    url: impl AsRef<str>,
) -> Result<(OnlineClient<PolkadotConfig>, RpcClient, String), anyhow::Error> {
    let url_string = url.as_ref().to_string();
    let (client, rpc_client) = connect_node(&url_string).await?;
    Ok((client, rpc_client, url_string))
}
