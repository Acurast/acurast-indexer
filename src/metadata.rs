use once_cell::sync::Lazy;
use serde::Serialize;
use std::collections::BTreeMap;
use subxt::{OnlineClient, PolkadotConfig};
use tokio::sync::RwLock;

// Type definitions
#[derive(Serialize, Clone, Debug)]
pub struct CallIndex {
    pub pallet: u8,
    pub method: u8,
}

pub type MethodMap = BTreeMap<String, CallIndex>;
pub type EventMap = BTreeMap<String, CallIndex>;
pub type PalletMap = BTreeMap<String, (MethodMap, EventMap)>;
pub type Index = (u8, u8);
pub type ReverseMap = BTreeMap<Index, (String, String)>;
pub type PalletIndexMap = BTreeMap<String, u8>;
pub type ReversePalletIndexMap = BTreeMap<u8, String>;
pub type EventsPalletMap = BTreeMap<String, EventMap>;

// Memoized maps
pub static MEMOIZED_EXTRINSICS_MAP: Lazy<RwLock<Option<(u32, PalletMap)>>> =
    Lazy::new(|| RwLock::new(None));

pub static MEMOIZED_EVENTS_MAP: Lazy<RwLock<Option<(u32, EventsPalletMap)>>> =
    Lazy::new(|| RwLock::new(None));

pub static MEMOIZED_EXTRINSICS_REVERSE_MAP: Lazy<RwLock<Option<(u32, ReverseMap)>>> =
    Lazy::new(|| RwLock::new(None));

pub static MEMOIZED_EVENTS_REVERSE_MAP: Lazy<RwLock<Option<(u32, ReverseMap)>>> =
    Lazy::new(|| RwLock::new(None));

pub static MEMOIZED_PALLET_INDEX_MAP: Lazy<RwLock<Option<(u32, PalletIndexMap)>>> =
    Lazy::new(|| RwLock::new(None));

pub static MEMOIZED_REVERSE_PALLET_INDEX_MAP: Lazy<RwLock<Option<(u32, ReversePalletIndexMap)>>> =
    Lazy::new(|| RwLock::new(None));

// Public API functions
pub async fn get_extrinsics_map(client: &OnlineClient<PolkadotConfig>) -> PalletMap {
    let runtime_version = client.runtime_version();
    let spec_version = runtime_version.spec_version;

    {
        let guard = MEMOIZED_EXTRINSICS_MAP.read().await;
        if let Some((cached_version, cached_map)) = &*guard {
            if *cached_version == spec_version {
                return cached_map.to_owned();
            }
        }
    }

    let new_map = fetch_extrinsics_map(client);

    {
        let mut guard = MEMOIZED_EXTRINSICS_MAP.write().await;
        *guard = Some((spec_version, new_map.clone()));
    }

    new_map
}

pub async fn get_events_map(client: &OnlineClient<PolkadotConfig>) -> EventsPalletMap {
    let runtime_version = client.runtime_version();
    let spec_version = runtime_version.spec_version;

    {
        let guard = MEMOIZED_EVENTS_MAP.read().await;
        if let Some((cached_version, cached_map)) = &*guard {
            if *cached_version == spec_version {
                return cached_map.to_owned();
            }
        }
    }

    let new_map = fetch_events_map(client);

    {
        let mut guard = MEMOIZED_EVENTS_MAP.write().await;
        *guard = Some((spec_version, new_map.clone()));
    }

    new_map
}

pub async fn get_extrinsics_reverse_map(client: &OnlineClient<PolkadotConfig>) -> ReverseMap {
    let runtime_version = client.runtime_version();
    let spec_version = runtime_version.spec_version;

    {
        let guard = MEMOIZED_EXTRINSICS_REVERSE_MAP.read().await;
        if let Some((cached_version, cached_map)) = &*guard {
            if *cached_version == spec_version {
                return cached_map.to_owned();
            }
        }
    }

    let new_map = fetch_extrinsics_reverse_map(client);

    {
        let mut guard = MEMOIZED_EXTRINSICS_REVERSE_MAP.write().await;
        *guard = Some((spec_version, new_map.clone()));
    }

    new_map
}

pub async fn get_events_reverse_map(client: &OnlineClient<PolkadotConfig>) -> ReverseMap {
    let runtime_version = client.runtime_version();
    let spec_version = runtime_version.spec_version;

    {
        let guard = MEMOIZED_EVENTS_REVERSE_MAP.read().await;
        if let Some((cached_version, cached_map)) = &*guard {
            if *cached_version == spec_version {
                return cached_map.to_owned();
            }
        }
    }

    let new_map = fetch_events_reverse_map(client);

    {
        let mut guard = MEMOIZED_EVENTS_REVERSE_MAP.write().await;
        *guard = Some((spec_version, new_map.clone()));
    }

    new_map
}

pub async fn get_pallet_index_map(client: &OnlineClient<PolkadotConfig>) -> PalletIndexMap {
    let runtime_version = client.runtime_version();
    let spec_version = runtime_version.spec_version;

    {
        let guard = MEMOIZED_PALLET_INDEX_MAP.read().await;
        if let Some((cached_version, cached_map)) = &*guard {
            if *cached_version == spec_version {
                return cached_map.to_owned();
            }
        }
    }

    let new_map = fetch_pallet_index_map(client);

    {
        let mut guard = MEMOIZED_PALLET_INDEX_MAP.write().await;
        *guard = Some((spec_version, new_map.clone()));
    }

    new_map
}

pub async fn get_reverse_pallet_index_map(
    client: &OnlineClient<PolkadotConfig>,
) -> ReversePalletIndexMap {
    let runtime_version = client.runtime_version();
    let spec_version = runtime_version.spec_version;

    {
        let guard = MEMOIZED_REVERSE_PALLET_INDEX_MAP.read().await;
        if let Some((cached_version, cached_map)) = &*guard {
            if *cached_version == spec_version {
                return cached_map.to_owned();
            }
        }
    }

    let new_map = fetch_reverse_pallet_index_map(client);

    {
        let mut guard = MEMOIZED_REVERSE_PALLET_INDEX_MAP.write().await;
        *guard = Some((spec_version, new_map.clone()));
    }

    new_map
}

// Fetch functions
fn fetch_extrinsics_map(client: &OnlineClient<PolkadotConfig>) -> PalletMap {
    let metadata = client.metadata();

    let mut pallet_map: PalletMap = BTreeMap::new();
    for (_pallet_index, pallet) in metadata.pallets().enumerate() {
        let pallet_name = pallet.name().to_string();

        let mut method_map: MethodMap = BTreeMap::new();
        if let Some(variants) = pallet.call_variants() {
            for (_method_index, variant) in variants.iter().enumerate() {
                let method_name = variant.name.to_string();
                method_map.insert(
                    method_name,
                    CallIndex {
                        pallet: pallet.index(),
                        method: variant.index,
                    },
                );
            }
        }

        let mut event_map: EventMap = BTreeMap::new();
        if let Some(variants) = pallet.event_variants() {
            for (_event_index, variant) in variants.iter().enumerate() {
                let event_name = variant.name.to_string();
                event_map.insert(
                    event_name,
                    CallIndex {
                        pallet: pallet.index(),
                        method: variant.index,
                    },
                );
            }
        }

        pallet_map.insert(pallet_name, (method_map, event_map));
    }
    pallet_map
}

fn fetch_events_map(client: &OnlineClient<PolkadotConfig>) -> EventsPalletMap {
    let mut events_map: EventsPalletMap = BTreeMap::new();

    for pallet in client.metadata().pallets() {
        let pallet_name = pallet.name().to_string();

        let mut event_map: EventMap = BTreeMap::new();
        if let Some(variants) = pallet.event_variants() {
            for variant in variants.iter() {
                let event_name = variant.name.to_string();
                event_map.insert(
                    event_name,
                    CallIndex {
                        pallet: pallet.index(),
                        method: variant.index,
                    },
                );
            }
        }

        // Only include pallets that have events
        if !event_map.is_empty() {
            events_map.insert(pallet_name, event_map);
        }
    }
    events_map
}

fn fetch_extrinsics_reverse_map(client: &OnlineClient<PolkadotConfig>) -> ReverseMap {
    let mut reverse = BTreeMap::new();

    for (pallet_name, (method_map, _event_map)) in fetch_extrinsics_map(client) {
        for (method_name, call_index) in method_map {
            reverse.insert(
                (call_index.pallet, call_index.method),
                (pallet_name.clone(), method_name.clone()),
            );
        }
    }

    reverse
}

fn fetch_events_reverse_map(client: &OnlineClient<PolkadotConfig>) -> ReverseMap {
    let mut reverse = BTreeMap::new();

    for (pallet_name, (_method_map, event_map)) in fetch_extrinsics_map(client) {
        for (event_name, call_index) in event_map {
            reverse.insert(
                (call_index.pallet, call_index.method),
                (pallet_name.clone(), event_name.clone()),
            );
        }
    }

    reverse
}

/// Build a HashMap mapping (pallet_name, method_name) to (pallet_index, method_index)
pub fn build_pallet_method_map(
    reverse_map: &ReverseMap,
) -> std::collections::HashMap<(String, String), (u32, u32)> {
    reverse_map
        .iter()
        .map(|((pallet_idx, method_idx), (pallet_name, method_name))| {
            (
                (pallet_name.clone(), method_name.clone()),
                (*pallet_idx as u32, *method_idx as u32),
            )
        })
        .collect()
}

fn fetch_pallet_index_map(client: &OnlineClient<PolkadotConfig>) -> PalletIndexMap {
    let metadata = client.metadata();
    metadata
        .pallets()
        .map(|pallet| (pallet.name().to_string(), pallet.index()))
        .collect()
}

fn fetch_reverse_pallet_index_map(client: &OnlineClient<PolkadotConfig>) -> ReversePalletIndexMap {
    let metadata = client.metadata();
    metadata
        .pallets()
        .map(|pallet| (pallet.index(), pallet.name().to_string()))
        .collect()
}
