//! Shared JSON field parsers used by post-snapshot / event handlers to
//! extract numeric fields from decoded chain values.

use bigdecimal::BigDecimal;
use serde_json::Value as JsonValue;
use std::str::FromStr;

/// Extract u64 from a JSON object field.
pub fn extract_u64(obj: Option<&JsonValue>, field: &str) -> Option<u64> {
    let val = obj?.get(field)?;
    match val {
        JsonValue::Number(n) => n.as_u64(),
        JsonValue::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// Extract a numeric field as `BigDecimal`, defaulting to zero when missing
/// or unparsable. Handles both JSON numbers and decimal strings — the latter
/// is how `ValueWrapper` serializes u128 values.
pub fn extract_numeric_string(obj: Option<&JsonValue>, field: &str) -> BigDecimal {
    let val = match obj.and_then(|o| o.get(field)) {
        Some(v) => v,
        None => return BigDecimal::from(0),
    };
    match val {
        JsonValue::Number(n) => {
            if let Some(u) = n.as_u64() {
                BigDecimal::from(u)
            } else if let Some(i) = n.as_i64() {
                BigDecimal::from(i)
            } else {
                BigDecimal::from(0)
            }
        }
        JsonValue::String(s) => BigDecimal::from_str(s).unwrap_or_else(|_| BigDecimal::from(0)),
        _ => BigDecimal::from(0),
    }
}

/// Extract an optional numeric field as `BigDecimal`.
pub fn extract_optional_numeric_string(obj: Option<&JsonValue>, field: &str) -> Option<BigDecimal> {
    let val = obj?.get(field)?;
    match val {
        JsonValue::Null => None,
        JsonValue::Number(n) => {
            if let Some(u) = n.as_u64() {
                Some(BigDecimal::from(u))
            } else if let Some(i) = n.as_i64() {
                Some(BigDecimal::from(i))
            } else {
                None
            }
        }
        JsonValue::String(s) => BigDecimal::from_str(s).ok(),
        _ => None,
    }
}
