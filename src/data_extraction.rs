use anyhow::anyhow;
use serde_json::Value;
use sqlx::types::JsonValue;
use std::collections::HashMap;
use tracing::{debug, trace};

pub fn extract<'a>(
    json: &'a JsonValue,
    path: &str,
) -> Result<(String, String, i32), anyhow::Error> {
    let results = resolve_json_path(json, path)?;
    if let Some(e) = results.first() {
        let e = e.as_array().unwrap();

        let (chain, address) = if e[0].is_object() {
            let (key, value) = e[0].as_object().unwrap().into_iter().next().unwrap();
            (key.to_owned(), value.as_str().unwrap().to_owned())
        } else {
            ("Acurast".to_owned(), e[0].as_str().unwrap().to_owned())
        };
        Ok((
            chain,
            address,
            e[1].as_str().unwrap().parse::<i32>().unwrap(),
        ))
    } else {
        Err(anyhow!("could not find path {:?} in event.data", path))
    }
}

/// Extract account address from a value, handling MultiAddress enum with "Id" field
/// like {"Id": "0x..."}. Falls back to JSON serialization for other formats.
pub fn extract_account_address(value: &Value) -> String {
    // If it's a string, return it directly
    if let Some(s) = value.as_str() {
        return s.to_owned();
    }

    // If it's an object with "Id" field (MultiAddress::Id variant)
    if let Some(obj) = value.as_object() {
        if let Some(id_value) = obj.get("Id") {
            if let Some(s) = id_value.as_str() {
                return s.to_owned();
            }
        }
    }

    // Fallback: serialize the entire value as JSON
    serde_json::to_string(value).unwrap_or_default()
}

/// Represents an inner call extracted from a batch or the original call itself
#[derive(Debug, Clone)]
pub struct CallInfo {
    pub pallet: u32,
    pub method: u32,
    pub data: Value,
}

/// Extract calls from extrinsic data, unwrapping utility batch calls if present.
/// Utility pallet batch methods (batch, batchAll, forceBatch) wrap multiple calls.
///
/// Returns a vector of CallInfo and if it was a batch call:
/// - For regular calls: returns single CallInfo with the original pallet/method/data
/// - For batch calls: returns multiple CallInfo, one for each inner call
///
/// `pallet_method_map`: HashMap mapping (pallet_name, method_name) to (pallet_idx, method_idx)
pub fn extract_calls(
    pallet: u32,
    method: u32,
    data: &Value,
    pallet_method_map: &HashMap<(String, String), (u32, u32)>,
) -> (Vec<CallInfo>, bool) {
    const UTILITY_PALLET: u32 = 8;

    // Utility pallet methods:
    // 0 = batch
    // 1 = as_derivative
    // 2 = batch_all
    // 3 = dispatch_as
    // 4 = force_batch
    // 5 = with_weight
    // 6 = if_else
    // 7 = dispatch_as_fallible

    // Check if this is a batch call from utility pallet
    if pallet == UTILITY_PALLET && (method == 0 || method == 2 || method == 4) {
        // Batch calls have structure: [calls_array]
        // where calls_array is an array of call objects
        if let Some(calls_array) = data.as_array() {
            debug!("found batch call, extracted {} calls", calls_array.len());
            let mut result = Vec::new();

            for call in calls_array {
                // Structure: {"PalletName": {"method_name": [args...]}}
                if let Some(call_obj) = call.as_object() {
                    if let Some((pallet_name, method_obj_value)) = call_obj.iter().next() {
                        if let Some(method_obj) = method_obj_value.as_object() {
                            if let Some((method_name, args)) = method_obj.iter().next() {
                                trace!("found batch call with method name: {:?}", method_name);

                                // Look up pallet and method indices from the map
                                let (inner_pallet, inner_method) = pallet_method_map
                                    .get(&(pallet_name.clone(), method_name.clone()))
                                    .copied()
                                    .unwrap_or((0, 0)); // Default if not found

                                result.push(CallInfo {
                                    pallet: inner_pallet,
                                    method: inner_method,
                                    data: args.clone(),
                                });
                            }
                        }
                    }
                }
            }

            if !result.is_empty() {
                trace!("Extracted {} calls from batch", result.len());
                return (result, true);
            }
        }
    }

    // Not a batch call, or couldn't unwrap - return original call
    (
        vec![CallInfo {
            pallet,
            method,
            data: data.clone(),
        }],
        false,
    )
}

/// Resolve values from `json` using a dot-path like "property.[0].id" or "items.[].name"
/// Returns a vector of matching values. The `[]` wildcard iterates over all array elements.
pub fn resolve_json_path<'a>(
    json: &'a JsonValue,
    path: &str,
) -> Result<Vec<&'a Value>, anyhow::Error> {
    Ok(resolve_json_path_with_resolved_paths(json, path)?
        .into_iter()
        .map(|(v, _)| v)
        .collect())
}

/// Resolve values from `json` and return both the values and their resolved paths
/// For example, path "[].sources.[].source" might resolve to:
/// - (value1, "[0].sources.[0].source")
/// - (value2, "[0].sources.[1].source")
/// - (value3, "[1].sources.[0].source")
///
/// Returns Ok(vec![]) for empty arrays (valid case)
/// Returns Err for actual resolution failures (wrong key, type mismatch, etc.)
pub fn resolve_json_path_with_resolved_paths<'a>(
    json: &'a JsonValue,
    path: &str,
) -> Result<Vec<(&'a Value, String)>, anyhow::Error> {
    debug!("resolve_json_path {:?} in {:?}", path, json);
    if path.is_empty() {
        return Ok(vec![(json, String::new())]);
    }

    resolve_json_path_recursive_with_paths(vec![(json, String::new())], path)
}

fn resolve_json_path_recursive_with_paths<'a>(
    current_values_with_paths: Vec<(&'a Value, String)>,
    remaining_path: &str,
) -> Result<Vec<(&'a Value, String)>, anyhow::Error> {
    if remaining_path.is_empty() {
        return Ok(current_values_with_paths);
    }

    let parts: Vec<&str> = remaining_path.split('.').collect();
    if parts.is_empty() {
        return Ok(current_values_with_paths);
    }

    let first_part = parts[0];
    let rest_path = if parts.len() > 1 {
        parts[1..].join(".")
    } else {
        String::new()
    };

    let mut next_values = Vec::new();
    let mut encountered_empty_array = false;
    let mut errors: Vec<String> = Vec::new();

    for (current, current_path) in current_values_with_paths.iter() {
        if first_part.is_empty() {
            next_values.push((*current, current_path.clone()));
            continue;
        }

        // Check for array wildcard "[]"
        if first_part == "[]" {
            if let Some(arr) = current.as_array() {
                if arr.is_empty() {
                    // Empty array is valid, not a failure
                    encountered_empty_array = true;
                } else {
                    for (idx, item) in arr.iter().enumerate() {
                        let new_path = if current_path.is_empty() {
                            format!("[{}]", idx)
                        } else {
                            format!("{}.[{}]", current_path, idx)
                        };
                        next_values.push((item, new_path));
                    }
                }
            } else {
                errors.push(format!(
                    "at '{}': expected array for '[]' but got {}",
                    current_path,
                    value_type_name(current)
                ));
            }
        } else if let Some(index) = parse_array_index(first_part) {
            if let Some(value) = current.get(index) {
                let new_path = if current_path.is_empty() {
                    format!("[{}]", index)
                } else {
                    format!("{}.[{}]", current_path, index)
                };
                next_values.push((value, new_path));
            } else {
                errors.push(format!(
                    "at '{}': index {} out of bounds for array of length {}",
                    current_path,
                    index,
                    current.as_array().map(|a| a.len()).unwrap_or(0)
                ));
            }
        } else if let Some((key, Some(index))) = parse_indexed_key(first_part) {
            // Step into object then array
            if let Some(obj_value) = current.get(key) {
                if let Some(value) = obj_value.get(index) {
                    let new_path = if current_path.is_empty() {
                        if key.is_empty() {
                            format!("[{}]", index)
                        } else {
                            format!("{}[{}]", key, index)
                        }
                    } else {
                        if key.is_empty() {
                            format!("{}.[{}]", current_path, index)
                        } else {
                            format!("{}.{}[{}]", current_path, key, index)
                        }
                    };
                    next_values.push((value, new_path));
                } else {
                    errors.push(format!(
                        "at '{}': index {} out of bounds for '{}'",
                        current_path, index, key
                    ));
                }
            } else {
                errors.push(format!("at '{}': key '{}' not found", current_path, key));
            }
        } else if first_part.ends_with("[]") {
            // Handle "key[]" syntax
            let key = &first_part[..first_part.len() - 2];
            if let Some(obj_value) = current.get(key) {
                if let Some(arr) = obj_value.as_array() {
                    if arr.is_empty() {
                        // Empty array is valid, not a failure
                        encountered_empty_array = true;
                    } else {
                        for (idx, item) in arr.iter().enumerate() {
                            let new_path = if current_path.is_empty() {
                                format!("{}[{}]", key, idx)
                            } else {
                                format!("{}.{}[{}]", current_path, key, idx)
                            };
                            next_values.push((item, new_path));
                        }
                    }
                } else {
                    errors.push(format!(
                        "at '{}': expected array for '{}[]' but got {}",
                        current_path,
                        key,
                        value_type_name(obj_value)
                    ));
                }
            } else {
                errors.push(format!("at '{}': key '{}' not found", current_path, key));
            }
        } else {
            if let Some(value) = current.get(first_part) {
                let new_path = if current_path.is_empty() {
                    first_part.to_string()
                } else {
                    format!("{}.{}", current_path, first_part)
                };
                next_values.push((value, new_path));
            } else {
                errors.push(format!(
                    "at '{}': key '{}' not found",
                    current_path, first_part
                ));
            }
        }
    }

    if next_values.is_empty() {
        // Empty result due to empty arrays is expected, not a failure
        if encountered_empty_array {
            return Ok(vec![]);
        }
        // Return error for actual failures with all collected errors
        return Err(anyhow!(
            "could not resolve path segment '{}' from remaining path '{}': [{}]",
            first_part,
            remaining_path,
            errors.join("; ")
        ));
    }

    resolve_json_path_recursive_with_paths(next_values, &rest_path)
}

fn value_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Parse keys like "property[2]" or "[2]" into (key, index)
fn parse_indexed_key(s: &str) -> Option<(&str, Option<usize>)> {
    if let Some(open) = s.find('[') {
        if s.ends_with(']') {
            let key = &s[..open];
            let index_str = &s[open + 1..s.len() - 1];
            if let Ok(index) = index_str.parse::<usize>() {
                return Some((key, Some(index)));
            }
        }
    }
    None
}

/// Parse "[2]" into Some(2)
fn parse_array_index(s: &str) -> Option<usize> {
    if s.starts_with('[') && s.ends_with(']') {
        let inside = &s[1..s.len() - 1];
        return inside.parse::<usize>().ok();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_resolve_json_path_simple() {
        let data = json!({
            "name": "test",
            "value": 42
        });
        let results = resolve_json_path(&data, "name").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_str(), Some("test"));
    }

    #[test]
    fn test_resolve_json_path_array_index() {
        let data = json!({
            "items": [1, 2, 3]
        });
        let results = resolve_json_path(&data, "items.[1]").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_i64(), Some(2));
    }

    #[test]
    fn test_resolve_json_path_wildcard() {
        let data = json!({
            "items": [
                {"name": "a"},
                {"name": "b"},
                {"name": "c"}
            ]
        });
        let results = resolve_json_path(&data, "items.[].name").unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_str(), Some("a"));
        assert_eq!(results[1].as_str(), Some("b"));
        assert_eq!(results[2].as_str(), Some("c"));
    }

    #[test]
    fn test_resolve_json_path_wildcard_syntax_alternative() {
        let data = json!({
            "sources": [
                {"source": "addr1"},
                {"source": "addr2"}
            ]
        });
        let results = resolve_json_path(&data, "sources.[].source").unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].as_str(), Some("addr1"));
        assert_eq!(results[1].as_str(), Some("addr2"));
    }

    #[test]
    fn test_resolve_json_path_nested() {
        let data = json!({
            "outer": {
                "inner": {
                    "value": "deep"
                }
            }
        });
        let results = resolve_json_path(&data, "outer.inner.value").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_str(), Some("deep"));
    }

    #[test]
    fn test_resolve_json_path_empty() {
        let data = json!({"name": "test"});
        let results = resolve_json_path(&data, "").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], &data);
    }

    #[test]
    fn test_resolve_json_path_not_found() {
        let data = json!({"name": "test"});
        let result = resolve_json_path(&data, "missing");
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_json_path_empty_array() {
        let data = json!({"items": []});
        let results = resolve_json_path(&data, "items.[]").unwrap();
        assert_eq!(results.len(), 0); // Empty array, valid result
    }

    #[test]
    fn test_resolve_json_path_empty_array_with_remaining_path() {
        let data = json!({"items": []});
        let results = resolve_json_path(&data, "items.[].name").unwrap();
        assert_eq!(results.len(), 0); // Empty array, valid result - no error
    }

    #[test]
    fn test_extract_account_address_id_variant() {
        let value =
            json!({"Id": "0xc42b4d1f2ba15a21ed09316146320162187c27aa27a1614e80af1119ca31a4ec"});
        let result = extract_account_address(&value);
        assert_eq!(
            result,
            "0xc42b4d1f2ba15a21ed09316146320162187c27aa27a1614e80af1119ca31a4ec"
        );
    }

    #[test]
    fn test_extract_account_address_direct_string() {
        let value = json!("0xc42b4d1f2ba15a21ed09316146320162187c27aa27a1614e80af1119ca31a4ec");
        let result = extract_account_address(&value);
        assert_eq!(
            result,
            "0xc42b4d1f2ba15a21ed09316146320162187c27aa27a1614e80af1119ca31a4ec"
        );
    }

    #[test]
    fn test_extract_account_address_fallback_other_variant() {
        let value = json!({"Address32": "0x1234567890123456789012345678901234567890123456789012345678901234"});
        let result = extract_account_address(&value);
        // Should fall back to JSON serialization
        assert!(result.contains("Address32"));
        assert!(
            result.contains("0x1234567890123456789012345678901234567890123456789012345678901234")
        );
    }

    #[test]
    fn test_extract_account_address_fallback_complex() {
        let value = json!({"custom": "value", "multiple": "keys"});
        let result = extract_account_address(&value);
        // Should fall back to JSON serialization
        assert!(result.contains("custom"));
    }

    #[test]
    fn test_resolve_json_path_nested_arrays_failing_case() {
        // This is the actual failing data from the log
        let data = json!([
            "60187",
            [
                [
                    "0x934222c8ee48591ed088243edc1a39d6b9a7ceb7e126f982549ef51470810165",
                    {
                        "public_key": "0x02ca4a3e69251d69d96cf3fca4dd665409edc43dbf8ea643cfd955aa36c0e949f3",
                        "variables": [
                            ["0x494e465552415f4150495f4b4559", "0x58bbeca6991e05b1cd51c0a96d3e68d749e5165b9ab4b35eead7b4d6ccc42f0ded13eb46285b9ee0ea7a9c38711f858735122b65e6966d75c1321c51"],
                            ["0x4f5241434c455f4150495f4b4559", "0xa571b999c7fde878801db4c7fc53b387bcd845d7e2f919fa2c0bae2a5ea2b0475a8eb7813015773b1a95b6ac2475a2beda3544896d8d0b1643014bf1bde674e7"]
                        ]
                    }
                ]
            ]
        ]);

        // Test path: "[1][][0]"
        // [1] -> get second element (the nested array)
        // [] -> iterate over each sub-array
        // [0] -> get first element of each sub-array
        let _results = resolve_json_path(&data, "[1].[].{0}");

        // Let's test step by step
        let step1 = resolve_json_path(&data, "[1]").unwrap();
        assert_eq!(step1.len(), 1);

        let step2 = resolve_json_path(&data, "[1].[]").unwrap();
        assert_eq!(step2.len(), 1); // Should have 1 sub-array

        // The actual failing path from config
        let _step3 = resolve_json_path(&data, "[1].[].{0}");
    }

    #[test]
    fn test_resolve_json_path_double_array_wildcard() {
        // Simpler test case for double wildcard
        let data = json!({
            "items": [
                [1, 2, 3],
                [4, 5, 6],
                [7, 8, 9]
            ]
        });

        // Should get first element of each nested array
        let results = resolve_json_path(&data, "items.[].[0]").unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_i64(), Some(1));
        assert_eq!(results[1].as_i64(), Some(4));
        assert_eq!(results[2].as_i64(), Some(7));
    }

    #[test]
    fn test_resolve_json_path_top_level_array_index() {
        // Test accessing first element of a top-level array using [0] path
        let data = json!(["552", "1466288941149563"]);

        let results = resolve_json_path(&data, "[0]").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_str(), Some("552"));
    }
}
