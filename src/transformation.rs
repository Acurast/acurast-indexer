use num_bigint::{BigInt, BigUint, Sign};
use scale_value::{Composite, Primitive, Value, ValueDef};
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};
use std::collections::BTreeMap;

#[derive(Debug)]
pub enum ValueWrapper {
    Null,
    Bool(bool),
    String(String),
    /// The second argument is a helper flag `collabsible` to allow for correcting double-wrapped arrays returned from runtime (or subxt's conversion)
    Hex(String, bool),
    NumberU128(u128),
    NumberI128(i128),
    NumberU256([u8; 32]),
    NumberI256([u8; 32]),
    NumberF64(f64),
    /// The second argument is a helper flag `collabsible` to allow for correcting double-wrapped arrays returned from runtime (or subxt's conversion)
    Array(Vec<ValueWrapper>, bool),
    Object(BTreeMap<String, ValueWrapper>),
}

impl From<Value<u32>> for ValueWrapper {
    fn from(value: Value<u32>) -> Self {
        match value.value {
            ValueDef::Composite(Composite::Named(values)) => {
                let map = values
                    .into_iter()
                    .map(|(name, val)| (name, ValueWrapper::from(val)))
                    .collect();
                ValueWrapper::Object(map)
            }
            ValueDef::Composite(Composite::Unnamed(values)) => {
                // Only hex-encode arrays that are standard byte array sizes (AccountIds, hashes)
                // Single values or non-standard sizes remain as regular arrays for better queryability
                let is_byte_array = !values.is_empty()
                    && values.iter().all(
                        |v| matches!(v.value, ValueDef::Primitive(Primitive::U128(n)) if  n <= 255),
                    );

                if is_byte_array && values.len() > 4 {
                    let bytes: Vec<u8> =
                        values.iter().map(|v| v.as_u128().unwrap() as u8).collect();
                    ValueWrapper::Hex(format!("0x{}", hex::encode(bytes)), true)
                } else {
                    let vec: Vec<ValueWrapper> = values
                        .into_iter()
                        .map(|val| ValueWrapper::from(val))
                        .collect();

                    // correct double nested arrays (returned like this from runtime)
                    if vec.len() == 1 {
                        let single = vec.into_iter().next().unwrap();
                        if let ValueWrapper::Array(inner_vec, true) = single {
                            ValueWrapper::Array(inner_vec, false)
                        } else if let ValueWrapper::Hex(hex, true) = single {
                            ValueWrapper::Hex(hex, false)
                        } else {
                            ValueWrapper::Array(vec![single], true)
                        }
                    } else {
                        ValueWrapper::Array(vec, true)
                    }
                }
            }
            ValueDef::Variant(var) => {
                let wrapped: Vec<ValueWrapper> = var
                    .values
                    .into_values()
                    .map(|v| ValueWrapper::from(v))
                    .collect();
                // correct double nested arrays (returned like this from runtime)
                let value = if wrapped.len() == 1 {
                    wrapped.into_iter().next().unwrap()
                } else {
                    ValueWrapper::Array(wrapped, false)
                };

                // collapse Option enums
                match var.name.as_str() {
                    "Some" => value,
                    "None" => ValueWrapper::Null,
                    _ => ValueWrapper::Object([(var.name, value)].into_iter().collect()),
                }
            }
            ValueDef::BitSequence(bits) => ValueWrapper::String(
                bits.into_iter()
                    .map(|b| if b { '1' } else { '0' })
                    .collect(),
            ),
            ValueDef::Primitive(p) => match p {
                scale_value::Primitive::Bool(b) => ValueWrapper::Bool(b),
                scale_value::Primitive::Char(c) => ValueWrapper::String(c.to_string()),
                scale_value::Primitive::String(s) => ValueWrapper::String(s),
                scale_value::Primitive::U128(n) => ValueWrapper::NumberU128(n),
                scale_value::Primitive::I128(n) => ValueWrapper::NumberI128(n),
                scale_value::Primitive::U256(n) => ValueWrapper::NumberU256(n),
                scale_value::Primitive::I256(n) => ValueWrapper::NumberI256(n),
            },
        }
    }
}

fn bytes32_to_signed_bigint(bytes: &[u8; 32]) -> BigInt {
    let is_negative = bytes[0] & 0x80 != 0; // check if MSB is set (sign bit)

    if is_negative {
        // Two's complement: invert and add 1, then negate
        let mut twos_complement = bytes.clone();
        for byte in twos_complement.iter_mut() {
            *byte = !*byte;
        }

        // Add 1
        for i in (0..32).rev() {
            twos_complement[i] = twos_complement[i].wrapping_add(1);
            if twos_complement[i] != 0 {
                break;
            }
        }

        BigInt::from_bytes_be(Sign::Minus, &twos_complement)
    } else {
        BigInt::from_bytes_be(Sign::Plus, bytes)
    }
}

impl Serialize for ValueWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ValueWrapper::Null => serializer.serialize_none(),
            ValueWrapper::Bool(b) => serializer.serialize_bool(*b),
            ValueWrapper::String(s) => serializer.serialize_str(s),
            ValueWrapper::Hex(s, _) => serializer.serialize_str(s),
            ValueWrapper::NumberU128(n) => serializer.serialize_str(&n.to_string()),
            ValueWrapper::NumberI128(n) => serializer.serialize_str(&n.to_string()),
            ValueWrapper::NumberU256(n) => {
                serializer.serialize_str(&BigUint::from_bytes_be(n).to_str_radix(10))
            }
            ValueWrapper::NumberI256(n) => {
                serializer.serialize_str(&bytes32_to_signed_bigint(n).to_str_radix(10))
            }
            ValueWrapper::NumberF64(n) => serializer.serialize_f64(*n),
            ValueWrapper::Array(arr, _) => {
                let mut seq = serializer.serialize_seq(Some(arr.len()))?;
                for item in arr {
                    seq.serialize_element(item)?;
                }
                seq.end()
            }
            ValueWrapper::Object(map) => {
                let mut m = serializer.serialize_map(Some(map.len()))?;
                for (k, v) in map {
                    m.serialize_entry(k, v)?;
                }
                m.end()
            }
        }
    }
}
