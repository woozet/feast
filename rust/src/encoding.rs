use crate::proto::feast::serving;
use crate::proto::feast::types;
use anyhow::Result;
use base64::Engine;
use chrono::{DateTime, Utc};
use serde_json::{json, Value as JsonValue};
use std::collections::HashMap;

pub fn json_map_to_proto(
    map: &HashMap<String, Vec<JsonValue>>,
) -> Result<HashMap<String, Vec<types::Value>>> {
    let mut result = HashMap::new();
    for (key, values) in map {
        result.insert(key.clone(), json_values_to_proto(values)?);
    }
    Ok(result)
}

pub fn value_to_json(value: &types::Value) -> JsonValue {
    match value.val.as_ref() {
        None => JsonValue::Null,
        Some(types::value::Val::BytesVal(bytes)) => {
            JsonValue::String(base64::engine::general_purpose::STANDARD.encode(bytes))
        }
        Some(types::value::Val::StringVal(value)) => JsonValue::String(value.clone()),
        Some(types::value::Val::Int32Val(value)) => json!(*value),
        Some(types::value::Val::Int64Val(value)) => json!(*value),
        Some(types::value::Val::DoubleVal(value)) => json!(*value),
        Some(types::value::Val::FloatVal(value)) => json!(*value),
        Some(types::value::Val::BoolVal(value)) => json!(*value),
        Some(types::value::Val::BytesListVal(list)) => JsonValue::Array(
            list.val
                .iter()
                .map(|bytes| {
                    JsonValue::String(base64::engine::general_purpose::STANDARD.encode(bytes))
                })
                .collect(),
        ),
        Some(types::value::Val::StringListVal(list)) => json!(list.val),
        Some(types::value::Val::Int32ListVal(list)) => json!(list.val),
        Some(types::value::Val::Int64ListVal(list)) => json!(list.val),
        Some(types::value::Val::DoubleListVal(list)) => json!(list.val),
        Some(types::value::Val::FloatListVal(list)) => json!(list.val),
        Some(types::value::Val::BoolListVal(list)) => json!(list.val),
        Some(types::value::Val::UnixTimestampVal(value)) => json!(*value),
        Some(types::value::Val::UnixTimestampListVal(list)) => {
            JsonValue::Array(list.val.iter().map(|value| json!(*value)).collect())
        }
        Some(types::value::Val::NullVal(_)) => JsonValue::Null,
        Some(types::value::Val::MapVal(map)) => map_to_json(map),
        Some(types::value::Val::MapListVal(list)) => {
            let values = list.val.iter().map(map_to_json).collect::<Vec<_>>();
            JsonValue::Array(values)
        }
        Some(types::value::Val::BytesSetVal(set)) => JsonValue::Array(
            set.val
                .iter()
                .map(|bytes| {
                    JsonValue::String(base64::engine::general_purpose::STANDARD.encode(bytes))
                })
                .collect(),
        ),
        Some(types::value::Val::StringSetVal(set)) => json!(set.val),
        Some(types::value::Val::Int32SetVal(set)) => json!(set.val),
        Some(types::value::Val::Int64SetVal(set)) => json!(set.val),
        Some(types::value::Val::DoubleSetVal(set)) => json!(set.val),
        Some(types::value::Val::FloatSetVal(set)) => json!(set.val),
        Some(types::value::Val::BoolSetVal(set)) => json!(set.val),
        Some(types::value::Val::UnixTimestampSetVal(set)) => json!(set.val),
    }
}

pub fn field_status_to_string(status: serving::FieldStatus) -> String {
    status.as_str_name().to_string()
}

pub fn timestamp_to_rfc3339(timestamp: &prost_types::Timestamp) -> String {
    DateTime::<Utc>::from_timestamp(timestamp.seconds, timestamp.nanos.max(0) as u32)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_default()
}

fn json_values_to_proto(values: &[JsonValue]) -> Result<Vec<types::Value>> {
    let mut result = Vec::with_capacity(values.len());
    for value in values {
        result.push(json_to_value(value)?);
    }
    Ok(result)
}

fn json_to_value(value: &JsonValue) -> Result<types::Value> {
    let val = match value {
        JsonValue::Null => None,
        JsonValue::Bool(value) => Some(types::value::Val::BoolVal(*value)),
        JsonValue::Number(number) => {
            if let Some(value) = number.as_i64() {
                Some(types::value::Val::Int64Val(value))
            } else if let Some(value) = number.as_u64() {
                Some(types::value::Val::Int64Val(value as i64))
            } else {
                Some(types::value::Val::DoubleVal(number.as_f64().unwrap_or(0.0)))
            }
        }
        JsonValue::String(value) => Some(types::value::Val::StringVal(value.clone())),
        JsonValue::Array(values) => Some(json_array_to_list_value(values)?),
        JsonValue::Object(_) => {
            anyhow::bail!("object values are not supported in entities request")
        }
    };

    Ok(types::Value { val })
}

fn json_array_to_list_value(values: &[JsonValue]) -> Result<types::value::Val> {
    enum ListType {
        Bool,
        Int64,
        Double,
        String,
    }

    if values.is_empty() {
        return Ok(types::value::Val::Int64ListVal(types::Int64List {
            val: vec![],
        }));
    }

    let mut list_type: Option<ListType> = None;
    let mut saw_null = false;
    for value in values {
        match value {
            JsonValue::Null => {
                saw_null = true;
                continue;
            }
            JsonValue::Bool(_) => {
                list_type = Some(match list_type {
                    None => ListType::Bool,
                    Some(ListType::Bool) => ListType::Bool,
                    _ => anyhow::bail!("mixed list types are not supported"),
                });
            }
            JsonValue::Number(number) => {
                let is_double = number.as_f64().map(|v| v.fract() != 0.0).unwrap_or(false);
                list_type = Some(match list_type {
                    None => {
                        if is_double {
                            ListType::Double
                        } else {
                            ListType::Int64
                        }
                    }
                    Some(ListType::Int64) => {
                        if is_double {
                            ListType::Double
                        } else {
                            ListType::Int64
                        }
                    }
                    Some(ListType::Double) => ListType::Double,
                    _ => anyhow::bail!("mixed list types are not supported"),
                });
            }
            JsonValue::String(_) => {
                list_type = Some(match list_type {
                    None => ListType::String,
                    Some(ListType::String) => ListType::String,
                    _ => anyhow::bail!("mixed list types are not supported"),
                });
            }
            JsonValue::Array(_) => anyhow::bail!("nested list values are not supported"),
            JsonValue::Object(_) => anyhow::bail!("object values are not supported in lists"),
        }
    }

    match list_type {
        None => {
            if saw_null {
                let list = vec![f64::NAN; values.len()];
                Ok(types::value::Val::DoubleListVal(types::DoubleList {
                    val: list,
                }))
            } else {
                anyhow::bail!("empty list values are not supported")
            }
        }
        Some(ListType::Bool) if saw_null => {
            anyhow::bail!("null values are not supported in bool lists")
        }
        Some(ListType::String) if saw_null => {
            anyhow::bail!("null values are not supported in string lists")
        }
        Some(ListType::Bool) => {
            let list = values
                .iter()
                .map(|value| value.as_bool().unwrap_or(false))
                .collect::<Vec<_>>();
            Ok(types::value::Val::BoolListVal(types::BoolList {
                val: list,
            }))
        }
        Some(ListType::Double) => {
            let list = values
                .iter()
                .map(|value| value.as_f64().unwrap_or(f64::NAN))
                .collect::<Vec<_>>();
            Ok(types::value::Val::DoubleListVal(types::DoubleList {
                val: list,
            }))
        }
        Some(ListType::Int64) if saw_null => {
            let list = values
                .iter()
                .map(|value| value.as_f64().unwrap_or(f64::NAN))
                .collect::<Vec<_>>();
            Ok(types::value::Val::DoubleListVal(types::DoubleList {
                val: list,
            }))
        }
        Some(ListType::Int64) => {
            let list = values
                .iter()
                .map(|value| value.as_i64().unwrap_or(0))
                .collect::<Vec<_>>();
            Ok(types::value::Val::Int64ListVal(types::Int64List {
                val: list,
            }))
        }
        Some(ListType::String) => {
            let list = values
                .iter()
                .map(|value| value.as_str().unwrap_or("").to_string())
                .collect::<Vec<_>>();
            Ok(types::value::Val::StringListVal(types::StringList {
                val: list,
            }))
        }
    }
}

fn map_to_json(map: &types::Map) -> JsonValue {
    let mut obj = serde_json::Map::new();
    for (key, value) in &map.val {
        obj.insert(key.clone(), value_to_json(value));
    }
    JsonValue::Object(obj)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unix_timestamp_formats_like_python_server_json() {
        let value = types::Value {
            val: Some(types::value::Val::UnixTimestampVal(0)),
        };
        assert_eq!(value_to_json(&value), json!(0));
    }
}
