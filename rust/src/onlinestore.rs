use crate::config::RepoConfig;
use crate::proto::feast::serving;
use crate::proto::feast::types;
use anyhow::{Context, Result};
use murmur3::murmur3_32;
use prost::Message;
use prost_types::Timestamp;
use redis::aio::ConnectionLike;
use std::collections::HashMap;
use std::io::Cursor;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisType {
    Node,
    Cluster,
}

#[derive(Debug, Clone)]
pub struct RedisOnlineStoreConfig {
    pub addresses: Vec<String>,
    pub password: Option<String>,
    pub use_tls: bool,
    pub db: i64,
    pub redis_type: RedisType,
}

impl RedisOnlineStoreConfig {
    pub fn from_repo_config(config: &RepoConfig) -> Result<Self> {
        Self::from_online_store_config(&config.online_store)
    }

    pub fn from_online_store_config(online_store: &HashMap<String, serde_yaml::Value>) -> Result<Self> {
        let store_type = online_store
            .get("type")
            .and_then(|value| value.as_str())
            .unwrap_or("sqlite");
        if store_type != "redis" {
            anyhow::bail!("online store type {store_type} is not supported for redis config");
        }

        let redis_type = online_store
            .get("redis_type")
            .and_then(|value| value.as_str())
            .unwrap_or("redis");
        let redis_type = match redis_type {
            "redis" => RedisType::Node,
            "redis_cluster" => RedisType::Cluster,
            _ => anyhow::bail!("redis_type must be 'redis' or 'redis_cluster'"),
        };

        let conn = online_store
            .get("connection_string")
            .and_then(|value| value.as_str())
            .unwrap_or("localhost:6379");

        let mut addresses = Vec::new();
        let mut password = None;
        let mut use_tls = false;
        let mut db = 0;

        for part in conn.split(',') {
            if part.contains('=') {
                let mut kv = part.splitn(2, '=');
                let key = kv.next().unwrap_or("");
                let value = kv.next().unwrap_or("");
                match key {
                    "password" => password = Some(value.to_string()),
                    "ssl" => {
                        use_tls = value.parse::<bool>().context("invalid ssl value")?;
                    }
                    "db" => {
                        db = value.parse::<i64>().context("invalid db value")?;
                    }
                    _ => anyhow::bail!("unrecognized option in connection_string: {key}"),
                }
            } else if part.contains(':') {
                addresses.push(part.to_string());
            } else {
                anyhow::bail!("unable to parse connection_string segment: {part}");
            }
        }

        if addresses.is_empty() {
            anyhow::bail!("connection_string must include at least one address");
        }

        Ok(Self {
            addresses,
            password,
            use_tls,
            db,
            redis_type,
        })
    }
}

#[derive(Debug, Clone)]
pub struct FeatureData {
    pub reference: serving::FeatureReferenceV2,
    pub timestamp: Option<Timestamp>,
    pub value: types::Value,
}

enum RedisConnection {
    Node(redis::Client),
    Cluster(redis::cluster::ClusterClient),
}

pub struct RedisOnlineStore {
    project: String,
    entity_key_serialization_version: i64,
    connection: RedisConnection,
}

impl RedisOnlineStore {
    pub fn new(project: String, config: &RepoConfig) -> Result<Self> {
        let redis_config = RedisOnlineStoreConfig::from_repo_config(config)?;
        let connection = build_redis_connection(&redis_config)?;
        Ok(Self {
            project,
            entity_key_serialization_version: config.entity_key_serialization_version,
            connection,
        })
    }

    pub fn build_feature_view_indices(
        &self,
        feature_view_names: &[String],
        feature_names: &[String],
    ) -> (HashMap<String, usize>, HashMap<usize, String>, usize) {
        let mut feature_view_indices = HashMap::new();
        let mut indices_feature_view = HashMap::new();
        let mut index = feature_names.len();
        for view_name in feature_view_names {
            if !feature_view_indices.contains_key(view_name) {
                feature_view_indices.insert(view_name.clone(), index);
                indices_feature_view.insert(index, view_name.clone());
                index += 1;
            }
        }
        (feature_view_indices, indices_feature_view, index)
    }

    pub fn build_redis_hash_set_keys(
        &self,
        feature_view_names: &[String],
        feature_names: &[String],
        indices_feature_view: &HashMap<usize, String>,
        index: usize,
    ) -> Result<(Vec<Vec<u8>>, Vec<String>)> {
        let feature_count = feature_names.len();
        let mut hset_keys = vec![Vec::new(); index];
        let mut feature_names_with_ts = feature_names.to_vec();

        for i in 0..feature_count {
            let input = format!("{}:{}", feature_view_names[i], feature_names[i]);
            let mut cursor = Cursor::new(input.as_bytes());
            let hash = murmur3_32(&mut cursor, 0)?;
            hset_keys[i] = hash.to_le_bytes().to_vec();
        }

        for i in feature_count..index {
            if let Some(view) = indices_feature_view.get(&i) {
                let ts_key = format!("_ts:{view}");
                hset_keys[i] = ts_key.as_bytes().to_vec();
                feature_names_with_ts.push(ts_key);
            }
        }

        Ok((hset_keys, feature_names_with_ts))
    }

    pub fn build_redis_keys(
        &self,
        entity_keys: &[types::EntityKey],
    ) -> Result<(Vec<Vec<u8>>, HashMap<Vec<u8>, usize>)> {
        let mut redis_keys = Vec::with_capacity(entity_keys.len());
        let mut redis_key_to_entity_index = HashMap::new();

        for (idx, entity_key) in entity_keys.iter().enumerate() {
            let key = build_redis_key(
                &self.project,
                entity_key,
                self.entity_key_serialization_version,
            )?;
            redis_key_to_entity_index.insert(key.clone(), idx);
            redis_keys.push(key);
        }

        Ok((redis_keys, redis_key_to_entity_index))
    }

    pub async fn online_read(
        &self,
        entity_keys: &[types::EntityKey],
        feature_view_names: &[String],
        feature_names: &[String],
    ) -> Result<Vec<Option<Vec<FeatureData>>>> {
        let feature_count = feature_names.len();
        let (feature_view_indices, indices_feature_view, index) =
            self.build_feature_view_indices(feature_view_names, feature_names);
        let (hset_keys, feature_names_with_ts) =
            self.build_redis_hash_set_keys(feature_view_names, feature_names, &indices_feature_view, index)?;
        let (redis_keys, redis_key_to_entity_index) = self.build_redis_keys(entity_keys)?;

        let mut results = vec![None; entity_keys.len()];

        match &self.connection {
            RedisConnection::Node(client) => {
                let mut conn = client.get_multiplexed_tokio_connection().await?;
                for redis_key in &redis_keys {
                    let values = hmget(&mut conn, redis_key, &hset_keys).await?;
                    let row = build_feature_data_row(
                        feature_count,
                        feature_view_names,
                        &feature_names_with_ts,
                        &feature_view_indices,
                        values,
                    )?;
                    let entity_index = redis_key_to_entity_index
                        .get(redis_key)
                        .copied()
                        .unwrap_or_default();
                    results[entity_index] = row;
                }
            }
            RedisConnection::Cluster(client) => {
                let mut conn = client.get_async_connection().await?;
                for redis_key in &redis_keys {
                    let values = hmget(&mut conn, redis_key, &hset_keys).await?;
                    let row = build_feature_data_row(
                        feature_count,
                        feature_view_names,
                        &feature_names_with_ts,
                        &feature_view_indices,
                        values,
                    )?;
                    let entity_index = redis_key_to_entity_index
                        .get(redis_key)
                        .copied()
                        .unwrap_or_default();
                    results[entity_index] = row;
                }
            }
        }

        Ok(results)
    }
}

pub fn build_redis_key(
    project: &str,
    entity_key: &types::EntityKey,
    entity_key_serialization_version: i64,
) -> Result<Vec<u8>> {
    let mut serialized = serialize_entity_key(entity_key, entity_key_serialization_version)?;
    serialized.extend_from_slice(project.as_bytes());
    Ok(serialized)
}

pub fn serialize_entity_key(
    entity_key: &types::EntityKey,
    entity_key_serialization_version: i64,
) -> Result<Vec<u8>> {
    if entity_key.join_keys.len() != entity_key.entity_values.len() {
        anyhow::bail!("join_keys and entity_values length mismatch");
    }

    let mut map = HashMap::new();
    for (key, value) in entity_key.join_keys.iter().zip(&entity_key.entity_values) {
        map.insert(key.clone(), value.clone());
    }

    let mut keys = entity_key.join_keys.clone();
    keys.sort();

    let mut out = Vec::new();

    if entity_key_serialization_version >= 3 {
        let count = keys.len() as u32;
        out.extend_from_slice(&count.to_le_bytes());
    }

    for key in &keys {
        let type_code = types::value_type::Enum::String as u32;
        out.extend_from_slice(&type_code.to_le_bytes());
        let len = key.len() as u32;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(key.as_bytes());
    }

    for key in &keys {
        let value = map
            .get(key)
            .context("missing value for join key")?;
        let (bytes, value_type) = serialize_value(value)?;
        let type_code = value_type as u32;
        out.extend_from_slice(&type_code.to_le_bytes());
        let len = bytes.len() as u32;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&bytes);
    }

    Ok(out)
}

fn serialize_value(value: &types::Value) -> Result<(Vec<u8>, types::value_type::Enum)> {
    match value.val.as_ref() {
        Some(types::value::Val::StringVal(value)) => {
            Ok((value.as_bytes().to_vec(), types::value_type::Enum::String))
        }
        Some(types::value::Val::BytesVal(value)) => {
            Ok((value.clone(), types::value_type::Enum::Bytes))
        }
        Some(types::value::Val::Int32Val(value)) => {
            Ok(((*value as u32).to_le_bytes().to_vec(), types::value_type::Enum::Int32))
        }
        Some(types::value::Val::Int64Val(value)) => {
            Ok(((*value as u64).to_le_bytes().to_vec(), types::value_type::Enum::Int64))
        }
        None => anyhow::bail!("missing value for entity key"),
        _ => anyhow::bail!("unsupported value type for entity key"),
    }
}

async fn hmget<C: ConnectionLike + Send>(
    conn: &mut C,
    key: &[u8],
    fields: &[Vec<u8>],
) -> Result<Vec<Option<Vec<u8>>>> {
    let mut cmd = redis::cmd("HMGET");
    cmd.arg(key);
    for field in fields {
        cmd.arg(field);
    }
    let values = cmd.query_async(conn).await?;
    Ok(values)
}

fn build_feature_data_row(
    feature_count: usize,
    feature_view_names: &[String],
    feature_names_with_ts: &[String],
    feature_view_indices: &HashMap<String, usize>,
    values: Vec<Option<Vec<u8>>>,
) -> Result<Option<Vec<FeatureData>>> {
    let mut row = Vec::with_capacity(feature_count);
    let mut contains_non_nil = false;

    for feature_index in 0..feature_count {
        let feature_name = feature_names_with_ts[feature_index].clone();
        let feature_view_name = feature_view_names[feature_index].clone();
        let timestamp_index = feature_view_indices
            .get(&feature_view_name)
            .copied()
            .unwrap_or(feature_count);

        let timestamp = values
            .get(timestamp_index)
            .and_then(|value| value.as_ref())
            .map(|bytes| Timestamp::decode(bytes.as_slice()))
            .transpose()?;

        let value = if let Some(value_bytes) = values
            .get(feature_index)
            .and_then(|value| value.as_ref())
        {
            contains_non_nil = true;
            types::Value::decode(value_bytes.as_slice())?
        } else {
            types::Value {
                val: Some(types::value::Val::NullVal(types::Null::Null as i32)),
            }
        };

        row.push(FeatureData {
            reference: serving::FeatureReferenceV2 {
                feature_view_name,
                feature_name,
            },
            timestamp,
            value,
        });
    }

    if contains_non_nil {
        Ok(Some(row))
    } else {
        Ok(None)
    }
}

fn build_redis_connection(config: &RedisOnlineStoreConfig) -> Result<RedisConnection> {
    match config.redis_type {
        RedisType::Node => {
            let url = build_redis_url(&config.addresses[0], config.password.as_deref(), config.db, config.use_tls)?;
            let client = redis::Client::open(url)?;
            Ok(RedisConnection::Node(client))
        }
        RedisType::Cluster => {
            let mut urls = Vec::new();
            for addr in &config.addresses {
                let url = build_redis_url(addr, config.password.as_deref(), config.db, config.use_tls)?;
                urls.push(url);
            }
            let client = redis::cluster::ClusterClient::new(urls)?;
            Ok(RedisConnection::Cluster(client))
        }
    }
}

fn build_redis_url(address: &str, password: Option<&str>, db: i64, use_tls: bool) -> Result<String> {
    let (host, port) = split_host_port(address)?;
    let scheme = if use_tls { "rediss" } else { "redis" };
    let auth = password.map(|pw| format!(":{pw}@")).unwrap_or_default();
    let db_segment = if db > 0 { format!("/{db}") } else { String::new() };
    Ok(format!("{scheme}://{auth}{host}:{port}{db_segment}"))
}

fn split_host_port(address: &str) -> Result<(String, u16)> {
    let addr = if let Some(idx) = address.find("://") {
        &address[idx + 3..]
    } else {
        address
    };
    let host_port = addr.split('/').next().unwrap_or(addr);
    let mut parts = host_port.splitn(2, ':');
    let host = parts
        .next()
        .context("missing host in address")?
        .to_string();
    let port = parts
        .next()
        .context("missing port in address")?
        .parse::<u16>()
        .context("invalid port in address")?;
    Ok((host, port))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_entity_key_matches_go() {
        let expected: Vec<u8> = vec![
            1, 0, 0, 0, 2, 0, 0, 0, 9, 0, 0, 0, 100, 114, 105, 118, 101, 114, 95, 105,
            100, 4, 0, 0, 0, 8, 0, 0, 0, 233, 3, 0, 0, 0, 0, 0, 0,
        ];
        let entity_key = types::EntityKey {
            join_keys: vec!["driver_id".to_string()],
            entity_values: vec![types::Value {
                val: Some(types::value::Val::Int64Val(1001)),
            }],
        };

        let out = serialize_entity_key(&entity_key, 3).expect("serialize entity key");
        assert_eq!(out, expected);
    }

    #[test]
    fn build_feature_view_indices_handles_duplicates() {
        let store = test_store();
        let (indices, reverse, index) = store.build_feature_view_indices(
            &vec!["view1".to_string(), "view1".to_string()],
            &vec!["feature1".to_string(), "feature2".to_string()],
        );
        assert_eq!(indices.len(), 1);
        assert_eq!(reverse.len(), 1);
        assert_eq!(index, 3);
        assert_eq!(reverse.get(&2).map(|s| s.as_str()), Some("view1"));
    }

    #[test]
    fn build_redis_hash_set_keys_adds_timestamps() {
        let store = test_store();
        let mut indices = HashMap::new();
        indices.insert(2, "view1".to_string());
        indices.insert(3, "view2".to_string());

        let (_keys, names) = store
            .build_redis_hash_set_keys(
                &vec!["view1".to_string(), "view2".to_string()],
                &vec!["feature1".to_string(), "feature2".to_string()],
                &indices,
                4,
            )
            .expect("hash set keys");
        assert!(names.contains(&"_ts:view1".to_string()));
        assert!(names.contains(&"_ts:view2".to_string()));
    }

    fn test_store() -> RedisOnlineStore {
        let client = redis::Client::open("redis://localhost:6379").expect("redis client");
        RedisOnlineStore {
            project: "project".to_string(),
            entity_key_serialization_version: 3,
            connection: RedisConnection::Node(client),
        }
    }
}
