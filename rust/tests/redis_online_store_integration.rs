use anyhow::Result;
use feast_rust::config::RepoConfig;
use feast_rust::onlinestore::RedisOnlineStore;
use feast_rust::proto::feast::types;
use prost::Message;
use prost_types::Timestamp;
use redis::aio::MultiplexedConnection;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn repo_config(project: &str, connection_string: &str) -> RepoConfig {
    let mut online_store = HashMap::new();
    online_store.insert(
        "type".to_string(),
        serde_yaml::Value::String("redis".to_string()),
    );
    online_store.insert(
        "connection_string".to_string(),
        serde_yaml::Value::String(connection_string.to_string()),
    );

    RepoConfig {
        project: project.to_string(),
        provider: String::new(),
        registry: None,
        online_store,
        offline_store: HashMap::new(),
        feature_server: HashMap::new(),
        flags: HashMap::new(),
        entity_key_serialization_version: 3,
        repo_path: PathBuf::new(),
    }
}

fn redis_address() -> Option<String> {
    if std::env::var("FEAST_REDIS_TESTS").is_err() {
        return None;
    }
    Some(
        std::env::var("FEAST_REDIS_ADDR").unwrap_or_else(|_| "localhost:6379".to_string()),
    )
}

fn redis_url(addr: &str) -> String {
    if addr.contains("://") {
        addr.to_string()
    } else {
        format!("redis://{addr}")
    }
}

async fn connect_redis(addr: &str) -> Option<MultiplexedConnection> {
    let client = match redis::Client::open(redis_url(addr)) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("redis client error: {err}");
            return None;
        }
    };

    let mut conn = match client.get_multiplexed_tokio_connection().await {
        Ok(conn) => conn,
        Err(err) => {
            eprintln!("redis connection error: {err}");
            return None;
        }
    };

    let ping: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut conn).await;
    if let Err(err) = ping {
        eprintln!("redis ping failed: {err}");
        return None;
    }

    Some(conn)
}

fn unique_project() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    format!("rust_test_{nanos}")
}

#[tokio::test]
async fn redis_online_read_roundtrip() -> Result<()> {
    let Some(address) = redis_address() else {
        eprintln!("set FEAST_REDIS_TESTS=1 to enable Redis integration tests");
        return Ok(());
    };

    let mut conn = match connect_redis(&address).await {
        Some(conn) => conn,
        None => {
            eprintln!("skipping Redis test (unable to connect)");
            return Ok(());
        }
    };

    let project = unique_project();
    let config = repo_config(&project, &address);
    let store = RedisOnlineStore::new(project.clone(), &config)?;

    let entity_key = types::EntityKey {
        join_keys: vec!["driver_id".to_string()],
        entity_values: vec![types::Value {
            val: Some(types::value::Val::Int64Val(1001)),
        }],
    };
    let feature_view_names = vec!["driver_stats".to_string(), "driver_stats".to_string()];
    let feature_names = vec!["rating".to_string(), "trips".to_string()];

    let (_indices, reverse, index) =
        store.build_feature_view_indices(&feature_view_names, &feature_names);
    let (hset_keys, _feature_names_with_ts) = store.build_redis_hash_set_keys(
        &feature_view_names,
        &feature_names,
        &reverse,
        index,
    )?;
    let (redis_keys, _index_map) = store.build_redis_keys(&[entity_key.clone()])?;
    let redis_key = redis_keys
        .get(0)
        .expect("redis key")
        .to_vec();

    let rating_value = types::Value {
        val: Some(types::value::Val::Int64Val(9)),
    };
    let trips_value = types::Value {
        val: Some(types::value::Val::Int64Val(20)),
    };
    let timestamp = Timestamp {
        seconds: 1_700_000_000,
        nanos: 0,
    };

    let mut cmd = redis::cmd("HSET");
    cmd.arg(&redis_key);
    cmd.arg(&hset_keys[0])
        .arg(rating_value.encode_to_vec());
    cmd.arg(&hset_keys[1])
        .arg(trips_value.encode_to_vec());
    cmd.arg(&hset_keys[2])
        .arg(timestamp.encode_to_vec());
    let _: () = cmd.query_async(&mut conn).await?;

    let rows = store
        .online_read(&[entity_key], &feature_view_names, &feature_names)
        .await?;
    assert_eq!(rows.len(), 1);
    let row = rows[0].as_ref().expect("row");
    assert_eq!(row.len(), 2);
    assert_eq!(row[0].reference.feature_view_name, "driver_stats");
    assert_eq!(row[0].reference.feature_name, "rating");
    assert_eq!(row[0].value, rating_value);
    assert_eq!(row[0].timestamp.as_ref(), Some(&timestamp));
    assert_eq!(row[1].reference.feature_name, "trips");
    assert_eq!(row[1].value, trips_value);
    assert_eq!(row[1].timestamp.as_ref(), Some(&timestamp));

    let _: () = redis::cmd("DEL").arg(&redis_key).query_async(&mut conn).await?;
    Ok(())
}
