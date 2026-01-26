use anyhow::{Context, Result};
use aws_sdk_s3::ByteStream;
use feast_rust::config::RegistryConfig;
use feast_rust::proto::feast::core;
use feast_rust::proto::feast::types;
use feast_rust::registry::Registry;
use prost::Message;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

fn build_registry() -> core::Registry {
    let entity = core::Entity {
        spec: Some(core::EntitySpecV2 {
            name: "driver".to_string(),
            join_key: "driver_id".to_string(),
            value_type: types::value_type::Enum::Int64 as i32,
            ..Default::default()
        }),
        ..Default::default()
    };

    let rating = core::FeatureSpecV2 {
        name: "rating".to_string(),
        value_type: types::value_type::Enum::Int64 as i32,
        ..Default::default()
    };
    let trips = core::FeatureSpecV2 {
        name: "trips".to_string(),
        value_type: types::value_type::Enum::Int64 as i32,
        ..Default::default()
    };

    let feature_view = core::FeatureView {
        spec: Some(core::FeatureViewSpec {
            name: "driver_stats".to_string(),
            project: "test_project".to_string(),
            entities: vec!["driver".to_string()],
            features: vec![rating.clone(), trips.clone()],
            ..Default::default()
        }),
        ..Default::default()
    };

    let projection = core::FeatureViewProjection {
        feature_view_name: "driver_stats".to_string(),
        feature_columns: vec![rating, trips],
        ..Default::default()
    };
    let feature_service = core::FeatureService {
        spec: Some(core::FeatureServiceSpec {
            name: "driver_service".to_string(),
            project: "test_project".to_string(),
            features: vec![projection],
            ..Default::default()
        }),
        ..Default::default()
    };

    core::Registry {
        entities: vec![entity],
        feature_views: vec![feature_view],
        feature_services: vec![feature_service],
        ..Default::default()
    }
}

fn s3_test_target() -> Option<(String, String)> {
    if std::env::var("FEAST_S3_TESTS").is_err() {
        eprintln!("set FEAST_S3_TESTS=1 to enable S3 registry integration test");
        return None;
    }

    let bucket = match std::env::var("FEAST_S3_BUCKET") {
        Ok(bucket) if !bucket.is_empty() => bucket,
        _ => {
            eprintln!("set FEAST_S3_BUCKET to run S3 registry integration test");
            return None;
        }
    };

    let prefix = std::env::var("FEAST_S3_PREFIX")
        .unwrap_or_else(|_| "feast_registry_tests".to_string());
    let prefix = prefix.trim_matches('/');

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    let key = if prefix.is_empty() {
        format!("registry_tests/registry_{nanos}.db")
    } else {
        format!("{prefix}/registry_{nanos}.db")
    };

    Some((bucket, key))
}

#[tokio::test]
async fn registry_loads_from_s3() -> Result<()> {
    let Some((bucket, key)) = s3_test_target() else {
        return Ok(());
    };

    let sdk_config = aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&sdk_config);
    let data = build_registry().encode_to_vec();

    if let Err(err) = client
        .put_object()
        .bucket(&bucket)
        .key(&key)
        .body(ByteStream::from(data.clone()))
        .send()
        .await
    {
        eprintln!("skipping S3 test (unable to write object): {err}");
        return Ok(());
    }

    let config = RegistryConfig {
        registry_store_type: Some("s3".to_string()),
        path: format!("s3://{bucket}/{key}"),
        client_id: "test".to_string(),
        cache_ttl_seconds: 0,
    };
    let registry = Registry::new(&config, Path::new("."), "test_project".to_string())?;
    registry
        .initialize()
        .context("failed to load registry from s3")?;

    let entities = registry.list_entities()?;
    assert_eq!(entities.len(), 1);
    assert_eq!(entities[0].name, "driver");
    assert_eq!(entities[0].join_key, "driver_id");

    let feature_views = registry.list_feature_views()?;
    assert_eq!(feature_views.len(), 1);
    assert_eq!(feature_views[0].base.name, "driver_stats");

    let feature_services = registry.list_feature_services()?;
    assert_eq!(feature_services.len(), 1);
    assert_eq!(feature_services[0].name, "driver_service");

    if let Err(err) = client
        .delete_object()
        .bucket(&bucket)
        .key(&key)
        .send()
        .await
    {
        eprintln!("failed to delete S3 test object: {err}");
    }

    Ok(())
}
