use anyhow::Result;
use feast_rust::config::RegistryConfig;
use feast_rust::proto::feast::core;
use feast_rust::proto::feast::types;
use feast_rust::registry::Registry;
use prost::Message;
use std::fs;
use std::path::PathBuf;
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

fn temp_root() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!("feast_registry_test_{nanos}"))
}

#[test]
fn registry_loads_fixture() -> Result<()> {
    let root = temp_root();
    fs::create_dir_all(&root)?;
    let file_name = "registry.db";
    let path = root.join(file_name);
    fs::write(&path, build_registry().encode_to_vec())?;

    let config = RegistryConfig {
        registry_store_type: None,
        path: file_name.to_string(),
        client_id: "test".to_string(),
        cache_ttl_seconds: 0,
    };
    let registry = Registry::new(&config, &root, "test_project".to_string())?;
    registry.initialize()?;

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

    let _ = fs::remove_file(&path);
    let _ = fs::remove_dir_all(&root);
    Ok(())
}
