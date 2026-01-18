use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

const DEFAULT_CACHE_TTL_SECONDS: i64 = 600;
const DEFAULT_CLIENT_ID: &str = "Unknown";

#[derive(Debug, Deserialize)]
pub struct RepoConfig {
    #[serde(default)]
    pub project: String,
    #[serde(default)]
    pub provider: String,
    #[serde(default)]
    pub registry: Option<serde_yaml::Value>,
    #[serde(default)]
    pub online_store: HashMap<String, serde_yaml::Value>,
    #[serde(default)]
    pub offline_store: HashMap<String, serde_yaml::Value>,
    #[serde(default)]
    pub feature_server: HashMap<String, serde_yaml::Value>,
    #[serde(default)]
    pub flags: HashMap<String, serde_yaml::Value>,
    #[serde(default)]
    pub entity_key_serialization_version: i64,
    #[serde(skip)]
    pub repo_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct RegistryConfig {
    pub registry_store_type: Option<String>,
    pub path: String,
    pub client_id: String,
    pub cache_ttl_seconds: i64,
}

impl RepoConfig {
    pub fn registry_config(&self) -> Result<RegistryConfig> {
        let registry = self
            .registry
            .as_ref()
            .context("registry config missing from feature_store.yaml")?;

        match registry {
            serde_yaml::Value::String(path) => Ok(RegistryConfig {
                registry_store_type: None,
                path: path.clone(),
                client_id: DEFAULT_CLIENT_ID.to_string(),
                cache_ttl_seconds: DEFAULT_CACHE_TTL_SECONDS,
            }),
            serde_yaml::Value::Mapping(map) => {
                let mut config = RegistryConfig {
                    registry_store_type: None,
                    path: String::new(),
                    client_id: DEFAULT_CLIENT_ID.to_string(),
                    cache_ttl_seconds: DEFAULT_CACHE_TTL_SECONDS,
                };

                for (key, value) in map {
                    let key = match key {
                        serde_yaml::Value::String(key) => key.as_str(),
                        _ => continue,
                    };
                    match key {
                        "path" => {
                            config.path = yaml_string(value)?
                        }
                        "registry_store_type" => {
                            config.registry_store_type = Some(yaml_string(value)?)
                        }
                        "client_id" => {
                            config.client_id = yaml_string(value)?
                        }
                        "cache_ttl_seconds" => {
                            config.cache_ttl_seconds = yaml_i64(value)?
                        }
                        _ => {}
                    }
                }

                if config.path.is_empty() {
                    anyhow::bail!("registry.path is required");
                }

                Ok(config)
            }
            _ => anyhow::bail!("registry must be a string path or a map"),
        }
    }
}

pub fn load_repo_config(repo_path: &Path) -> Result<RepoConfig> {
    let config_path = repo_path.join("feature_store.yaml");
    let raw = std::fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    let expanded = shellexpand::env(&raw)
        .map_err(|err| anyhow::anyhow!("failed to expand env vars: {err}"))?;
    let mut config: RepoConfig = serde_yaml::from_str(&expanded)?;
    config.repo_path = repo_path
        .canonicalize()
        .unwrap_or_else(|_| repo_path.to_path_buf());
    Ok(config)
}

fn yaml_string(value: &serde_yaml::Value) -> Result<String> {
    match value {
        serde_yaml::Value::String(value) => Ok(value.clone()),
        _ => anyhow::bail!("expected string value, got {value:?}"),
    }
}

fn yaml_i64(value: &serde_yaml::Value) -> Result<i64> {
    match value {
        serde_yaml::Value::Number(number) => number
            .as_i64()
            .or_else(|| number.as_f64().map(|v| v as i64))
            .ok_or_else(|| anyhow::anyhow!("invalid numeric value: {number:?}")),
        _ => anyhow::bail!("expected numeric value, got {value:?}"),
    }
}
