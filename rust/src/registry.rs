use crate::config::RegistryConfig;
use crate::model;
use crate::proto::feast::core;
use anyhow::{Context, Result};
use prost::Message;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub struct Registry {
    project: String,
    store: FileRegistryStore,
    cache_ttl: Duration,
    cached: Option<core::Registry>,
    cached_at: Option<Instant>,
}

impl Registry {
    pub fn new(config: &RegistryConfig, repo_path: &Path, project: String) -> Result<Self> {
        let store = match config.registry_store_type.as_deref() {
            None | Some("") | Some("file") => FileRegistryStore::new(config, repo_path),
            Some(other) => anyhow::bail!("registry_store_type {other} is not supported"),
        };

        Ok(Self {
            project,
            store,
            cache_ttl: Duration::from_secs(config.cache_ttl_seconds.max(0) as u64),
            cached: None,
            cached_at: None,
        })
    }

    pub fn initialize(&mut self) -> Result<()> {
        self.get_registry_proto().map(|_| ())
    }

    pub fn project(&self) -> &str {
        &self.project
    }

    pub fn get_registry_proto(&mut self) -> Result<&core::Registry> {
        let expired = self.cached.is_none()
            || self
                .cached_at
                .map(|ts| {
                    if self.cache_ttl.is_zero() {
                        true
                    } else {
                        ts.elapsed() > self.cache_ttl
                    }
                })
                .unwrap_or(true);

        if expired {
            let registry = self.store.get_registry_proto()?;
            self.cached = Some(registry);
            self.cached_at = Some(Instant::now());
        }

        self.cached
            .as_ref()
            .context("registry cache unexpectedly empty")
    }

    pub fn list_entities(&mut self) -> Result<Vec<model::Entity>> {
        let registry = self.get_registry_proto()?;
        Ok(registry
            .entities
            .iter()
            .map(model::Entity::from_proto)
            .collect())
    }

    pub fn list_feature_views(&mut self) -> Result<Vec<model::FeatureView>> {
        let registry = self.get_registry_proto()?;
        Ok(registry
            .feature_views
            .iter()
            .map(model::FeatureView::from_proto)
            .collect())
    }

    pub fn list_stream_feature_views(&mut self) -> Result<Vec<model::FeatureView>> {
        let registry = self.get_registry_proto()?;
        Ok(registry
            .stream_feature_views
            .iter()
            .map(model::FeatureView::from_stream_proto)
            .collect())
    }

    pub fn list_feature_services(&mut self) -> Result<Vec<model::FeatureService>> {
        let registry = self.get_registry_proto()?;
        Ok(registry
            .feature_services
            .iter()
            .map(model::FeatureService::from_proto)
            .collect())
    }

    pub fn list_on_demand_feature_views(&mut self) -> Result<Vec<model::OnDemandFeatureView>> {
        let registry = self.get_registry_proto()?;
        Ok(registry
            .on_demand_feature_views
            .iter()
            .map(model::OnDemandFeatureView::from_proto)
            .collect())
    }

    pub fn get_feature_service(&mut self, name: &str) -> Result<model::FeatureService> {
        let registry = self.get_registry_proto()?;
        registry
            .feature_services
            .iter()
            .find(|service| {
                service
                    .spec
                    .as_ref()
                    .map(|spec| spec.name == name)
                    .unwrap_or(false)
            })
            .map(model::FeatureService::from_proto)
            .with_context(|| format!("feature service not found: {name}"))
    }

    pub fn get_feature_view(&mut self, name: &str) -> Result<model::FeatureView> {
        let registry = self.get_registry_proto()?;
        registry
            .feature_views
            .iter()
            .find(|view| {
                view.spec
                    .as_ref()
                    .map(|spec| spec.name == name)
                    .unwrap_or(false)
            })
            .map(model::FeatureView::from_proto)
            .with_context(|| format!("feature view not found: {name}"))
    }
}

struct FileRegistryStore {
    file_path: PathBuf,
}

impl FileRegistryStore {
    fn new(config: &RegistryConfig, repo_path: &Path) -> Self {
        let registry_path = Path::new(&config.path);
        let file_path = if registry_path.is_absolute() {
            registry_path.to_path_buf()
        } else {
            repo_path.join(registry_path)
        };

        Self { file_path }
    }

    fn get_registry_proto(&self) -> Result<core::Registry> {
        let data = std::fs::read(&self.file_path)
            .with_context(|| format!("failed to read registry at {}", self.file_path.display()))?;
        let registry = core::Registry::decode(data.as_slice())?;
        Ok(registry)
    }
}
