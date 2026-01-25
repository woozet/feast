use crate::config::RegistryConfig;
use crate::model;
use crate::proto::feast::core;
use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use prost::Message;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct RegistrySnapshot {
    pub(crate) entities: Vec<model::Entity>,
    pub(crate) feature_views: Vec<model::FeatureView>,
    pub(crate) stream_feature_views: Vec<model::FeatureView>,
    pub(crate) feature_services: Vec<model::FeatureService>,
    pub(crate) on_demand_feature_views: Vec<model::OnDemandFeatureView>,
    pub(crate) entities_by_name: HashMap<String, model::Entity>,
    pub(crate) feature_views_by_name: HashMap<String, model::FeatureView>,
    pub(crate) stream_feature_views_by_name: HashMap<String, model::FeatureView>,
    pub(crate) feature_services_by_name: HashMap<String, model::FeatureService>,
    pub(crate) on_demand_feature_views_by_name: HashMap<String, model::OnDemandFeatureView>,
    pub(crate) all_feature_views_by_name: HashMap<String, model::FeatureView>,
}

impl RegistrySnapshot {
    fn empty() -> Self {
        Self {
            entities: Vec::new(),
            feature_views: Vec::new(),
            stream_feature_views: Vec::new(),
            feature_services: Vec::new(),
            on_demand_feature_views: Vec::new(),
            entities_by_name: HashMap::new(),
            feature_views_by_name: HashMap::new(),
            stream_feature_views_by_name: HashMap::new(),
            feature_services_by_name: HashMap::new(),
            on_demand_feature_views_by_name: HashMap::new(),
            all_feature_views_by_name: HashMap::new(),
        }
    }

    fn from_proto(registry: core::Registry) -> Self {
        let entities = registry
            .entities
            .iter()
            .map(model::Entity::from_proto)
            .collect::<Vec<_>>();
        let feature_views = registry
            .feature_views
            .iter()
            .map(model::FeatureView::from_proto)
            .collect::<Vec<_>>();
        let stream_feature_views = registry
            .stream_feature_views
            .iter()
            .map(model::FeatureView::from_stream_proto)
            .collect::<Vec<_>>();
        let feature_services = registry
            .feature_services
            .iter()
            .map(model::FeatureService::from_proto)
            .collect::<Vec<_>>();
        let on_demand_feature_views = registry
            .on_demand_feature_views
            .iter()
            .map(model::OnDemandFeatureView::from_proto)
            .collect::<Vec<_>>();

        let mut entities_by_name = HashMap::new();
        for entity in &entities {
            entities_by_name.insert(entity.name.clone(), entity.clone());
        }

        let mut feature_views_by_name = HashMap::new();
        for view in &feature_views {
            feature_views_by_name.insert(view.base.name.clone(), view.clone());
        }

        let mut stream_feature_views_by_name = HashMap::new();
        for view in &stream_feature_views {
            stream_feature_views_by_name.insert(view.base.name.clone(), view.clone());
        }

        let mut feature_services_by_name = HashMap::new();
        for service in &feature_services {
            feature_services_by_name.insert(service.name.clone(), service.clone());
        }

        let mut on_demand_feature_views_by_name = HashMap::new();
        for view in &on_demand_feature_views {
            on_demand_feature_views_by_name.insert(view.base.name.clone(), view.clone());
        }

        let mut all_feature_views_by_name = feature_views_by_name.clone();
        for (name, view) in &stream_feature_views_by_name {
            all_feature_views_by_name.insert(name.clone(), view.clone());
        }

        Self {
            entities,
            feature_views,
            stream_feature_views,
            feature_services,
            on_demand_feature_views,
            entities_by_name,
            feature_views_by_name,
            stream_feature_views_by_name,
            feature_services_by_name,
            on_demand_feature_views_by_name,
            all_feature_views_by_name,
        }
    }
}

pub struct Registry {
    project: String,
    store: FileRegistryStore,
    snapshot: ArcSwap<RegistrySnapshot>,
    last_refresh_epoch_secs: AtomicU64,
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
            snapshot: ArcSwap::from_pointee(RegistrySnapshot::empty()),
            last_refresh_epoch_secs: AtomicU64::new(0),
        })
    }

    pub fn initialize(&self) -> Result<()> {
        self.refresh()
    }

    /// Force a reload of the registry from the underlying store.
    ///
    /// This mirrors the Python feature server behavior which refreshes registry out-of-band
    /// to avoid synchronous downloads/reads in the request path.
    pub fn refresh(&self) -> Result<()> {
        let registry = self.store.get_registry_proto()?;
        let snapshot = Arc::new(RegistrySnapshot::from_proto(registry));
        self.snapshot.store(snapshot);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_refresh_epoch_secs.store(now, Ordering::Release);
        Ok(())
    }

    pub fn project(&self) -> &str {
        &self.project
    }

    pub fn snapshot(&self) -> Arc<RegistrySnapshot> {
        self.snapshot.load_full()
    }

    pub fn last_refresh_epoch_secs(&self) -> u64 {
        self.last_refresh_epoch_secs.load(Ordering::Acquire)
    }

    pub fn list_entities(&self) -> Result<Vec<model::Entity>> {
        Ok(self.snapshot().entities.clone())
    }

    pub fn list_feature_views(&self) -> Result<Vec<model::FeatureView>> {
        Ok(self.snapshot().feature_views.clone())
    }

    pub fn list_stream_feature_views(&self) -> Result<Vec<model::FeatureView>> {
        Ok(self.snapshot().stream_feature_views.clone())
    }

    pub fn list_feature_services(&self) -> Result<Vec<model::FeatureService>> {
        Ok(self.snapshot().feature_services.clone())
    }

    pub fn list_on_demand_feature_views(&self) -> Result<Vec<model::OnDemandFeatureView>> {
        Ok(self.snapshot().on_demand_feature_views.clone())
    }

    pub fn feature_views_by_name(&self) -> HashMap<String, model::FeatureView> {
        self.snapshot().feature_views_by_name.clone()
    }

    pub fn stream_feature_views_by_name(&self) -> HashMap<String, model::FeatureView> {
        self.snapshot().stream_feature_views_by_name.clone()
    }

    pub fn all_feature_views_by_name(&self) -> HashMap<String, model::FeatureView> {
        self.snapshot().all_feature_views_by_name.clone()
    }

    pub fn on_demand_feature_views_by_name(
        &self,
    ) -> HashMap<String, model::OnDemandFeatureView> {
        self.snapshot().on_demand_feature_views_by_name.clone()
    }

    pub fn get_feature_service(&self, name: &str) -> Result<model::FeatureService> {
        self.snapshot()
            .feature_services_by_name
            .get(name)
            .cloned()
            .with_context(|| format!("feature service not found: {name}"))
    }

    pub fn get_feature_view(&self, name: &str) -> Result<model::FeatureView> {
        self.snapshot()
            .feature_views_by_name
            .get(name)
            .cloned()
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
