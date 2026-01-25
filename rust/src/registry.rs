mod snapshot;
mod store;

use crate::config::RegistryConfig;
use crate::model;
use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub use snapshot::RegistrySnapshot;
use store::RegistryStore;

pub struct Registry {
    project: String,
    store: RegistryStore,
    snapshot: ArcSwap<RegistrySnapshot>,
    last_refresh_epoch_secs: AtomicU64,
}

impl Registry {
    pub fn new(config: &RegistryConfig, repo_path: &Path, project: String) -> Result<Self> {
        let store = RegistryStore::new(config, repo_path)?;

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
