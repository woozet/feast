use crate::model;
use crate::proto::feast::core;
use std::collections::HashMap;

#[derive(Debug)]
pub struct RegistrySnapshot {
    pub(crate) entities: Vec<model::Entity>,
    pub(crate) feature_views: Vec<model::FeatureView>,
    pub(crate) stream_feature_views: Vec<model::FeatureView>,
    pub(crate) feature_services: Vec<model::FeatureService>,
    pub(crate) on_demand_feature_views: Vec<model::OnDemandFeatureView>,
    pub(crate) feature_views_by_name: HashMap<String, model::FeatureView>,
    pub(crate) stream_feature_views_by_name: HashMap<String, model::FeatureView>,
    pub(crate) feature_services_by_name: HashMap<String, model::FeatureService>,
    pub(crate) on_demand_feature_views_by_name: HashMap<String, model::OnDemandFeatureView>,
    pub(crate) all_feature_views_by_name: HashMap<String, model::FeatureView>,
}

impl RegistrySnapshot {
    pub(super) fn empty() -> Self {
        Self {
            entities: Vec::new(),
            feature_views: Vec::new(),
            stream_feature_views: Vec::new(),
            feature_services: Vec::new(),
            on_demand_feature_views: Vec::new(),
            feature_views_by_name: HashMap::new(),
            stream_feature_views_by_name: HashMap::new(),
            feature_services_by_name: HashMap::new(),
            on_demand_feature_views_by_name: HashMap::new(),
            all_feature_views_by_name: HashMap::new(),
        }
    }

    pub(super) fn from_proto(registry: core::Registry) -> Self {
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
            feature_views_by_name,
            stream_feature_views_by_name,
            feature_services_by_name,
            on_demand_feature_views_by_name,
            all_feature_views_by_name,
        }
    }
}
