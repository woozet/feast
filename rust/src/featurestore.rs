use crate::config::RepoConfig;
use crate::model;
use crate::onlineserving;
use crate::onlinestore::RedisOnlineStore;
use crate::registry::Registry;
use crate::proto::feast::serving;
use crate::proto::feast::types;
use anyhow::Result;
use std::collections::HashMap;

pub struct FeatureStore {
    config: RepoConfig,
    registry: Registry,
    online_store: RedisOnlineStore,
}

pub struct Features {
    pub feature_refs: Vec<String>,
    pub feature_service: Option<model::FeatureService>,
}

impl FeatureStore {
    pub fn new(config: RepoConfig) -> Result<Self> {
        let registry_config = config.registry_config()?;
        let mut registry = Registry::new(&registry_config, &config.repo_path, config.project.clone())?;
        registry.initialize()?;
        let online_store = RedisOnlineStore::new(config.project.clone(), &config)?;
        Ok(Self {
            config,
            registry,
            online_store,
        })
    }

    pub fn parse_features(
        &mut self,
        kind: &serving::get_online_features_request::Kind,
    ) -> Result<Features> {
        match kind {
            serving::get_online_features_request::Kind::Features(feature_list) => Ok(Features {
                feature_refs: feature_list.val.clone(),
                feature_service: None,
            }),
            serving::get_online_features_request::Kind::FeatureService(name) => {
                let feature_service = self.registry.get_feature_service(name)?;
                Ok(Features {
                    feature_refs: Vec::new(),
                    feature_service: Some(feature_service),
                })
            }
        }
    }

    pub fn list_feature_views(&mut self) -> Result<Vec<model::FeatureView>> {
        self.registry.list_feature_views()
    }

    pub fn list_stream_feature_views(&mut self) -> Result<Vec<model::FeatureView>> {
        self.registry.list_stream_feature_views()
    }

    pub fn list_entities(&mut self) -> Result<Vec<model::Entity>> {
        self.registry.list_entities()
    }

    pub fn get_feature_service(&mut self, name: &str) -> Result<model::FeatureService> {
        self.registry.get_feature_service(name)
    }

    pub fn get_feature_view(&mut self, name: &str) -> Result<model::FeatureView> {
        self.registry.get_feature_view(name)
    }

    pub fn project(&self) -> &str {
        &self.config.project
    }

    pub fn online_store(&self) -> &RedisOnlineStore {
        &self.online_store
    }

    pub async fn get_online_features(
        &mut self,
        feature_refs: Vec<String>,
        feature_service: Option<model::FeatureService>,
        mut join_key_to_entity_values: HashMap<String, Vec<types::Value>>,
        mut request_data: HashMap<String, Vec<types::Value>>,
        full_feature_names: bool,
    ) -> Result<Vec<onlineserving::FeatureVector>> {
        let mut feature_views = HashMap::new();
        for fv in self.list_feature_views()? {
            feature_views.insert(fv.base.name.clone(), fv);
        }
        for fv in self.list_stream_feature_views()? {
            feature_views.insert(fv.base.name.clone(), fv);
        }

        let requested_feature_views = if let Some(service) = feature_service.as_ref() {
            onlineserving::get_feature_views_to_use_by_service(service, &feature_views)?
        } else {
            onlineserving::get_feature_views_to_use_by_feature_refs(&feature_refs, &feature_views)?
        };

        let mut entities = self.list_entities()?;
        let entityless_case = requested_feature_views.iter().any(|view_and_refs| {
            view_and_refs
                .view
                .entity_names
                .iter()
                .any(|name| name == model::DUMMY_ENTITY_NAME)
        });
        if entityless_case
            && !entities
                .iter()
                .any(|entity| entity.name == model::DUMMY_ENTITY_NAME)
        {
            entities.push(model::Entity {
                name: model::DUMMY_ENTITY_NAME.to_string(),
                join_key: model::DUMMY_ENTITY_ID.to_string(),
            });
        }
        let (entity_name_to_join_key_map, expected_join_keys_set) =
            onlineserving::get_entity_maps(&requested_feature_views, &entities)?;

        onlineserving::validate_feature_refs(&requested_feature_views, full_feature_names)?;
        let num_rows = onlineserving::validate_entity_values(
            &mut join_key_to_entity_values,
            &mut request_data,
            &expected_join_keys_set,
        )?;

        if entityless_case {
            let dummy_value = types::Value {
                val: Some(types::value::Val::StringVal(model::DUMMY_ENTITY_VAL.to_string())),
            };
            join_key_to_entity_values.insert(
                model::DUMMY_ENTITY_ID.to_string(),
                vec![dummy_value; num_rows],
            );
        }

        let grouped_refs = onlineserving::group_feature_refs(
            &requested_feature_views,
            &join_key_to_entity_values,
            &entity_name_to_join_key_map,
            full_feature_names,
        )?;

        let mut vectors = Vec::new();
        for group_ref in grouped_refs.values() {
            let feature_data = self
                .online_store
                .online_read(&group_ref.entity_keys, &group_ref.feature_view_names, &group_ref.feature_names)
                .await?;
            let mut group_vectors = onlineserving::transpose_feature_rows_into_columns(
                &feature_data,
                group_ref,
                &requested_feature_views,
                num_rows,
            )?;
            vectors.append(&mut group_vectors);
        }

        let vectors = onlineserving::keep_only_requested_features(
            vectors,
            &feature_refs,
            feature_service.as_ref(),
            full_feature_names,
        )?;

        let mut entity_vectors =
            onlineserving::entities_to_feature_vectors(&join_key_to_entity_values, num_rows)?;
        entity_vectors.extend(vectors);

        Ok(entity_vectors)
    }
}

pub fn build_join_key_map(entities: &[model::Entity]) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for entity in entities {
        map.insert(entity.name.clone(), entity.join_key.clone());
    }
    map
}
