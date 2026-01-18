use crate::proto::feast::core;
use crate::proto::feast::types;
use anyhow::Result;
use prost_types::Duration;
use std::collections::HashMap;

pub const DUMMY_ENTITY_ID: &str = "__dummy_id";
pub const DUMMY_ENTITY_NAME: &str = "__dummy";
pub const DUMMY_ENTITY_VAL: &str = "";

#[derive(Debug, Clone)]
pub struct Entity {
    pub name: String,
    pub join_key: String,
}

impl Entity {
    pub fn from_proto(proto: &core::Entity) -> Self {
        let spec = proto.spec.as_ref();
        Self {
            name: spec.map(|s| s.name.clone()).unwrap_or_default(),
            join_key: spec.map(|s| s.join_key.clone()).unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub dtype: types::value_type::Enum,
}

impl Field {
    pub fn from_proto(proto: &core::FeatureSpecV2) -> Self {
        Self {
            name: proto.name.clone(),
            dtype: types::value_type::Enum::try_from(proto.value_type).unwrap_or(types::value_type::Enum::Invalid),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FeatureViewProjection {
    pub name: String,
    pub name_alias: String,
    pub features: Vec<Field>,
    pub join_key_map: HashMap<String, String>,
}

impl FeatureViewProjection {
    pub fn name_to_use(&self) -> &str {
        if self.name_alias.is_empty() {
            &self.name
        } else {
            &self.name_alias
        }
    }

    pub fn from_proto(proto: &core::FeatureViewProjection) -> Self {
        let features = proto
            .feature_columns
            .iter()
            .map(Field::from_proto)
            .collect();
        Self {
            name: proto.feature_view_name.clone(),
            name_alias: proto.feature_view_name_alias.clone(),
            features,
            join_key_map: proto.join_key_map.clone(),
        }
    }

    pub fn from_definition(base: &BaseFeatureView) -> Self {
        Self {
            name: base.name.clone(),
            name_alias: String::new(),
            features: base.features.clone(),
            join_key_map: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BaseFeatureView {
    pub name: String,
    pub features: Vec<Field>,
    pub projection: FeatureViewProjection,
}

impl BaseFeatureView {
    pub fn new(name: String, feature_protos: &[core::FeatureSpecV2]) -> Self {
        let features = feature_protos.iter().map(Field::from_proto).collect::<Vec<_>>();
        let mut base = Self {
            name,
            features,
            projection: FeatureViewProjection {
                name: String::new(),
                name_alias: String::new(),
                features: Vec::new(),
                join_key_map: HashMap::new(),
            },
        };
        base.projection = FeatureViewProjection::from_definition(&base);
        base
    }

    pub fn with_projection(&self, projection: FeatureViewProjection) -> Result<Self> {
        if projection.name != self.name {
            anyhow::bail!(
                "projection name {} does not match feature view {}",
                projection.name,
                self.name
            );
        }
        let features = self
            .features
            .iter()
            .map(|f| f.name.as_str())
            .collect::<std::collections::HashSet<_>>();
        for feature in &projection.features {
            if !features.contains(feature.name.as_str()) {
                anyhow::bail!(
                    "projection contains feature {} which is not in view {}",
                    feature.name,
                    self.name
                );
            }
        }
        Ok(Self {
            name: self.name.clone(),
            features: self.features.clone(),
            projection,
        })
    }

    pub fn project_with_features(&self, feature_names: &[String]) -> FeatureViewProjection {
        let mut features = Vec::new();
        for feature in &self.features {
            if feature_names.iter().any(|name| name == &feature.name) {
                features.push(feature.clone());
            }
        }
        FeatureViewProjection {
            name: self.name.clone(),
            name_alias: String::new(),
            features,
            join_key_map: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FeatureView {
    pub base: BaseFeatureView,
    pub ttl: Option<Duration>,
    pub entity_names: Vec<String>,
    pub entity_columns: Vec<Field>,
}

impl FeatureView {
    pub fn from_proto(proto: &core::FeatureView) -> Self {
        let spec = proto.spec.as_ref();
        let (name, features, ttl, entities, entity_columns) = if let Some(spec) = spec {
            (
                spec.name.clone(),
                spec.features.clone(),
                spec.ttl.clone(),
                spec.entities.clone(),
                spec.entity_columns.clone(),
            )
        } else {
            (String::new(), Vec::new(), None, Vec::new(), Vec::new())
        };

        let base = BaseFeatureView::new(name, &features);
        let entity_names = if entities.is_empty() {
            vec![DUMMY_ENTITY_NAME.to_string()]
        } else {
            entities
        };
        let entity_columns = entity_columns
            .iter()
            .map(Field::from_proto)
            .collect::<Vec<_>>();
        Self {
            base,
            ttl,
            entity_names,
            entity_columns,
        }
    }

    pub fn from_stream_proto(proto: &core::StreamFeatureView) -> Self {
        let spec = proto.spec.as_ref();
        let (name, features, ttl, entities, entity_columns) = if let Some(spec) = spec {
            (
                spec.name.clone(),
                spec.features.clone(),
                spec.ttl.clone(),
                spec.entities.clone(),
                spec.entity_columns.clone(),
            )
        } else {
            (String::new(), Vec::new(), None, Vec::new(), Vec::new())
        };

        let base = BaseFeatureView::new(name, &features);
        let entity_names = if entities.is_empty() {
            vec![DUMMY_ENTITY_NAME.to_string()]
        } else {
            entities
        };
        let entity_columns = entity_columns
            .iter()
            .map(Field::from_proto)
            .collect::<Vec<_>>();
        Self {
            base,
            ttl,
            entity_names,
            entity_columns,
        }
    }

    pub fn new_from_base(&self, base: BaseFeatureView) -> Self {
        Self {
            base,
            ttl: self.ttl.clone(),
            entity_names: self.entity_names.clone(),
            entity_columns: self.entity_columns.clone(),
        }
    }

    pub fn has_entity(&self, name: &str) -> bool {
        self.entity_names.iter().any(|entity| entity == name)
    }
}

#[derive(Debug, Clone)]
pub struct FeatureServiceLoggingConfig {
    pub sample_rate: f32,
}

#[derive(Debug, Clone)]
pub struct FeatureService {
    pub name: String,
    pub project: String,
    pub projections: Vec<FeatureViewProjection>,
    pub logging_config: Option<FeatureServiceLoggingConfig>,
}

impl FeatureService {
    pub fn from_proto(proto: &core::FeatureService) -> Self {
        let spec = proto.spec.as_ref();
        let (name, project, features, logging_config) = if let Some(spec) = spec {
            let projections = spec
                .features
                .iter()
                .map(FeatureViewProjection::from_proto)
                .collect();
            let logging_config = spec.logging_config.as_ref().map(|config| {
                FeatureServiceLoggingConfig {
                    sample_rate: config.sample_rate,
                }
            });
            (
                spec.name.clone(),
                spec.project.clone(),
                projections,
                logging_config,
            )
        } else {
            (String::new(), String::new(), Vec::new(), None)
        };

        Self {
            name,
            project,
            projections: features,
            logging_config,
        }
    }
}
