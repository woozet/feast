use super::DUMMY_ENTITY_NAME;
use crate::proto::feast::core;
use crate::proto::feast::types;
use anyhow::Result;
use prost_types::Duration;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub dtype: types::value_type::Enum,
}

impl Field {
    pub fn from_proto(proto: &core::FeatureSpecV2) -> Self {
        Self {
            name: proto.name.clone(),
            dtype: types::value_type::Enum::try_from(proto.value_type)
                .unwrap_or(types::value_type::Enum::Invalid),
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
        let features = feature_protos
            .iter()
            .map(Field::from_proto)
            .collect::<Vec<_>>();
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
            .collect::<HashSet<_>>();
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
