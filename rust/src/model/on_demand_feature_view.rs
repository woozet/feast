use super::{BaseFeatureView, FeatureView, FeatureViewProjection};
use crate::proto::feast::core;
use crate::proto::feast::types;
use anyhow::Result;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct OnDemandFeatureView {
    pub base: BaseFeatureView,
    pub source_feature_view_projections: HashMap<String, FeatureViewProjection>,
    pub source_request_data_sources: HashMap<String, Vec<core::FeatureSpecV2>>,
}

impl OnDemandFeatureView {
    pub fn from_proto(proto: &core::OnDemandFeatureView) -> Self {
        let spec = proto.spec.as_ref();
        let (name, features, sources) = if let Some(spec) = spec {
            (spec.name.clone(), spec.features.clone(), spec.sources.clone())
        } else {
            (String::new(), Vec::new(), HashMap::new())
        };

        let mut view = Self {
            base: BaseFeatureView::new(name, &features),
            source_feature_view_projections: HashMap::new(),
            source_request_data_sources: HashMap::new(),
        };

        for (source_name, on_demand_source) in sources {
            if let Some(source) = on_demand_source.source {
                match source {
                    core::on_demand_source::Source::FeatureView(feature_view) => {
                        let fv = FeatureView::from_proto(&feature_view);
                        view.source_feature_view_projections
                            .insert(source_name.clone(), fv.base.projection.clone());
                    }
                    core::on_demand_source::Source::FeatureViewProjection(projection) => {
                        view.source_feature_view_projections.insert(
                            source_name.clone(),
                            FeatureViewProjection::from_proto(&projection),
                        );
                    }
                    core::on_demand_source::Source::RequestDataSource(data_source) => {
                        if let Some(core::data_source::Options::RequestDataOptions(options)) =
                            data_source.options
                        {
                            view.source_request_data_sources
                                .insert(source_name.clone(), options.schema);
                        }
                    }
                }
            }
        }

        view
    }

    pub fn new_with_projection(&self, projection: FeatureViewProjection) -> Result<Self> {
        let projected_base = self.base.with_projection(projection)?;
        Ok(Self {
            base: projected_base,
            source_feature_view_projections: self.source_feature_view_projections.clone(),
            source_request_data_sources: self.source_request_data_sources.clone(),
        })
    }

    pub fn project_with_features(&self, feature_names: &[String]) -> Result<Self> {
        self.new_with_projection(self.base.project_with_features(feature_names))
    }

    pub fn get_request_data_schema(&self) -> HashMap<String, types::value_type::Enum> {
        let mut schema = HashMap::new();
        for features in self.source_request_data_sources.values() {
            for feature_spec in features {
                let value_type =
                    types::value_type::Enum::try_from(feature_spec.value_type).unwrap_or(
                        types::value_type::Enum::Invalid,
                    );
                schema.insert(feature_spec.name.clone(), value_type);
            }
        }
        schema
    }
}
