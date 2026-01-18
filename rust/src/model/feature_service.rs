use super::FeatureViewProjection;
use crate::proto::feast::core;

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
