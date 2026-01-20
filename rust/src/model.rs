pub const DUMMY_ENTITY_ID: &str = "__dummy_id";
pub const DUMMY_ENTITY_NAME: &str = "__dummy";
pub const DUMMY_ENTITY_VAL: &str = "";

mod entity;
mod feature_service;
mod feature_view;
mod on_demand_feature_view;

pub use entity::Entity;
pub use feature_service::{FeatureService, FeatureServiceLoggingConfig};
pub use feature_view::{BaseFeatureView, FeatureView, FeatureViewProjection, Field};
pub use on_demand_feature_view::OnDemandFeatureView;
