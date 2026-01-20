use anyhow::Result;
use feast_rust::model::{
    BaseFeatureView, FeatureService, FeatureView, FeatureViewProjection, Field, OnDemandFeatureView,
};
use feast_rust::onlineserving::{
    get_feature_views_to_use_by_feature_refs_with_odfv,
    get_feature_views_to_use_by_service_with_odfv, FeatureViewAndRefs,
};
use feast_rust::proto::feast::{core, types};
use std::collections::HashMap;

#[test]
fn unpack_feature_service_with_odfv() -> Result<()> {
    let fixtures = Fixtures::new();

    let feature_service = FeatureService {
        name: "service".to_string(),
        project: "project".to_string(),
        projections: vec![
            fixtures.projection("viewA", &[fixtures.feat_a.clone(), fixtures.feat_b.clone()]),
            fixtures.projection("viewB", &[fixtures.feat_c.clone()]),
            fixtures.projection("odfv", &[fixtures.feat_g.clone()]),
        ],
        logging_config: None,
    };

    let (fvs, odfvs) = get_feature_views_to_use_by_service_with_odfv(
        &feature_service,
        &fixtures.feature_views,
        &fixtures.on_demand_views,
    )?;

    assert_correct_unpacking(fvs, odfvs);
    Ok(())
}

#[test]
fn unpack_feature_refs_with_odfv() -> Result<()> {
    let fixtures = Fixtures::new();

    let features = vec![
        "viewA:featA".to_string(),
        "viewA:featB".to_string(),
        "viewB:featC".to_string(),
        "odfv:featG".to_string(),
    ];

    let (fvs, odfvs) = get_feature_views_to_use_by_feature_refs_with_odfv(
        &features,
        &fixtures.feature_views,
        &fixtures.on_demand_views,
    )?;

    assert_correct_unpacking(fvs, odfvs);
    Ok(())
}

fn assert_correct_unpacking(fvs: Vec<FeatureViewAndRefs>, odfvs: Vec<OnDemandFeatureView>) {
    assert_eq!(odfvs.len(), 1);
    assert_eq!(fvs.len(), 3);

    let mut fvs_by_name = HashMap::new();
    for fv in fvs {
        fvs_by_name.insert(fv.view.base.name.clone(), fv);
    }

    assert_eq!(
        sorted(fvs_by_name["viewA"].feature_refs.clone()),
        vec!["featA".to_string(), "featB".to_string()]
    );
    assert_eq!(
        sorted(fvs_by_name["viewB"].feature_refs.clone()),
        vec!["featC".to_string()]
    );
    assert_eq!(
        sorted(fvs_by_name["viewC"].feature_refs.clone()),
        vec!["featE".to_string()]
    );

    let odfv = &odfvs[0];
    assert_eq!(odfv.base.projection.features.len(), 1);
    assert_eq!(odfv.base.projection.features[0].name, "featG");
}

fn sorted(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values
}

struct Fixtures {
    feat_a: core::FeatureSpecV2,
    feat_b: core::FeatureSpecV2,
    feat_c: core::FeatureSpecV2,
    feat_g: core::FeatureSpecV2,
    feature_views: HashMap<String, FeatureView>,
    on_demand_views: HashMap<String, OnDemandFeatureView>,
}

impl Fixtures {
    fn new() -> Self {
        let feat_a = feature_spec("featA", types::value_type::Enum::Int32);
        let feat_b = feature_spec("featB", types::value_type::Enum::Int32);
        let feat_c = feature_spec("featC", types::value_type::Enum::Int32);
        let feat_d = feature_spec("featD", types::value_type::Enum::Int32);
        let feat_e = feature_spec("featE", types::value_type::Enum::Float);
        let feat_f = feature_spec("featF", types::value_type::Enum::Float);
        let feat_g = feature_spec("featG", types::value_type::Enum::Float);

        let view_a = feature_view("viewA", "entity", &[feat_a.clone(), feat_b.clone()]);
        let view_b = feature_view("viewB", "entity", &[feat_c.clone(), feat_d.clone()]);
        let view_c = feature_view("viewC", "entity", &[feat_e.clone()]);

        let odfv = on_demand_view(
            "odfv",
            &[
                ("viewB", vec![feat_c.clone()]),
                ("viewC", vec![feat_e.clone()]),
            ],
            vec![feat_f.clone(), feat_g.clone()],
        );

        let mut feature_views = HashMap::new();
        feature_views.insert("viewA".to_string(), view_a);
        feature_views.insert("viewB".to_string(), view_b);
        feature_views.insert("viewC".to_string(), view_c);

        let mut on_demand_views = HashMap::new();
        on_demand_views.insert("odfv".to_string(), odfv);

        Self {
            feat_a,
            feat_b,
            feat_c,
            feat_g,
            feature_views,
            on_demand_views,
        }
    }

    fn projection(&self, name: &str, features: &[core::FeatureSpecV2]) -> FeatureViewProjection {
        FeatureViewProjection {
            name: name.to_string(),
            name_alias: String::new(),
            features: features.iter().map(Field::from_proto).collect(),
            join_key_map: HashMap::new(),
        }
    }
}

fn feature_view(name: &str, entity: &str, features: &[core::FeatureSpecV2]) -> FeatureView {
    let base = BaseFeatureView::new(name.to_string(), features);
    FeatureView {
        base,
        ttl: None,
        entity_names: vec![entity.to_string()],
        entity_columns: Vec::new(),
    }
}

fn on_demand_view(
    name: &str,
    sources: &[(&str, Vec<core::FeatureSpecV2>)],
    output_features: Vec<core::FeatureSpecV2>,
) -> OnDemandFeatureView {
    let base = BaseFeatureView::new(name.to_string(), &output_features);
    let mut source_feature_view_projections = HashMap::new();
    for (view_name, features) in sources {
        let projection = FeatureViewProjection {
            name: (*view_name).to_string(),
            name_alias: String::new(),
            features: features.iter().map(Field::from_proto).collect(),
            join_key_map: HashMap::new(),
        };
        source_feature_view_projections.insert((*view_name).to_string(), projection);
    }

    OnDemandFeatureView {
        base,
        source_feature_view_projections,
        source_request_data_sources: HashMap::new(),
    }
}

fn feature_spec(name: &str, value_type: types::value_type::Enum) -> core::FeatureSpecV2 {
    core::FeatureSpecV2 {
        name: name.to_string(),
        value_type: value_type as i32,
        ..Default::default()
    }
}
