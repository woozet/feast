use crate::model;
use crate::onlinestore::FeatureData;
use crate::proto::feast::serving;
use crate::proto::feast::types;
use anyhow::{Context, Result};
use prost::Message;
use prost_types::{Duration, Timestamp};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct FeatureVector {
    pub name: String,
    pub values: Vec<types::Value>,
    pub statuses: Vec<serving::FieldStatus>,
    pub timestamps: Vec<Timestamp>,
}

#[derive(Debug, Clone)]
pub struct FeatureViewAndRefs {
    pub view: model::FeatureView,
    pub feature_refs: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct GroupedFeaturesPerEntitySet {
    pub feature_names: Vec<String>,
    pub feature_view_names: Vec<String>,
    pub aliased_feature_names: Vec<String>,
    pub entity_keys: Vec<types::EntityKey>,
    pub indices: Vec<Vec<usize>>,
}

pub fn parse_feature_reference(feature_ref: &str) -> Result<(String, String)> {
    let parts: Vec<&str> = feature_ref.split(':').collect();
    if parts.is_empty() {
        anyhow::bail!("feature reference must be in the format 'FeatureViewName:FeatureName'");
    }
    if parts.len() == 1 {
        Ok((String::new(), parts[0].to_string()))
    } else {
        Ok((parts[0].to_string(), parts[1].to_string()))
    }
}

fn add_string_if_not_contains(mut values: Vec<String>, element: &str) -> Vec<String> {
    if !values.iter().any(|value| value == element) {
        values.push(element.to_string());
    }
    values
}

pub fn get_feature_views_to_use_by_service(
    feature_service: &model::FeatureService,
    feature_views: &HashMap<String, model::FeatureView>,
) -> Result<(Vec<FeatureViewAndRefs>, Vec<model::OnDemandFeatureView>)> {
    get_feature_views_to_use_by_service_with_odfv(feature_service, feature_views, &HashMap::new())
}

pub fn get_feature_views_to_use_by_service_with_odfv(
    feature_service: &model::FeatureService,
    feature_views: &HashMap<String, model::FeatureView>,
    on_demand_feature_views: &HashMap<String, model::OnDemandFeatureView>,
) -> Result<(Vec<FeatureViewAndRefs>, Vec<model::OnDemandFeatureView>)> {
    let mut view_name_to_view = HashMap::new();
    let mut odfvs_to_use = Vec::new();

    for projection in &feature_service.projections {
        let feature_view_name = projection.name.clone();
        if let Some(fv) = feature_views.get(&feature_view_name) {
            let base = fv.base.with_projection(projection.clone())?;
            let projected_view = fv.new_from_base(base);
            let key = projection.name_to_use().to_string();
            let entry = view_name_to_view.entry(key).or_insert_with(|| FeatureViewAndRefs {
                view: projected_view.clone(),
                feature_refs: Vec::new(),
            });

            for feature in &projection.features {
                entry.feature_refs =
                    add_string_if_not_contains(entry.feature_refs.clone(), &feature.name);
            }
        } else if let Some(odfv) = on_demand_feature_views.get(&feature_view_name) {
            let projected_odfv = odfv.new_with_projection(projection.clone())?;
            extract_odfv_dependencies(&projected_odfv, feature_views, &mut view_name_to_view)?;
            odfvs_to_use.push(projected_odfv);
        } else {
            anyhow::bail!("feature view {feature_view_name} not found");
        }
    }

    Ok((view_name_to_view.into_values().collect(), odfvs_to_use))
}

pub fn get_feature_views_to_use_by_feature_refs(
    features: &[String],
    feature_views: &HashMap<String, model::FeatureView>,
) -> Result<(Vec<FeatureViewAndRefs>, Vec<model::OnDemandFeatureView>)> {
    get_feature_views_to_use_by_feature_refs_with_odfv(features, feature_views, &HashMap::new())
}

pub fn get_feature_views_to_use_by_feature_refs_with_odfv(
    features: &[String],
    feature_views: &HashMap<String, model::FeatureView>,
    on_demand_feature_views: &HashMap<String, model::OnDemandFeatureView>,
) -> Result<(Vec<FeatureViewAndRefs>, Vec<model::OnDemandFeatureView>)> {
    let mut view_name_to_view = HashMap::new();
    let mut odfv_to_features: HashMap<String, Vec<String>> = HashMap::new();

    for feature_ref in features {
        let (feature_view_name, feature_name) = parse_feature_reference(feature_ref)?;
        if let Some(fv) = feature_views.get(&feature_view_name) {
            let entry = view_name_to_view
                .entry(fv.base.name.clone())
                .or_insert_with(|| FeatureViewAndRefs {
                    view: fv.clone(),
                    feature_refs: Vec::new(),
                });
            entry.feature_refs =
                add_string_if_not_contains(entry.feature_refs.clone(), &feature_name);
        } else if on_demand_feature_views.contains_key(&feature_view_name) {
            odfv_to_features
                .entry(feature_view_name.clone())
                .or_default()
                .push(feature_name);
        } else {
            anyhow::bail!("feature view {feature_view_name} not found");
        }
    }

    let mut odfvs_to_use = Vec::new();
    for (odfv_name, feature_names) in odfv_to_features {
        let odfv = on_demand_feature_views
            .get(&odfv_name)
            .with_context(|| format!("feature view {odfv_name} not found"))?;
        let projected = odfv.project_with_features(&feature_names)?;
        extract_odfv_dependencies(&projected, feature_views, &mut view_name_to_view)?;
        odfvs_to_use.push(projected);
    }

    Ok((view_name_to_view.into_values().collect(), odfvs_to_use))
}

fn extract_odfv_dependencies(
    odfv: &model::OnDemandFeatureView,
    source_fvs: &HashMap<String, model::FeatureView>,
    requested_features: &mut HashMap<String, FeatureViewAndRefs>,
) -> Result<()> {
    for projection in odfv.source_feature_view_projections.values() {
        let fv = source_fvs
            .get(&projection.name)
            .with_context(|| format!("feature view {} not found", projection.name))?;
        let base = fv.base.with_projection(projection.clone())?;
        let new_fv = fv.new_from_base(base);

        let entry = requested_features
            .entry(projection.name_to_use().to_string())
            .or_insert_with(|| FeatureViewAndRefs {
                view: new_fv.clone(),
                feature_refs: Vec::new(),
            });

        for feature in &projection.features {
            entry.feature_refs =
                add_string_if_not_contains(entry.feature_refs.clone(), &feature.name);
        }
    }

    Ok(())
}

pub fn get_entity_maps(
    requested_feature_views: &[FeatureViewAndRefs],
    entities: &[model::Entity],
) -> Result<(HashMap<String, String>, HashSet<String>)> {
    let mut entity_name_to_join_key = HashMap::new();
    let mut expected_join_keys = HashSet::new();

    let entities_by_name = entities
        .iter()
        .map(|entity| (entity.name.clone(), entity.clone()))
        .collect::<HashMap<_, _>>();

    for features_and_view in requested_feature_views {
        let feature_view = &features_and_view.view;
        let join_key_map = feature_view.base.projection.join_key_map.clone();

        for entity_name in &feature_view.entity_names {
            let entity = entities_by_name
                .get(entity_name)
                .with_context(|| format!("entity not found: {entity_name}"))?;
            let join_key = entity.join_key.clone();
            entity_name_to_join_key.insert(entity_name.clone(), join_key.clone());

            if let Some(alias) = join_key_map.get(&join_key) {
                expected_join_keys.insert(alias.clone());
            } else {
                expected_join_keys.insert(join_key);
            }
        }
    }

    Ok((entity_name_to_join_key, expected_join_keys))
}

pub fn validate_entity_values(
    join_key_values: &mut HashMap<String, Vec<types::Value>>,
    request_data: &mut HashMap<String, Vec<types::Value>>,
    expected_join_keys_set: &HashSet<String>,
) -> Result<usize> {
    let mut num_rows: i64 = -1;

    let mut unexpected_keys = Vec::new();
    for join_key in join_key_values.keys() {
        if !expected_join_keys_set.contains(join_key) {
            unexpected_keys.push(join_key.clone());
        }
    }

    for join_key in unexpected_keys {
        if let Some(values) = join_key_values.remove(&join_key) {
            request_data.insert(join_key, values);
        }
    }

    for (join_key, values) in join_key_values.iter() {
        if !expected_join_keys_set.contains(join_key) {
            continue;
        }
        if num_rows < 0 {
            num_rows = values.len() as i64;
        } else if values.len() as i64 != num_rows {
            anyhow::bail!("all entity rows must have the same length");
        }
    }

    if num_rows < 0 {
        anyhow::bail!("entity rows are empty");
    }

    Ok(num_rows as usize)
}

pub fn validate_feature_refs(
    requested_features: &[FeatureViewAndRefs],
    full_feature_names: bool,
) -> Result<()> {
    let mut feature_ref_counter: HashMap<String, usize> = HashMap::new();
    let mut feature_refs: Vec<String> = Vec::new();

    for view_and_features in requested_features {
        for feature in &view_and_features.feature_refs {
            let projected_view_name = view_and_features.view.base.projection.name_to_use();
            feature_refs.push(format!("{}:{}", projected_view_name, feature));
        }
    }

    for feature_ref in &feature_refs {
        if full_feature_names {
            *feature_ref_counter.entry(feature_ref.clone()).or_default() += 1;
        } else {
            let (_view_name, feature_name) = parse_feature_reference(feature_ref)?;
            *feature_ref_counter.entry(feature_name).or_default() += 1;
        }
    }

    feature_ref_counter.retain(|_, count| *count > 1);
    if feature_ref_counter.is_empty() {
        return Ok(());
    }

    let mut collided = Vec::new();
    if full_feature_names {
        collided.extend(feature_ref_counter.keys().cloned());
    } else {
        for feature_ref in &feature_refs {
            let (_view_name, feature_name) = parse_feature_reference(feature_ref)?;
            if feature_ref_counter.contains_key(&feature_name) {
                collided.push(feature_ref.clone());
            }
        }
    }

    anyhow::bail!("feature name collision: {:?}", collided)
}

pub fn group_feature_refs(
    requested_feature_views: &[FeatureViewAndRefs],
    join_key_values: &HashMap<String, Vec<types::Value>>,
    entity_name_to_join_key_map: &HashMap<String, String>,
    full_feature_names: bool,
) -> Result<HashMap<String, GroupedFeaturesPerEntitySet>> {
    let mut groups: HashMap<String, GroupedFeaturesPerEntitySet> = HashMap::new();

    for features_and_view in requested_feature_views {
        let feature_view = &features_and_view.view;
        let feature_names = &features_and_view.feature_refs;

        let mut join_keys = Vec::new();
        for entity_name in &feature_view.entity_names {
            if let Some(join_key) = entity_name_to_join_key_map.get(entity_name) {
                join_keys.push(join_key.clone());
            }
        }

        let join_key_to_alias = feature_view.base.projection.join_key_map.clone();
        let mut group_key_builder = Vec::new();
        let mut join_keys_values_projection: HashMap<String, Vec<types::Value>> = HashMap::new();

        for join_key in &join_keys {
            let (join_key_display, join_key_or_alias) = if let Some(alias) = join_key_to_alias.get(join_key) {
                (format!("{join_key}[{alias}]"), alias.as_str())
            } else {
                (join_key.clone(), join_key.as_str())
            };

            group_key_builder.push(join_key_display);
            let values = join_key_values
                .get(join_key_or_alias)
                .with_context(|| format!("key {join_key} is missing in provided entity rows"))?;
            join_keys_values_projection.insert(join_key.clone(), values.clone());
        }

        group_key_builder.sort();
        let group_key = group_key_builder.join(",");

        let view_name_to_use = feature_view.base.projection.name_to_use().to_string();
        let mut aliased_feature_names = Vec::new();
        let mut feature_view_names = Vec::new();

        for feature_name in feature_names {
            aliased_feature_names.push(get_qualified_feature_name(
                &view_name_to_use,
                feature_name,
                full_feature_names,
            ));
            feature_view_names.push(feature_view.base.name.clone());
        }

        if let Some(group) = groups.get_mut(&group_key) {
            group.feature_names.extend(feature_names.clone());
            group.aliased_feature_names.extend(aliased_feature_names);
            group.feature_view_names.extend(feature_view_names);
        } else {
            let join_keys_proto = entity_keys_to_protos(&join_keys_values_projection);
            let (unique_rows, mapping_indices) = get_unique_entity_rows(&join_keys_proto)?;
            groups.insert(
                group_key,
                GroupedFeaturesPerEntitySet {
                    feature_names: feature_names.clone(),
                    feature_view_names,
                    aliased_feature_names,
                    indices: mapping_indices,
                    entity_keys: unique_rows,
                },
            );
        }
    }

    Ok(groups)
}

fn entity_keys_to_protos(
    join_key_values: &HashMap<String, Vec<types::Value>>,
) -> Vec<types::EntityKey> {
    let mut keys = join_key_values.keys().cloned().collect::<Vec<_>>();
    keys.sort();

    let num_rows = join_key_values
        .values()
        .next()
        .map(|values| values.len())
        .unwrap_or(0);

    let mut entity_keys = vec![
        types::EntityKey {
            join_keys: keys.clone(),
            entity_values: vec![types::Value { val: None }; keys.len()],
        };
        num_rows
    ];

    for (col_index, key) in keys.iter().enumerate() {
        if let Some(values) = join_key_values.get(key) {
            for (row_index, value) in values.iter().enumerate() {
                if let Some(entity_key) = entity_keys.get_mut(row_index) {
                    if let Some(slot) = entity_key.entity_values.get_mut(col_index) {
                        *slot = value.clone();
                    }
                }
            }
        }
    }

    entity_keys
}

fn get_unique_entity_rows(
    join_keys_proto: &[types::EntityKey],
) -> Result<(Vec<types::EntityKey>, Vec<Vec<usize>>)> {
    let mut unique_values: HashMap<Vec<u8>, types::EntityKey> = HashMap::new();
    let mut positions: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();

    for (index, entity_key) in join_keys_proto.iter().enumerate() {
        let serialized = entity_key.encode_to_vec();
        let hash = Sha256::digest(&serialized).to_vec();
        if !unique_values.contains_key(&hash) {
            unique_values.insert(hash.clone(), entity_key.clone());
            positions.insert(hash.clone(), vec![index]);
        } else if let Some(pos) = positions.get_mut(&hash) {
            pos.push(index);
        }
    }

    let mut mapping_indices = Vec::with_capacity(unique_values.len());
    let mut unique_rows = Vec::with_capacity(unique_values.len());
    for (hash, row) in unique_values {
        if let Some(pos) = positions.get(&hash) {
            mapping_indices.push(pos.clone());
            unique_rows.push(row);
        }
    }

    Ok((unique_rows, mapping_indices))
}

pub fn transpose_feature_rows_into_columns(
    feature_data_rows: &[Option<Vec<FeatureData>>],
    group_ref: &GroupedFeaturesPerEntitySet,
    requested_feature_views: &[FeatureViewAndRefs],
    num_rows: usize,
) -> Result<Vec<FeatureVector>> {
    let mut fvs = HashMap::new();
    for view_and_refs in requested_feature_views {
        fvs.insert(view_and_refs.view.base.name.clone(), view_and_refs.view.clone());
    }

    let num_features = group_ref.aliased_feature_names.len();
    let mut vectors = Vec::new();

    for feature_index in 0..num_features {
        let mut vector = FeatureVector {
            name: group_ref.aliased_feature_names[feature_index].clone(),
            values: vec![types::Value { val: None }; num_rows],
            statuses: vec![serving::FieldStatus::Invalid; num_rows],
            timestamps: vec![Timestamp::default(); num_rows],
        };

        for (row_entity_index, output_indexes) in group_ref.indices.iter().enumerate() {
            let (value, status, timestamp) = if let Some(row_values) = feature_data_rows.get(row_entity_index).and_then(|row| row.as_ref()) {
                let feature_data = &row_values[feature_index];
                let feature_view_name = &feature_data.reference.feature_view_name;
                let feature_view = fvs.get(feature_view_name).context("feature view missing")?;
                let timestamp = feature_data.timestamp.clone().unwrap_or_default();

                if matches!(feature_data.value.val, Some(types::value::Val::NullVal(_))) {
                    (types::Value { val: None }, serving::FieldStatus::NotFound, timestamp)
                } else if check_outside_ttl(&timestamp, feature_view.ttl.as_ref()) {
                    (feature_data.value.clone(), serving::FieldStatus::OutsideMaxAge, timestamp)
                } else {
                    (feature_data.value.clone(), serving::FieldStatus::Present, timestamp)
                }
            } else {
                (types::Value { val: None }, serving::FieldStatus::NotFound, Timestamp::default())
            };

            for row_index in output_indexes {
                vector.values[*row_index] = value.clone();
                vector.statuses[*row_index] = status;
                vector.timestamps[*row_index] = timestamp.clone();
            }
        }

        vectors.push(vector);
    }

    Ok(vectors)
}

pub fn keep_only_requested_features(
    vectors: Vec<FeatureVector>,
    requested_feature_refs: &[String],
    feature_service: Option<&model::FeatureService>,
    full_feature_names: bool,
) -> Result<Vec<FeatureVector>> {
    let mut vectors_by_name = HashMap::new();
    for vector in vectors {
        vectors_by_name.insert(vector.name.clone(), vector);
    }

    let mut expected_refs = requested_feature_refs.to_vec();
    if let Some(service) = feature_service {
        for projection in &service.projections {
            for feature in &projection.features {
                expected_refs.push(format!("{}:{}", projection.name_to_use(), feature.name));
            }
        }
    }

    let mut expected_vectors = Vec::new();
    for feature_ref in expected_refs {
        let (view_name, feature_name) = parse_feature_reference(&feature_ref)?;
        let qualified_name = get_qualified_feature_name(&view_name, &feature_name, full_feature_names);
        let vector = vectors_by_name
            .remove(&qualified_name)
            .with_context(|| format!("requested feature {feature_ref} can't be retrieved"))?;
        expected_vectors.push(vector);
    }

    Ok(expected_vectors)
}

pub fn entities_to_feature_vectors(
    entity_columns: &HashMap<String, Vec<types::Value>>,
    num_rows: usize,
) -> Result<Vec<FeatureVector>> {
    let mut vectors = Vec::new();
    let now = now_timestamp();
    let present = serving::FieldStatus::Present;

    for (entity_name, values) in entity_columns {
        let mut timestamps = Vec::with_capacity(num_rows);
        let mut statuses = Vec::with_capacity(num_rows);
        for _ in 0..num_rows {
            timestamps.push(now.clone());
            statuses.push(present);
        }
        vectors.push(FeatureVector {
            name: entity_name.clone(),
            values: values.clone(),
            statuses,
            timestamps,
        });
    }

    Ok(vectors)
}

fn check_outside_ttl(feature_timestamp: &Timestamp, ttl: Option<&Duration>) -> bool {
    let ttl = match ttl {
        Some(ttl) if ttl.seconds > 0 => ttl.seconds,
        _ => return false,
    };
    let current = now_timestamp();
    current.seconds - feature_timestamp.seconds > ttl
}

fn get_qualified_feature_name(view_name: &str, feature_name: &str, full_feature_names: bool) -> String {
    if full_feature_names {
        format!("{view_name}__{feature_name}")
    } else {
        feature_name.to_string()
    }
}

pub fn now_timestamp() -> Timestamp {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    Timestamp {
        seconds: now.as_secs() as i64,
        nanos: now.subsec_nanos() as i32,
    }
}
