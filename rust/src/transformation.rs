use crate::model;
use crate::onlineserving::FeatureVector;
use crate::proto::feast::serving;
use crate::proto::feast::types;
use anyhow::{Context, Result};
use arrow::array::{
    Array, ArrayRef, BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder,
    Int64Builder, ListBuilder, StringBuilder, TimestampSecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::{reader::FileReader, writer::FileWriter};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

pub struct GrpcTransformationService {
    project: String,
    channel: tonic::transport::Channel,
}

impl GrpcTransformationService {
    pub fn from_config(config: &crate::config::RepoConfig) -> Result<Option<Self>> {
        let endpoint = config
            .feature_server
            .get("transformation_service_endpoint")
            .and_then(|value| value.as_str());
        if let Some(endpoint) = endpoint {
            let channel = new_transformation_channel(endpoint)?;
            Ok(Some(Self {
                project: config.project.clone(),
                channel,
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn get_transformation(
        &self,
        feature_view: &model::OnDemandFeatureView,
        retrieved_features: &HashMap<String, Vec<types::Value>>,
        request_context: &HashMap<String, Vec<types::Value>>,
        num_rows: usize,
    ) -> Result<Vec<FeatureVector>> {
        let mut fields = Vec::new();
        let mut arrays = Vec::new();

        for (name, values) in retrieved_features {
            let array = proto_values_to_array(values, num_rows)?;
            fields.push(Field::new(name, array.data_type().clone(), true));
            arrays.push(array);
        }
        for (name, values) in request_context {
            let array = proto_values_to_array(values, num_rows)?;
            fields.push(Field::new(name, array.data_type().clone(), true));
            arrays.push(array);
        }

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), arrays)?;
        let input_bytes = record_batch_to_ipc(&schema, &batch)?;

        let request = serving::TransformFeaturesRequest {
            on_demand_feature_view_name: feature_view.base.name.clone(),
            project: self.project.clone(),
            transformation_input: Some(serving::ValueType {
                value: Some(serving::value_type::Value::ArrowValue(input_bytes)),
            }),
        };

        let mut client =
            serving::transformation_service_client::TransformationServiceClient::new(
                self.channel.clone(),
            );
        let response = client.transform_features(request).await?.into_inner();
        let output_bytes = response
            .transformation_output
            .and_then(|value| value.value)
            .and_then(|value| match value {
                serving::value_type::Value::ArrowValue(bytes) => Some(bytes),
            })
            .context("missing transformation output")?;

        extract_transformation_response(feature_view, &output_bytes, num_rows)
    }
}

pub fn ensure_requested_data_exist(
    requested_on_demand_feature_views: &[model::OnDemandFeatureView],
    request_data_features: &HashMap<String, Vec<types::Value>>,
) -> Result<()> {
    let needed_request_data = get_needed_request_data(requested_on_demand_feature_views)?;
    let missing = needed_request_data
        .keys()
        .filter(|key| !request_data_features.contains_key(*key))
        .cloned()
        .collect::<Vec<_>>();

    if !missing.is_empty() {
        anyhow::bail!("request data not found in entity rows: {}", missing.join(", "));
    }
    Ok(())
}

pub async fn augment_response_with_on_demand_transforms(
    service: &GrpcTransformationService,
    on_demand_feature_views: &[model::OnDemandFeatureView],
    request_data: &HashMap<String, Vec<types::Value>>,
    entity_rows: &HashMap<String, Vec<types::Value>>,
    features: &[FeatureVector],
    num_rows: usize,
    _full_feature_names: bool,
) -> Result<Vec<FeatureVector>> {
    let mut result = Vec::new();

    for odfv in on_demand_feature_views {
        let mut request_context = HashMap::new();
        request_context.extend(request_data.clone());
        request_context.extend(entity_rows.clone());

        let mut retrieved_features = HashMap::new();
        for vector in features {
            retrieved_features.insert(vector.name.clone(), vector.values.clone());
        }

        let mut on_demand_vectors = service
            .get_transformation(odfv, &retrieved_features, &request_context, num_rows)
            .await?;
        result.append(&mut on_demand_vectors);
    }

    Ok(result)
}

fn get_needed_request_data(
    requested_on_demand_feature_views: &[model::OnDemandFeatureView],
) -> Result<HashMap<String, types::value_type::Enum>> {
    let mut needed = HashMap::new();
    for on_demand_feature_view in requested_on_demand_feature_views {
        let schema = on_demand_feature_view.get_request_data_schema();
        needed.extend(schema);
    }
    Ok(needed)
}

fn extract_transformation_response(
    feature_view: &model::OnDemandFeatureView,
    arrow_bytes: &[u8],
    num_rows: usize,
) -> Result<Vec<FeatureVector>> {
    let mut reader = FileReader::try_new(Cursor::new(arrow_bytes), None)?;
    let batch = reader
        .next()
        .transpose()?
        .context("missing transformation output batch")?;

    let mut result = Vec::new();

    for (idx, field) in batch.schema().fields().iter().enumerate() {
        let mut drop_feature = true;
        let feature_name = if let Some((_, name)) = field.name().split_once("__") {
            name.to_string()
        } else {
            field.name().to_string()
        };

        for feature in &feature_view.base.projection.features {
            if feature.name == feature_name {
                drop_feature = false;
                break;
            }
        }

        if drop_feature {
            continue;
        }

        let values = arrow_array_to_proto_values(batch.column(idx), num_rows)?;
        let statuses = vec![serving::FieldStatus::Present; num_rows];
        let timestamps = vec![now_timestamp(); num_rows];

        result.push(FeatureVector {
            name: feature_name,
            values,
            statuses,
            timestamps,
        });
    }

    Ok(result)
}

fn record_batch_to_ipc(schema: &Schema, batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    let mut writer = FileWriter::try_new(&mut buffer, schema)?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(buffer)
}

fn proto_values_to_array(values: &[types::Value], num_rows: usize) -> Result<ArrayRef> {
    let mut data_type = DataType::Null;
    for value in values {
        if let Some(dtype) = proto_value_to_data_type(value)? {
            data_type = dtype;
            break;
        }
    }

    if matches!(data_type, DataType::Null) {
        let array = arrow::array::NullArray::new(num_rows);
        return Ok(Arc::new(array));
    }

    let array: ArrayRef = match data_type {
        DataType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for value in values {
                match value.val.as_ref() {
                    Some(types::value::Val::BoolVal(val)) => builder.append_value(*val),
                    None | Some(types::value::Val::NullVal(_)) => builder.append_null(),
                    _ => anyhow::bail!("mixed types in boolean column"),
                };
            }
            Arc::new(builder.finish())
        }
        DataType::Binary => {
            let mut builder = BinaryBuilder::new();
            for value in values {
                match value.val.as_ref() {
                    Some(types::value::Val::BytesVal(val)) => builder.append_value(val),
                    None | Some(types::value::Val::NullVal(_)) => builder.append_null(),
                    _ => anyhow::bail!("mixed types in binary column"),
                };
            }
            Arc::new(builder.finish())
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for value in values {
                match value.val.as_ref() {
                    Some(types::value::Val::StringVal(val)) => builder.append_value(val),
                    None | Some(types::value::Val::NullVal(_)) => builder.append_null(),
                    _ => anyhow::bail!("mixed types in string column"),
                };
            }
            Arc::new(builder.finish())
        }
        DataType::Int32 => {
            let mut builder = Int32Builder::new();
            for value in values {
                match value.val.as_ref() {
                    Some(types::value::Val::Int32Val(val)) => builder.append_value(*val),
                    None | Some(types::value::Val::NullVal(_)) => builder.append_null(),
                    _ => anyhow::bail!("mixed types in int32 column"),
                };
            }
            Arc::new(builder.finish())
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::new();
            for value in values {
                match value.val.as_ref() {
                    Some(types::value::Val::Int64Val(val)) => builder.append_value(*val),
                    None | Some(types::value::Val::NullVal(_)) => builder.append_null(),
                    _ => anyhow::bail!("mixed types in int64 column"),
                };
            }
            Arc::new(builder.finish())
        }
        DataType::Float32 => {
            let mut builder = Float32Builder::new();
            for value in values {
                match value.val.as_ref() {
                    Some(types::value::Val::FloatVal(val)) => builder.append_value(*val),
                    None | Some(types::value::Val::NullVal(_)) => builder.append_null(),
                    _ => anyhow::bail!("mixed types in float column"),
                };
            }
            Arc::new(builder.finish())
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::new();
            for value in values {
                match value.val.as_ref() {
                    Some(types::value::Val::DoubleVal(val)) => builder.append_value(*val),
                    None | Some(types::value::Val::NullVal(_)) => builder.append_null(),
                    _ => anyhow::bail!("mixed types in double column"),
                };
            }
            Arc::new(builder.finish())
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let mut builder = TimestampSecondBuilder::new();
            for value in values {
                match value.val.as_ref() {
                    Some(types::value::Val::UnixTimestampVal(val)) => builder.append_value(*val),
                    None | Some(types::value::Val::NullVal(_)) => builder.append_null(),
                    _ => anyhow::bail!("mixed types in timestamp column"),
                };
            }
            Arc::new(builder.finish())
        }
        DataType::List(field) => match field.data_type() {
            DataType::Boolean => list_builder(values, BooleanBuilder::new(), |builder, value| {
                match value.val.as_ref() {
                    Some(types::value::Val::BoolListVal(list)) => {
                        for v in &list.val {
                            builder.append_value(*v);
                        }
                        Ok(())
                    }
                    _ => anyhow::bail!("mixed list types"),
                }
            })?,
            DataType::Binary => list_builder(values, BinaryBuilder::new(), |builder, value| {
                match value.val.as_ref() {
                    Some(types::value::Val::BytesListVal(list)) => {
                        for v in &list.val {
                            builder.append_value(v);
                        }
                        Ok(())
                    }
                    _ => anyhow::bail!("mixed list types"),
                }
            })?,
            DataType::Utf8 => list_builder(values, StringBuilder::new(), |builder, value| {
                match value.val.as_ref() {
                    Some(types::value::Val::StringListVal(list)) => {
                        for v in &list.val {
                            builder.append_value(v);
                        }
                        Ok(())
                    }
                    _ => anyhow::bail!("mixed list types"),
                }
            })?,
            DataType::Int32 => list_builder(values, Int32Builder::new(), |builder, value| {
                match value.val.as_ref() {
                    Some(types::value::Val::Int32ListVal(list)) => {
                        for v in &list.val {
                            builder.append_value(*v);
                        }
                        Ok(())
                    }
                    _ => anyhow::bail!("mixed list types"),
                }
            })?,
            DataType::Int64 => list_builder(values, Int64Builder::new(), |builder, value| {
                match value.val.as_ref() {
                    Some(types::value::Val::Int64ListVal(list)) => {
                        for v in &list.val {
                            builder.append_value(*v);
                        }
                        Ok(())
                    }
                    _ => anyhow::bail!("mixed list types"),
                }
            })?,
            DataType::Float32 => list_builder(values, Float32Builder::new(), |builder, value| {
                match value.val.as_ref() {
                    Some(types::value::Val::FloatListVal(list)) => {
                        for v in &list.val {
                            builder.append_value(*v);
                        }
                        Ok(())
                    }
                    _ => anyhow::bail!("mixed list types"),
                }
            })?,
            DataType::Float64 => list_builder(values, Float64Builder::new(), |builder, value| {
                match value.val.as_ref() {
                    Some(types::value::Val::DoubleListVal(list)) => {
                        for v in &list.val {
                            builder.append_value(*v);
                        }
                        Ok(())
                    }
                    _ => anyhow::bail!("mixed list types"),
                }
            })?,
            DataType::Timestamp(TimeUnit::Second, _) => {
                list_builder(values, TimestampSecondBuilder::new(), |builder, value| {
                    match value.val.as_ref() {
                        Some(types::value::Val::UnixTimestampListVal(list)) => {
                            for v in &list.val {
                                builder.append_value(*v);
                            }
                            Ok(())
                        }
                        _ => anyhow::bail!("mixed list types"),
                    }
                })?
            }
            _ => anyhow::bail!("unsupported list type"),
        },
        _ => anyhow::bail!("unsupported arrow data type"),
    };

    Ok(array)
}

fn list_builder<T, F>(
    values: &[types::Value],
    inner_builder: T,
    mut append: F,
) -> Result<ArrayRef>
where
    T: arrow::array::ArrayBuilder,
    F: FnMut(&mut T, &types::Value) -> Result<()>,
{
    let mut builder = ListBuilder::new(inner_builder);
    for value in values {
        match value.val.as_ref() {
            None | Some(types::value::Val::NullVal(_)) => builder.append(false),
            _ => {
                builder.append(true);
                append(builder.values(), value)?;
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn proto_value_to_data_type(value: &types::Value) -> Result<Option<DataType>> {
    Ok(match value.val.as_ref() {
        None | Some(types::value::Val::NullVal(_)) => None,
        Some(types::value::Val::BoolVal(_)) => Some(DataType::Boolean),
        Some(types::value::Val::BytesVal(_)) => Some(DataType::Binary),
        Some(types::value::Val::StringVal(_)) => Some(DataType::Utf8),
        Some(types::value::Val::Int32Val(_)) => Some(DataType::Int32),
        Some(types::value::Val::Int64Val(_)) => Some(DataType::Int64),
        Some(types::value::Val::FloatVal(_)) => Some(DataType::Float32),
        Some(types::value::Val::DoubleVal(_)) => Some(DataType::Float64),
        Some(types::value::Val::BoolListVal(_)) => Some(list_type(DataType::Boolean)),
        Some(types::value::Val::BytesListVal(_)) => Some(list_type(DataType::Binary)),
        Some(types::value::Val::StringListVal(_)) => Some(list_type(DataType::Utf8)),
        Some(types::value::Val::Int32ListVal(_)) => Some(list_type(DataType::Int32)),
        Some(types::value::Val::Int64ListVal(_)) => Some(list_type(DataType::Int64)),
        Some(types::value::Val::FloatListVal(_)) => Some(list_type(DataType::Float32)),
        Some(types::value::Val::DoubleListVal(_)) => Some(list_type(DataType::Float64)),
        Some(types::value::Val::UnixTimestampVal(_)) => {
            Some(DataType::Timestamp(TimeUnit::Second, None))
        }
        Some(types::value::Val::UnixTimestampListVal(_)) => {
            Some(list_type(DataType::Timestamp(TimeUnit::Second, None)))
        }
        _ => anyhow::bail!("unsupported proto value type"),
    })
}

fn list_type(data_type: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("item", data_type, true)))
}

fn arrow_array_to_proto_values(array: &ArrayRef, num_rows: usize) -> Result<Vec<types::Value>> {
    let mut values = Vec::with_capacity(num_rows);
    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<arrow::array::BooleanArray>().unwrap();
            for i in 0..arr.len() {
                values.push(if arr.is_null(i) {
                    types::Value { val: None }
                } else {
                    types::Value {
                        val: Some(types::value::Val::BoolVal(arr.value(i))),
                    }
                });
            }
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<arrow::array::BinaryArray>().unwrap();
            for i in 0..arr.len() {
                values.push(if arr.is_null(i) {
                    types::Value { val: None }
                } else {
                    types::Value {
                        val: Some(types::value::Val::BytesVal(arr.value(i).to_vec())),
                    }
                });
            }
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
            for i in 0..arr.len() {
                values.push(if arr.is_null(i) {
                    types::Value { val: None }
                } else {
                    types::Value {
                        val: Some(types::value::Val::StringVal(arr.value(i).to_string())),
                    }
                });
            }
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
            for i in 0..arr.len() {
                values.push(if arr.is_null(i) {
                    types::Value { val: None }
                } else {
                    types::Value {
                        val: Some(types::value::Val::Int32Val(arr.value(i))),
                    }
                });
            }
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
            for i in 0..arr.len() {
                values.push(if arr.is_null(i) {
                    types::Value { val: None }
                } else {
                    types::Value {
                        val: Some(types::value::Val::Int64Val(arr.value(i))),
                    }
                });
            }
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<arrow::array::Float32Array>().unwrap();
            for i in 0..arr.len() {
                values.push(if arr.is_null(i) {
                    types::Value { val: None }
                } else {
                    types::Value {
                        val: Some(types::value::Val::FloatVal(arr.value(i))),
                    }
                });
            }
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
            for i in 0..arr.len() {
                values.push(if arr.is_null(i) {
                    types::Value { val: None }
                } else {
                    types::Value {
                        val: Some(types::value::Val::DoubleVal(arr.value(i))),
                    }
                });
            }
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampSecondArray>()
                .unwrap();
            for i in 0..arr.len() {
                values.push(if arr.is_null(i) {
                    types::Value { val: None }
                } else {
                    types::Value {
                        val: Some(types::value::Val::UnixTimestampVal(arr.value(i))),
                    }
                });
            }
        }
        DataType::List(field) => {
            let arr = array.as_any().downcast_ref::<arrow::array::ListArray>().unwrap();
            let list_values = arr.values();
            let offsets = arr.value_offsets();
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    values.push(types::Value { val: None });
                    continue;
                }
                let start = offsets[i] as usize;
                let end = offsets[i + 1] as usize;
                let slice = list_values.slice(start, end - start);
                let list_value = match field.data_type() {
                    DataType::Boolean => {
                        let arr = slice
                            .as_any()
                            .downcast_ref::<arrow::array::BooleanArray>()
                            .unwrap();
                        let mut vals = Vec::new();
                        for j in 0..arr.len() {
                            vals.push(arr.value(j));
                        }
                        types::value::Val::BoolListVal(types::BoolList { val: vals })
                    }
                    DataType::Binary => {
                        let arr = slice
                            .as_any()
                            .downcast_ref::<arrow::array::BinaryArray>()
                            .unwrap();
                        let mut vals = Vec::new();
                        for j in 0..arr.len() {
                            vals.push(arr.value(j).to_vec());
                        }
                        types::value::Val::BytesListVal(types::BytesList { val: vals })
                    }
                    DataType::Utf8 => {
                        let arr = slice
                            .as_any()
                            .downcast_ref::<arrow::array::StringArray>()
                            .unwrap();
                        let mut vals = Vec::new();
                        for j in 0..arr.len() {
                            vals.push(arr.value(j).to_string());
                        }
                        types::value::Val::StringListVal(types::StringList { val: vals })
                    }
                    DataType::Int32 => {
                        let arr = slice
                            .as_any()
                            .downcast_ref::<arrow::array::Int32Array>()
                            .unwrap();
                        let mut vals = Vec::new();
                        for j in 0..arr.len() {
                            vals.push(arr.value(j));
                        }
                        types::value::Val::Int32ListVal(types::Int32List { val: vals })
                    }
                    DataType::Int64 => {
                        let arr = slice
                            .as_any()
                            .downcast_ref::<arrow::array::Int64Array>()
                            .unwrap();
                        let mut vals = Vec::new();
                        for j in 0..arr.len() {
                            vals.push(arr.value(j));
                        }
                        types::value::Val::Int64ListVal(types::Int64List { val: vals })
                    }
                    DataType::Float32 => {
                        let arr = slice
                            .as_any()
                            .downcast_ref::<arrow::array::Float32Array>()
                            .unwrap();
                        let mut vals = Vec::new();
                        for j in 0..arr.len() {
                            vals.push(arr.value(j));
                        }
                        types::value::Val::FloatListVal(types::FloatList { val: vals })
                    }
                    DataType::Float64 => {
                        let arr = slice
                            .as_any()
                            .downcast_ref::<arrow::array::Float64Array>()
                            .unwrap();
                        let mut vals = Vec::new();
                        for j in 0..arr.len() {
                            vals.push(arr.value(j));
                        }
                        types::value::Val::DoubleListVal(types::DoubleList { val: vals })
                    }
                    DataType::Timestamp(TimeUnit::Second, _) => {
                        let arr = slice
                            .as_any()
                            .downcast_ref::<arrow::array::TimestampSecondArray>()
                            .unwrap();
                        let mut vals = Vec::new();
                        for j in 0..arr.len() {
                            vals.push(arr.value(j));
                        }
                        types::value::Val::UnixTimestampListVal(types::Int64List { val: vals })
                    }
                    _ => anyhow::bail!("unsupported list element type"),
                };
                values.push(types::Value {
                    val: Some(list_value),
                });
            }
        }
        DataType::Null => {
            for _ in 0..num_rows {
                values.push(types::Value { val: None });
            }
        }
        _ => anyhow::bail!("unsupported arrow array type"),
    }

    Ok(values)
}

fn now_timestamp() -> prost_types::Timestamp {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    prost_types::Timestamp {
        seconds: now.as_secs() as i64,
        nanos: now.subsec_nanos() as i32,
    }
}

fn new_transformation_channel(endpoint: &str) -> Result<tonic::transport::Channel> {
    let uri = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    };
    let channel = tonic::transport::Endpoint::from_shared(uri)?.connect_lazy();
    Ok(channel)
}
