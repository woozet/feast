use crate::config;
use crate::featurestore::FeatureStore;
use crate::proto::feast::types;
use anyhow::{Context, Result};
use arrow::array::{
    make_array, Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, BooleanBuilder,
    Float32Array, Float32Builder, Float64Array, Float64Builder, Int32Array, Int32Builder,
    Int64Array, Int64Builder, ListArray, ListBuilder, NullArray, StringArray, StringBuilder,
    StructArray, TimestampSecondArray, TimestampSecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::path::Path;

pub struct ServiceHandle {
    store: FeatureStore,
    runtime: tokio::runtime::Runtime,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct DataTable {
    pub schema: *mut FFI_ArrowSchema,
    pub array: *mut FFI_ArrowArray,
}

#[no_mangle]
pub extern "C" fn feast_rust_new_service(
    repo_path: *const c_char,
    err_out: *mut *mut c_char,
) -> *mut ServiceHandle {
    let result = std::panic::catch_unwind(|| -> Result<ServiceHandle> {
        if repo_path.is_null() {
            anyhow::bail!("repo_path is null");
        }
        let repo_path = unsafe { CStr::from_ptr(repo_path) }
            .to_str()
            .context("invalid repo_path")?;
        let config = config::load_repo_config(Path::new(repo_path))?;
        let store = FeatureStore::new(config)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        Ok(ServiceHandle {
            store,
            runtime,
        })
    });

    match result {
        Ok(Ok(handle)) => Box::into_raw(Box::new(handle)),
        Ok(Err(err)) => {
            set_error(err_out, err.to_string());
            std::ptr::null_mut()
        }
        Err(panic) => {
            set_error(err_out, format!("panic: {}", panic_message(panic)));
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn feast_rust_free_service(service: *mut ServiceHandle) {
    if service.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(service));
    }
}

#[no_mangle]
pub extern "C" fn feast_rust_get_online_features(
    service: *mut ServiceHandle,
    feature_refs: *const *const c_char,
    feature_refs_len: usize,
    feature_service_name: *const c_char,
    entities: DataTable,
    request_data: DataTable,
    full_feature_names: bool,
    output: *mut DataTable,
    err_out: *mut *mut c_char,
) -> bool {
    let result = std::panic::catch_unwind(|| -> Result<()> {
        if service.is_null() {
            anyhow::bail!("service is null");
        }
        if output.is_null() {
            anyhow::bail!("output is null");
        }

        let feature_refs = unsafe { c_str_array_to_vec(feature_refs, feature_refs_len)? };
        let feature_service_name = unsafe { optional_c_string(feature_service_name)? };

        let entities_batch = unsafe { import_record_batch(entities)? };
        let request_batch = unsafe { import_record_batch(request_data)? };
        let join_key_values = record_batch_to_values(&entities_batch)?;
        let request_values = record_batch_to_values(&request_batch)?;

        let handle = unsafe { &*service };
        let store = &handle.store;

        let feature_service = if let Some(name) = feature_service_name {
            Some(store.get_feature_service(&name)?)
        } else {
            None
        };

        if feature_service.is_none() && feature_refs.is_empty() {
            anyhow::bail!("either feature_service or feature_refs is required");
        }

        let vectors = handle.runtime.block_on(store.get_online_features(
            feature_refs,
            feature_service,
            join_key_values,
            request_values,
            full_feature_names,
        ))?;

        let batch = vectors_to_record_batch(&vectors)?;
        export_record_batch(output, batch)?;
        Ok(())
    });

    match result {
        Ok(Ok(_)) => true,
        Ok(Err(err)) => {
            set_error(err_out, err.to_string());
            false
        }
        Err(panic) => {
            set_error(err_out, format!("panic: {}", panic_message(panic)));
            false
        }
    }
}

#[no_mangle]
pub extern "C" fn feast_rust_free_string(ptr: *mut c_char) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        drop(CString::from_raw(ptr));
    }
}

fn set_error(err_out: *mut *mut c_char, message: String) {
    if err_out.is_null() {
        return;
    }
    if let Ok(cstr) = CString::new(message) {
        unsafe {
            *err_out = cstr.into_raw();
        }
    }
}

fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    if let Some(msg) = panic.downcast_ref::<&str>() {
        (*msg).to_string()
    } else if let Some(msg) = panic.downcast_ref::<String>() {
        msg.clone()
    } else {
        "unknown panic".to_string()
    }
}

unsafe fn c_str_array_to_vec(
    ptr: *const *const c_char,
    len: usize,
) -> Result<Vec<String>> {
    if ptr.is_null() || len == 0 {
        return Ok(Vec::new());
    }
    let slice = std::slice::from_raw_parts(ptr, len);
    let mut values = Vec::with_capacity(len);
    for &item in slice {
        if item.is_null() {
            continue;
        }
        let value = CStr::from_ptr(item).to_str()?.to_string();
        values.push(value);
    }
    Ok(values)
}

unsafe fn optional_c_string(ptr: *const c_char) -> Result<Option<String>> {
    if ptr.is_null() {
        return Ok(None);
    }
    let value = CStr::from_ptr(ptr).to_str()?.to_string();
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(value))
    }
}

unsafe fn import_record_batch(table: DataTable) -> Result<RecordBatch> {
    if table.schema.is_null() || table.array.is_null() {
        anyhow::bail!("input table schema/array is null");
    }

    let ffi_schema = FFI_ArrowSchema::from_raw(table.schema);
    let ffi_array = FFI_ArrowArray::from_raw(table.array);
    let data = unsafe { from_ffi(ffi_array, &ffi_schema) }?;
    drop(ffi_schema);

    let array = make_array(data);
    let struct_array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .context("expected StructArray for record batch")?;

    Ok(RecordBatch::from(struct_array))
}

fn export_record_batch(output: *mut DataTable, batch: RecordBatch) -> Result<()> {
    let out = unsafe { &mut *output };
    if out.schema.is_null() || out.array.is_null() {
        anyhow::bail!("output table schema/array is null");
    }

    let struct_array = StructArray::from(batch);
    let (ffi_array, ffi_schema) = to_ffi(&struct_array.to_data())?;

    unsafe {
        std::ptr::write(out.schema, ffi_schema);
        std::ptr::write(out.array, ffi_array);
    }

    Ok(())
}

fn record_batch_to_values(
    batch: &RecordBatch,
) -> Result<std::collections::HashMap<String, Vec<types::Value>>> {
    let mut map = std::collections::HashMap::new();
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        let values = array_to_values(batch.column(idx))?;
        map.insert(field.name().to_string(), values);
    }
    Ok(map)
}

fn array_to_values(array: &ArrayRef) -> Result<Vec<types::Value>> {
    match array.data_type() {
        DataType::Null => Ok(vec![value_none(); array.len()]),
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .context("invalid int32 array")?;
            Ok(arr
                .iter()
                .map(|value| match value {
                    Some(v) => value_some(types::value::Val::Int32Val(v)),
                    None => value_none(),
                })
                .collect())
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("invalid int64 array")?;
            Ok(arr
                .iter()
                .map(|value| match value {
                    Some(v) => value_some(types::value::Val::Int64Val(v)),
                    None => value_none(),
                })
                .collect())
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .context("invalid float32 array")?;
            Ok(arr
                .iter()
                .map(|value| match value {
                    Some(v) => value_some(types::value::Val::FloatVal(v)),
                    None => value_none(),
                })
                .collect())
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .context("invalid float64 array")?;
            Ok(arr
                .iter()
                .map(|value| match value {
                    Some(v) => value_some(types::value::Val::DoubleVal(v)),
                    None => value_none(),
                })
                .collect())
        }
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .context("invalid bool array")?;
            Ok(arr
                .iter()
                .map(|value| match value {
                    Some(v) => value_some(types::value::Val::BoolVal(v)),
                    None => value_none(),
                })
                .collect())
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .context("invalid string array")?;
            Ok(arr
                .iter()
                .map(|value| match value {
                    Some(v) => value_some(types::value::Val::StringVal(v.to_string())),
                    None => value_none(),
                })
                .collect())
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .context("invalid binary array")?;
            Ok(arr
                .iter()
                .map(|value| match value {
                    Some(v) => value_some(types::value::Val::BytesVal(v.to_vec())),
                    None => value_none(),
                })
                .collect())
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .context("invalid timestamp array")?;
            Ok(arr
                .iter()
                .map(|value| match value {
                    Some(v) => value_some(types::value::Val::UnixTimestampVal(v)),
                    None => value_none(),
                })
                .collect())
        }
        DataType::List(field) => array_to_list_values(array, field.data_type()),
        other => anyhow::bail!("unsupported data type: {other:?}"),
    }
}

fn array_to_list_values(array: &ArrayRef, element_type: &DataType) -> Result<Vec<types::Value>> {
    let arr = array
        .as_any()
        .downcast_ref::<ListArray>()
        .context("invalid list array")?;

    let mut values = Vec::with_capacity(arr.len());
    for i in 0..arr.len() {
        if arr.is_null(i) {
            values.push(value_none());
            continue;
        }
        let child = arr.value(i);
        let val = list_values_from_array(child, element_type)?;
        values.push(value_some(val));
    }

    Ok(values)
}

fn list_values_from_array(array: ArrayRef, element_type: &DataType) -> Result<types::value::Val> {
    match element_type {
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .context("invalid int32 list")?;
            let vals = arr
                .iter()
                .map(|value| value.ok_or_else(|| anyhow::anyhow!("null list value")))
                .collect::<Result<Vec<_>>>()?;
            Ok(types::value::Val::Int32ListVal(types::Int32List { val: vals }))
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("invalid int64 list")?;
            let vals = arr
                .iter()
                .map(|value| value.ok_or_else(|| anyhow::anyhow!("null list value")))
                .collect::<Result<Vec<_>>>()?;
            Ok(types::value::Val::Int64ListVal(types::Int64List { val: vals }))
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .context("invalid float32 list")?;
            let vals = arr
                .iter()
                .map(|value| value.ok_or_else(|| anyhow::anyhow!("null list value")))
                .collect::<Result<Vec<_>>>()?;
            Ok(types::value::Val::FloatListVal(types::FloatList { val: vals }))
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .context("invalid float64 list")?;
            let vals = arr
                .iter()
                .map(|value| value.ok_or_else(|| anyhow::anyhow!("null list value")))
                .collect::<Result<Vec<_>>>()?;
            Ok(types::value::Val::DoubleListVal(types::DoubleList { val: vals }))
        }
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .context("invalid bool list")?;
            let vals = arr
                .iter()
                .map(|value| value.ok_or_else(|| anyhow::anyhow!("null list value")))
                .collect::<Result<Vec<_>>>()?;
            Ok(types::value::Val::BoolListVal(types::BoolList { val: vals }))
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .context("invalid string list")?;
            let vals = arr
                .iter()
                .map(|value| {
                    value
                        .map(|v| v.to_string())
                        .ok_or_else(|| anyhow::anyhow!("null list value"))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(types::value::Val::StringListVal(types::StringList { val: vals }))
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .context("invalid binary list")?;
            let vals = arr
                .iter()
                .map(|value| {
                    value
                        .map(|v| v.to_vec())
                        .ok_or_else(|| anyhow::anyhow!("null list value"))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(types::value::Val::BytesListVal(types::BytesList { val: vals }))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .context("invalid timestamp list")?;
            let vals = arr
                .iter()
                .map(|value| value.ok_or_else(|| anyhow::anyhow!("null list value")))
                .collect::<Result<Vec<_>>>()?;
            Ok(types::value::Val::UnixTimestampListVal(types::Int64List { val: vals }))
        }
        other => anyhow::bail!("unsupported list type: {other:?}"),
    }
}

fn vectors_to_record_batch(vectors: &[crate::onlineserving::FeatureVector]) -> Result<RecordBatch> {
    let mut fields = Vec::new();
    let mut columns = Vec::new();

    for vector in vectors {
        let (values_array, value_type) = values_to_array(&vector.values)?;
        fields.push(Field::new(vector.name.clone(), value_type, true));
        columns.push(values_array);

        let status_values = vector
            .statuses
            .iter()
            .map(|status| Some(*status as i32))
            .collect::<Vec<_>>();
        let status_array = Int32Array::from(status_values);
        fields.push(Field::new(
            format!("{}__status", vector.name),
            DataType::Int32,
            true,
        ));
        columns.push(std::sync::Arc::new(status_array));

        let timestamp_values = vector
            .timestamps
            .iter()
            .map(|timestamp| Some(timestamp.seconds))
            .collect::<Vec<_>>();
        let timestamp_array = Int64Array::from(timestamp_values);
        fields.push(Field::new(
            format!("{}__timestamp", vector.name),
            DataType::Int64,
            true,
        ));
        columns.push(std::sync::Arc::new(timestamp_array));
    }

    let schema = std::sync::Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).map_err(|err| err.into())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ValueKind {
    Null,
    Int32,
    Int64,
    Float32,
    Float64,
    Bool,
    String,
    Bytes,
    Timestamp,
    Int32List,
    Int64List,
    Float32List,
    Float64List,
    BoolList,
    StringList,
    BytesList,
    TimestampList,
}

fn values_to_array(values: &[types::Value]) -> Result<(ArrayRef, DataType)> {
    let kind = infer_value_kind(values)?;
    match kind {
        ValueKind::Null => {
            let array = NullArray::new(values.len());
            Ok((std::sync::Arc::new(array), DataType::Null))
        }
        ValueKind::Int32 => {
            let mut builder = Int32Builder::with_capacity(values.len());
            for value in values {
                match &value.val {
                    Some(types::value::Val::Int32Val(v)) => builder.append_value(*v),
                    None => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok((std::sync::Arc::new(builder.finish()), DataType::Int32))
        }
        ValueKind::Int64 => {
            let mut builder = Int64Builder::with_capacity(values.len());
            for value in values {
                match &value.val {
                    Some(types::value::Val::Int64Val(v)) => builder.append_value(*v),
                    None => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok((std::sync::Arc::new(builder.finish()), DataType::Int64))
        }
        ValueKind::Float32 => {
            let mut builder = Float32Builder::with_capacity(values.len());
            for value in values {
                match &value.val {
                    Some(types::value::Val::FloatVal(v)) => builder.append_value(*v),
                    None => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok((std::sync::Arc::new(builder.finish()), DataType::Float32))
        }
        ValueKind::Float64 => {
            let mut builder = Float64Builder::with_capacity(values.len());
            for value in values {
                match &value.val {
                    Some(types::value::Val::DoubleVal(v)) => builder.append_value(*v),
                    None => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok((std::sync::Arc::new(builder.finish()), DataType::Float64))
        }
        ValueKind::Bool => {
            let mut builder = BooleanBuilder::with_capacity(values.len());
            for value in values {
                match &value.val {
                    Some(types::value::Val::BoolVal(v)) => builder.append_value(*v),
                    None => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok((std::sync::Arc::new(builder.finish()), DataType::Boolean))
        }
        ValueKind::String => {
            let mut builder = StringBuilder::with_capacity(values.len(), values.len());
            for value in values {
                match &value.val {
                    Some(types::value::Val::StringVal(v)) => builder.append_value(v),
                    None => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok((std::sync::Arc::new(builder.finish()), DataType::Utf8))
        }
        ValueKind::Bytes => {
            let mut builder = BinaryBuilder::with_capacity(values.len(), values.len());
            for value in values {
                match &value.val {
                    Some(types::value::Val::BytesVal(v)) => builder.append_value(v),
                    None => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok((std::sync::Arc::new(builder.finish()), DataType::Binary))
        }
        ValueKind::Timestamp => {
            let mut builder = TimestampSecondBuilder::with_capacity(values.len());
            for value in values {
                match &value.val {
                    Some(types::value::Val::UnixTimestampVal(v)) => builder.append_value(*v),
                    None => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok((
                std::sync::Arc::new(builder.finish()),
                DataType::Timestamp(TimeUnit::Second, None),
            ))
        }
        ValueKind::Int32List => {
            let array = build_int32_list_array(values)?;
            Ok((
                array,
                DataType::List(std::sync::Arc::new(Field::new(
                    "item",
                    DataType::Int32,
                    true,
                ))),
            ))
        }
        ValueKind::Int64List => {
            let array = build_int64_list_array(values)?;
            Ok((
                array,
                DataType::List(std::sync::Arc::new(Field::new(
                    "item",
                    DataType::Int64,
                    true,
                ))),
            ))
        }
        ValueKind::Float32List => {
            let array = build_float32_list_array(values)?;
            Ok((
                array,
                DataType::List(std::sync::Arc::new(Field::new(
                    "item",
                    DataType::Float32,
                    true,
                ))),
            ))
        }
        ValueKind::Float64List => {
            let array = build_float64_list_array(values)?;
            Ok((
                array,
                DataType::List(std::sync::Arc::new(Field::new(
                    "item",
                    DataType::Float64,
                    true,
                ))),
            ))
        }
        ValueKind::BoolList => {
            let array = build_bool_list_array(values)?;
            Ok((
                array,
                DataType::List(std::sync::Arc::new(Field::new(
                    "item",
                    DataType::Boolean,
                    true,
                ))),
            ))
        }
        ValueKind::StringList => {
            let array = build_string_list_array(values)?;
            Ok((
                array,
                DataType::List(std::sync::Arc::new(Field::new(
                    "item",
                    DataType::Utf8,
                    true,
                ))),
            ))
        }
        ValueKind::BytesList => {
            let array = build_bytes_list_array(values)?;
            Ok((
                array,
                DataType::List(std::sync::Arc::new(Field::new(
                    "item",
                    DataType::Binary,
                    true,
                ))),
            ))
        }
        ValueKind::TimestampList => {
            Ok((
                build_timestamp_list_array(values)?,
                DataType::List(std::sync::Arc::new(Field::new(
                    "item",
                    DataType::Timestamp(TimeUnit::Second, None),
                    true,
                ))),
            ))
        }
    }
}

fn build_int32_list_array(values: &[types::Value]) -> Result<ArrayRef> {
    let mut builder = ListBuilder::new(Int32Builder::new());
    for value in values {
        match &value.val {
            Some(types::value::Val::Int32ListVal(list)) => {
                for item in &list.val {
                    builder.values().append_value(*item);
                }
                builder.append(true);
            }
            None => builder.append(false),
            _ => builder.append(false),
        }
    }
    Ok(std::sync::Arc::new(builder.finish()))
}

fn build_int64_list_array(values: &[types::Value]) -> Result<ArrayRef> {
    let mut builder = ListBuilder::new(Int64Builder::new());
    for value in values {
        match &value.val {
            Some(types::value::Val::Int64ListVal(list)) => {
                for item in &list.val {
                    builder.values().append_value(*item);
                }
                builder.append(true);
            }
            None => builder.append(false),
            _ => builder.append(false),
        }
    }
    Ok(std::sync::Arc::new(builder.finish()))
}

fn build_float32_list_array(values: &[types::Value]) -> Result<ArrayRef> {
    let mut builder = ListBuilder::new(Float32Builder::new());
    for value in values {
        match &value.val {
            Some(types::value::Val::FloatListVal(list)) => {
                for item in &list.val {
                    builder.values().append_value(*item);
                }
                builder.append(true);
            }
            None => builder.append(false),
            _ => builder.append(false),
        }
    }
    Ok(std::sync::Arc::new(builder.finish()))
}

fn build_float64_list_array(values: &[types::Value]) -> Result<ArrayRef> {
    let mut builder = ListBuilder::new(Float64Builder::new());
    for value in values {
        match &value.val {
            Some(types::value::Val::DoubleListVal(list)) => {
                for item in &list.val {
                    builder.values().append_value(*item);
                }
                builder.append(true);
            }
            None => builder.append(false),
            _ => builder.append(false),
        }
    }
    Ok(std::sync::Arc::new(builder.finish()))
}

fn build_bool_list_array(values: &[types::Value]) -> Result<ArrayRef> {
    let mut builder = ListBuilder::new(BooleanBuilder::new());
    for value in values {
        match &value.val {
            Some(types::value::Val::BoolListVal(list)) => {
                for item in &list.val {
                    builder.values().append_value(*item);
                }
                builder.append(true);
            }
            None => builder.append(false),
            _ => builder.append(false),
        }
    }
    Ok(std::sync::Arc::new(builder.finish()))
}

fn build_string_list_array(values: &[types::Value]) -> Result<ArrayRef> {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for value in values {
        match &value.val {
            Some(types::value::Val::StringListVal(list)) => {
                for item in &list.val {
                    builder.values().append_value(item);
                }
                builder.append(true);
            }
            None => builder.append(false),
            _ => builder.append(false),
        }
    }
    Ok(std::sync::Arc::new(builder.finish()))
}

fn build_bytes_list_array(values: &[types::Value]) -> Result<ArrayRef> {
    let mut builder = ListBuilder::new(BinaryBuilder::new());
    for value in values {
        match &value.val {
            Some(types::value::Val::BytesListVal(list)) => {
                for item in &list.val {
                    builder.values().append_value(item);
                }
                builder.append(true);
            }
            None => builder.append(false),
            _ => builder.append(false),
        }
    }
    Ok(std::sync::Arc::new(builder.finish()))
}

fn build_timestamp_list_array(values: &[types::Value]) -> Result<ArrayRef> {
    let mut builder = ListBuilder::new(TimestampSecondBuilder::new());
    for value in values {
        match &value.val {
            Some(types::value::Val::UnixTimestampListVal(list)) => {
                for item in &list.val {
                    builder.values().append_value(*item);
                }
                builder.append(true);
            }
            None => builder.append(false),
            _ => builder.append(false),
        }
    }
    Ok(std::sync::Arc::new(builder.finish()))
}

fn infer_value_kind(values: &[types::Value]) -> Result<ValueKind> {
    let mut kind: Option<ValueKind> = None;
    for value in values {
        let Some(val) = &value.val else { continue; };
        let current = value_kind_for(val)?;
        if let Some(existing) = kind {
            if existing != current {
                anyhow::bail!("mixed value types are not supported");
            }
        } else {
            kind = Some(current);
        }
    }
    Ok(kind.unwrap_or(ValueKind::Null))
}

fn value_kind_for(val: &types::value::Val) -> Result<ValueKind> {
    let kind = match val {
        types::value::Val::Int32Val(_) => ValueKind::Int32,
        types::value::Val::Int64Val(_) => ValueKind::Int64,
        types::value::Val::FloatVal(_) => ValueKind::Float32,
        types::value::Val::DoubleVal(_) => ValueKind::Float64,
        types::value::Val::BoolVal(_) => ValueKind::Bool,
        types::value::Val::StringVal(_) => ValueKind::String,
        types::value::Val::BytesVal(_) => ValueKind::Bytes,
        types::value::Val::UnixTimestampVal(_) => ValueKind::Timestamp,
        types::value::Val::Int32ListVal(_) => ValueKind::Int32List,
        types::value::Val::Int64ListVal(_) => ValueKind::Int64List,
        types::value::Val::FloatListVal(_) => ValueKind::Float32List,
        types::value::Val::DoubleListVal(_) => ValueKind::Float64List,
        types::value::Val::BoolListVal(_) => ValueKind::BoolList,
        types::value::Val::StringListVal(_) => ValueKind::StringList,
        types::value::Val::BytesListVal(_) => ValueKind::BytesList,
        types::value::Val::UnixTimestampListVal(_) => ValueKind::TimestampList,
        types::value::Val::NullVal(_) => ValueKind::Null,
        types::value::Val::MapVal(_) | types::value::Val::MapListVal(_) => {
            anyhow::bail!("map values are not supported")
        }
    };
    Ok(kind)
}

fn value_none() -> types::Value {
    types::Value { val: None }
}

fn value_some(val: types::value::Val) -> types::Value {
    types::Value { val: Some(val) }
}
