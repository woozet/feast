use crate::featurestore::FeatureStore;
use crate::proto::feast::serving::serving_service_server::{
    ServingService, ServingServiceServer,
};
use crate::proto::feast::serving::{
    self, GetFeastServingInfoRequest, GetFeastServingInfoResponse, GetOnlineFeaturesRequest,
    GetOnlineFeaturesResponse,
};
use crate::proto::feast::types;
use anyhow::Result;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use base64::Engine;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

const FEAST_SERVER_VERSION: &str = "0.0.1";

#[derive(Clone)]
struct AppState {
    store: Arc<Mutex<FeatureStore>>,
}

pub async fn start_http(store: FeatureStore, host: &str, port: u16) -> Result<()> {
    let addr = bind_addr(host, port)?;
    let state = AppState {
        store: Arc::new(Mutex::new(store)),
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/get-online-features", post(get_online_features))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

pub async fn start_grpc(store: FeatureStore, host: &str, port: u16) -> Result<()> {
    let addr = bind_addr(host, port)?;
    let service = GrpcServingService::new(Arc::new(Mutex::new(store)));

    tonic::transport::Server::builder()
        .add_service(ServingServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}

async fn health() -> StatusCode {
    StatusCode::OK
}

#[derive(Debug, Deserialize)]
struct HttpGetOnlineFeaturesRequest {
    #[serde(default)]
    feature_service: Option<String>,
    #[serde(default)]
    features: Vec<String>,
    entities: HashMap<String, Vec<JsonValue>>,
    #[serde(default)]
    full_feature_names: bool,
    #[serde(default)]
    request_context: HashMap<String, Vec<JsonValue>>,
}

async fn get_online_features(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
    Json(request): Json<HttpGetOnlineFeaturesRequest>,
) -> impl IntoResponse {
    let status_flag = if let Some(value) = params.get("status") {
        match value.parse::<bool>() {
            Ok(parsed) => parsed,
            Err(_) => {
                return json_error("invalid status query parameter", StatusCode::BAD_REQUEST);
            }
        }
    } else {
        false
    };

    let mut store = state.store.lock().await;
    let feature_service = if let Some(name) = request.feature_service.as_ref() {
        match store.get_feature_service(name) {
            Ok(service) => Some(service),
            Err(err) => {
                return json_error(&format!("feature service error: {err}"), StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    } else {
        None
    };

    if feature_service.is_none() && request.features.is_empty() {
        return json_error("either feature_service or features is required", StatusCode::BAD_REQUEST);
    }

    let join_key_values = match json_map_to_proto(&request.entities) {
        Ok(values) => values,
        Err(err) => {
            return json_error(&format!("invalid entities: {err}"), StatusCode::BAD_REQUEST);
        }
    };
    let request_context = match json_map_to_proto(&request.request_context) {
        Ok(values) => values,
        Err(err) => {
            return json_error(&format!("invalid request_context: {err}"), StatusCode::BAD_REQUEST);
        }
    };

    let vectors = match store
        .get_online_features(
            request.features,
            feature_service,
            join_key_values,
            request_context,
            request.full_feature_names,
        )
        .await
    {
        Ok(values) => values,
        Err(err) => {
            return json_error(&format!("error getting online features: {err}"), StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    let mut feature_names = Vec::new();
    let mut results = Vec::new();

    for vector in vectors {
        feature_names.push(vector.name.clone());
        let mut result = serde_json::Map::new();
        let values = vector
            .values
            .iter()
            .map(value_to_json)
            .collect::<Vec<_>>();
        result.insert("values".to_string(), JsonValue::Array(values));

        if status_flag {
            let statuses = vector
                .statuses
                .iter()
                .map(|status| field_status_to_string(*status))
                .map(JsonValue::String)
                .collect::<Vec<_>>();
            let timestamps = vector
                .timestamps
                .iter()
                .map(timestamp_to_rfc3339)
                .map(JsonValue::String)
                .collect::<Vec<_>>();
            result.insert("statuses".to_string(), JsonValue::Array(statuses));
            result.insert(
                "event_timestamps".to_string(),
                JsonValue::Array(timestamps),
            );
        }

        results.push(JsonValue::Object(result));
    }

    let response = json!({
        "metadata": {"feature_names": feature_names},
        "results": results,
    });

    (StatusCode::OK, Json(response))
}

fn json_error(message: &str, status: StatusCode) -> (StatusCode, Json<JsonValue>) {
    let response = json!({
        "error": message,
        "status_code": status.as_u16(),
    });
    (status, Json(response))
}

fn json_map_to_proto(
    map: &HashMap<String, Vec<JsonValue>>,
) -> Result<HashMap<String, Vec<types::Value>>> {
    let mut result = HashMap::new();
    for (key, values) in map {
        result.insert(key.clone(), json_values_to_proto(values)?);
    }
    Ok(result)
}

fn json_values_to_proto(values: &[JsonValue]) -> Result<Vec<types::Value>> {
    let mut result = Vec::with_capacity(values.len());
    for value in values {
        result.push(json_to_value(value)?);
    }
    Ok(result)
}

fn json_to_value(value: &JsonValue) -> Result<types::Value> {
    let val = match value {
        JsonValue::Null => None,
        JsonValue::Bool(value) => Some(types::value::Val::BoolVal(*value)),
        JsonValue::Number(number) => {
            if let Some(value) = number.as_i64() {
                Some(types::value::Val::Int64Val(value))
            } else if let Some(value) = number.as_u64() {
                Some(types::value::Val::Int64Val(value as i64))
            } else {
                Some(types::value::Val::DoubleVal(number.as_f64().unwrap_or(0.0)))
            }
        }
        JsonValue::String(value) => Some(types::value::Val::StringVal(value.clone())),
        JsonValue::Array(values) => Some(json_array_to_list_value(values)?),
        JsonValue::Object(_) => {
            anyhow::bail!("object values are not supported in entities request")
        }
    };

    Ok(types::Value { val })
}

fn json_array_to_list_value(values: &[JsonValue]) -> Result<types::value::Val> {
    enum ListType {
        Bool,
        Int64,
        Double,
        String,
    }

    let mut list_type: Option<ListType> = None;
    for value in values {
        match value {
            JsonValue::Null => continue,
            JsonValue::Bool(_) => {
                list_type = Some(match list_type {
                    None => ListType::Bool,
                    Some(ListType::Bool) => ListType::Bool,
                    _ => anyhow::bail!("mixed list types are not supported"),
                });
            }
            JsonValue::Number(number) => {
                let is_double = number
                    .as_f64()
                    .map(|v| v.fract() != 0.0)
                    .unwrap_or(false);
                list_type = Some(match list_type {
                    None => {
                        if is_double {
                            ListType::Double
                        } else {
                            ListType::Int64
                        }
                    }
                    Some(ListType::Int64) => {
                        if is_double {
                            ListType::Double
                        } else {
                            ListType::Int64
                        }
                    }
                    Some(ListType::Double) => ListType::Double,
                    _ => anyhow::bail!("mixed list types are not supported"),
                });
            }
            JsonValue::String(_) => {
                list_type = Some(match list_type {
                    None => ListType::String,
                    Some(ListType::String) => ListType::String,
                    _ => anyhow::bail!("mixed list types are not supported"),
                });
            }
            JsonValue::Array(_) => anyhow::bail!("nested list values are not supported"),
            JsonValue::Object(_) => anyhow::bail!("object values are not supported in lists"),
        }
    }

    match list_type {
        Some(ListType::Bool) => {
            let list = values
                .iter()
                .map(|value| value.as_bool().unwrap_or(false))
                .collect::<Vec<_>>();
            Ok(types::value::Val::BoolListVal(types::BoolList { val: list }))
        }
        Some(ListType::Double) => {
            let list = values
                .iter()
                .map(|value| value.as_f64().unwrap_or(0.0))
                .collect::<Vec<_>>();
            Ok(types::value::Val::DoubleListVal(types::DoubleList { val: list }))
        }
        Some(ListType::Int64) => {
            let list = values
                .iter()
                .map(|value| value.as_i64().unwrap_or(0))
                .collect::<Vec<_>>();
            Ok(types::value::Val::Int64ListVal(types::Int64List { val: list }))
        }
        Some(ListType::String) => {
            let list = values
                .iter()
                .map(|value| value.as_str().unwrap_or("").to_string())
                .collect::<Vec<_>>();
            Ok(types::value::Val::StringListVal(types::StringList { val: list }))
        }
        None => anyhow::bail!("empty list values are not supported"),
    }
}

fn value_to_json(value: &types::Value) -> JsonValue {
    match value.val.as_ref() {
        None => JsonValue::Null,
        Some(types::value::Val::BytesVal(bytes)) => {
            JsonValue::String(base64::engine::general_purpose::STANDARD.encode(bytes))
        }
        Some(types::value::Val::StringVal(value)) => JsonValue::String(value.clone()),
        Some(types::value::Val::Int32Val(value)) => json!(*value),
        Some(types::value::Val::Int64Val(value)) => json!(*value),
        Some(types::value::Val::DoubleVal(value)) => json!(*value),
        Some(types::value::Val::FloatVal(value)) => json!(*value),
        Some(types::value::Val::BoolVal(value)) => json!(*value),
        Some(types::value::Val::BytesListVal(list)) => JsonValue::Array(
            list.val
                .iter()
                .map(|bytes| JsonValue::String(base64::engine::general_purpose::STANDARD.encode(bytes)))
                .collect(),
        ),
        Some(types::value::Val::StringListVal(list)) => json!(list.val),
        Some(types::value::Val::Int32ListVal(list)) => json!(list.val),
        Some(types::value::Val::Int64ListVal(list)) => json!(list.val),
        Some(types::value::Val::DoubleListVal(list)) => json!(list.val),
        Some(types::value::Val::FloatListVal(list)) => json!(list.val),
        Some(types::value::Val::BoolListVal(list)) => json!(list.val),
        Some(types::value::Val::UnixTimestampVal(value)) => json!(*value),
        Some(types::value::Val::UnixTimestampListVal(list)) => json!(list.val),
        Some(types::value::Val::NullVal(_)) => JsonValue::Null,
        Some(types::value::Val::MapVal(map)) => {
            let mut obj = serde_json::Map::new();
            for (key, value) in &map.val {
                obj.insert(key.clone(), value_to_json(value));
            }
            JsonValue::Object(obj)
        }
        Some(types::value::Val::MapListVal(list)) => {
            let values = list.val.iter().map(map_to_json).collect::<Vec<_>>();
            JsonValue::Array(values)
        }
    }
}

fn map_to_json(map: &types::Map) -> JsonValue {
    let mut obj = serde_json::Map::new();
    for (key, value) in &map.val {
        obj.insert(key.clone(), value_to_json(value));
    }
    JsonValue::Object(obj)
}

fn field_status_to_string(status: serving::FieldStatus) -> String {
    status.as_str_name().to_string()
}

fn timestamp_to_rfc3339(timestamp: &prost_types::Timestamp) -> String {
    let nanos = timestamp.nanos.max(0) as u32;
    let seconds = timestamp.seconds;
    DateTime::<Utc>::from_timestamp(seconds, nanos)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_default()
}

fn bind_addr(host: &str, port: u16) -> Result<SocketAddr> {
    let bind_host = if host.is_empty() { "0.0.0.0" } else { host };
    let addr: SocketAddr = format!("{bind_host}:{port}").parse()?;
    Ok(addr)
}

struct GrpcServingService {
    store: Arc<Mutex<FeatureStore>>,
}

impl GrpcServingService {
    fn new(store: Arc<Mutex<FeatureStore>>) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl ServingService for GrpcServingService {
    async fn get_feast_serving_info(
        &self,
        _request: Request<GetFeastServingInfoRequest>,
    ) -> Result<Response<GetFeastServingInfoResponse>, Status> {
        Ok(Response::new(GetFeastServingInfoResponse {
            version: FEAST_SERVER_VERSION.to_string(),
        }))
    }

    async fn get_online_features(
        &self,
        request: Request<GetOnlineFeaturesRequest>,
    ) -> Result<Response<GetOnlineFeaturesResponse>, Status> {
        let request = request.into_inner();
        let kind = request
            .kind
            .ok_or_else(|| Status::invalid_argument("missing feature service or feature list"))?;

        let mut store = self.store.lock().await;
        let features = store.parse_features(&kind).map_err(to_status)?;
        let entities = request
            .entities
            .into_iter()
            .map(|(key, value)| (key, value.val))
            .collect::<HashMap<_, _>>();
        let request_context = request
            .request_context
            .into_iter()
            .map(|(key, value)| (key, value.val))
            .collect::<HashMap<_, _>>();

        let vectors = store
            .get_online_features(
                features.feature_refs,
                features.feature_service,
                entities,
                request_context,
                request.full_feature_names,
            )
            .await
            .map_err(to_status)?;

        let mut feature_names = Vec::new();
        let mut results = Vec::new();
        for vector in vectors {
            feature_names.push(vector.name.clone());
            results.push(serving::get_online_features_response::FeatureVector {
                values: vector.values,
                statuses: vector
                    .statuses
                    .into_iter()
                    .map(|status| status as i32)
                    .collect(),
                event_timestamps: vector.timestamps,
            });
        }

        Ok(Response::new(GetOnlineFeaturesResponse {
            metadata: Some(serving::GetOnlineFeaturesResponseMetadata {
                feature_names: Some(serving::FeatureList { val: feature_names }),
            }),
            results,
            status: false,
        }))
    }
}

fn to_status(err: anyhow::Error) -> Status {
    Status::internal(err.to_string())
}
