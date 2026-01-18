use crate::encoding;
use crate::featurestore::FeatureStore;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
struct AppState {
    store: Arc<Mutex<FeatureStore>>,
}

pub async fn start_http(store: FeatureStore, host: &str, port: u16) -> anyhow::Result<()> {
    let addr = super::bind_addr(host, port)?;
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
                return json_error(
                    &format!("feature service error: {err}"),
                    StatusCode::INTERNAL_SERVER_ERROR,
                );
            }
        }
    } else {
        None
    };

    if feature_service.is_none() && request.features.is_empty() {
        return json_error("either feature_service or features is required", StatusCode::BAD_REQUEST);
    }

    let join_key_values = match encoding::json_map_to_proto(&request.entities) {
        Ok(values) => values,
        Err(err) => {
            return json_error(&format!("invalid entities: {err}"), StatusCode::BAD_REQUEST);
        }
    };
    let request_context = match encoding::json_map_to_proto(&request.request_context) {
        Ok(values) => values,
        Err(err) => {
            return json_error(
                &format!("invalid request_context: {err}"),
                StatusCode::BAD_REQUEST,
            );
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
            return json_error(
                &format!("error getting online features: {err}"),
                StatusCode::INTERNAL_SERVER_ERROR,
            );
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
            .map(encoding::value_to_json)
            .collect::<Vec<_>>();
        result.insert("values".to_string(), JsonValue::Array(values));

        if status_flag {
            let statuses = vector
                .statuses
                .iter()
                .map(|status| encoding::field_status_to_string(*status))
                .map(JsonValue::String)
                .collect::<Vec<_>>();
            let timestamps = vector
                .timestamps
                .iter()
                .map(encoding::timestamp_to_rfc3339)
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
