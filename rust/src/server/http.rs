use crate::encoding;
use crate::featurestore::FeatureStore;
use axum::{
    extract::State,
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
use tokio::time::Duration;
use tracing::warn;

#[derive(Clone)]
struct AppState {
    store: Arc<Mutex<FeatureStore>>,
}

pub async fn start_http(
    store: FeatureStore,
    host: &str,
    port: u16,
    registry_ttl_sec: u64,
) -> anyhow::Result<()> {
    let addr = super::bind_addr(host, port)?;
    let state = AppState {
        store: Arc::new(Mutex::new(store)),
    };
    spawn_registry_refresher(state.clone(), registry_ttl_sec);
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

fn spawn_registry_refresher(state: AppState, registry_ttl_sec: u64) {
    if registry_ttl_sec == 0 {
        return;
    }

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(registry_ttl_sec));
        loop {
            ticker.tick().await;
            let mut store = state.store.lock().await;
            if let Err(err) = store.refresh_registry() {
                // Keep serving with last-good registry; just log.
                warn!(error = %err, "registry refresh failed");
            }
        }
    });
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
    Json(request): Json<HttpGetOnlineFeaturesRequest>,
) -> impl IntoResponse {
    let mut store = state.store.lock().await;
    let feature_service = if let Some(name) = request.feature_service.as_ref() {
        match store.get_feature_service(name) {
            Ok(service) => Some(service),
            Err(err) => {
                let msg = format!("feature service error: {err}");
                // Match Python SDK expectations: raise a typed FeastError for common not-found cases.
                if msg.contains("feature service not found") {
                    return feast_error(
                        "FeatureServiceNotFoundException",
                        &msg,
                        StatusCode::NOT_FOUND,
                    );
                }
                return feast_error("FeastError", &msg, StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    } else {
        None
    };

    if feature_service.is_none() && request.features.is_empty() {
        return feast_error(
            "FeastError",
            "either feature_service or features is required",
            StatusCode::BAD_REQUEST,
        );
    }

    let join_key_values = match encoding::json_map_to_proto(&request.entities) {
        Ok(values) => values,
        Err(err) => {
            return feast_error(
                "FeastError",
                &format!("invalid entities: {err}"),
                StatusCode::BAD_REQUEST,
            );
        }
    };
    let request_context = match encoding::json_map_to_proto(&request.request_context) {
        Ok(values) => values,
        Err(err) => {
            return feast_error(
                "FeastError",
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
            return feast_error(
                "FeastError",
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

        // Python Feast RemoteOnlineStore assumes these fields always exist.
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

        results.push(JsonValue::Object(result));
    }

    let response = json!({
        "metadata": {"feature_names": feature_names},
        "results": results,
    });

    (StatusCode::OK, Json(response))
}

fn feast_error(class: &str, message: &str, status: StatusCode) -> (StatusCode, Json<JsonValue>) {
    // Python Feast uses a JSON *string* that itself contains a JSON object:
    // FeastError.to_error_detail() -> json.dumps({module,class,message}), then JSONResponse(content=str).
    // The Python RemoteOnlineStore expects response.json() to return a string that can be json.loads()'d.
    let detail = json!({
        "module": "feast.errors",
        "class": class,
        "message": message,
    })
    .to_string();
    (status, Json(JsonValue::String(detail)))
}
