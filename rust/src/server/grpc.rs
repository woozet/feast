use crate::featurestore::FeatureStore;
use crate::proto::feast::serving::serving_service_server::{
    ServingService, ServingServiceServer,
};
use crate::proto::feast::serving::{
    self, GetFeastServingInfoRequest, GetFeastServingInfoResponse, GetOnlineFeaturesRequest,
    GetOnlineFeaturesResponse,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::Duration;
use tonic::{Request, Response, Status};
use tracing::warn;

pub async fn start_grpc(
    store: FeatureStore,
    host: &str,
    port: u16,
    registry_ttl_sec: u64,
) -> anyhow::Result<()> {
    let addr = super::bind_addr(host, port)?;
    let store = Arc::new(store);
    spawn_registry_refresher(store.clone(), registry_ttl_sec);
    let service = GrpcServingService::new(store);

    tonic::transport::Server::builder()
        .add_service(ServingServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}

fn spawn_registry_refresher(store: Arc<FeatureStore>, registry_ttl_sec: u64) {
    if registry_ttl_sec == 0 {
        return;
    }

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(registry_ttl_sec));
        loop {
            ticker.tick().await;
            if let Err(err) = store.refresh_registry() {
                warn!(error = %err, "registry refresh failed");
            }
        }
    });
}

struct GrpcServingService {
    store: Arc<FeatureStore>,
}

impl GrpcServingService {
    fn new(store: Arc<FeatureStore>) -> Self {
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
            version: super::FEAST_SERVER_VERSION.to_string(),
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

        let features = self.store.parse_features(&kind).map_err(to_status)?;
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

        let vectors = self.store
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
