use anyhow::{Context, Result};
use arrow::array::{Int64Array, Int64Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::{reader::FileReader, writer::FileWriter};
use arrow::record_batch::RecordBatch;
use feast_rust::config::RepoConfig;
use feast_rust::model::{BaseFeatureView, OnDemandFeatureView};
use feast_rust::onlineserving::FeatureVector;
use feast_rust::proto::feast::core;
use feast_rust::proto::feast::serving;
use feast_rust::proto::feast::types;
use feast_rust::transformation::{self, GrpcTransformationService};
use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};
use tonic::transport::Server;

#[derive(Default)]
struct TestTransformationService;

#[tonic::async_trait]
impl serving::transformation_service_server::TransformationService for TestTransformationService {
    async fn get_transformation_service_info(
        &self,
        _request: Request<serving::GetTransformationServiceInfoRequest>,
    ) -> Result<Response<serving::GetTransformationServiceInfoResponse>, Status> {
        Ok(Response::new(serving::GetTransformationServiceInfoResponse {
            version: "test".to_string(),
            r#type: serving::TransformationServiceType::Custom as i32,
            transformation_service_type_details: "test".to_string(),
        }))
    }

    async fn transform_features(
        &self,
        request: Request<serving::TransformFeaturesRequest>,
    ) -> Result<Response<serving::TransformFeaturesResponse>, Status> {
        let request = request.into_inner();
        if request.on_demand_feature_view_name != "odfv_view" {
            return Err(Status::invalid_argument("unexpected ODFV name"));
        }

        let input = request
            .transformation_input
            .and_then(|value| value.value)
            .and_then(|value| match value {
                serving::value_type::Value::ArrowValue(bytes) => Some(bytes),
            })
            .ok_or_else(|| Status::invalid_argument("missing input"))?;

        let batch = read_record_batch(&input)
            .map_err(|err| Status::internal(format!("input IPC error: {err}")))?;
        let rating = int64_column(&batch, "rating")
            .map_err(|err| Status::invalid_argument(format!("missing rating: {err}")))?;
        let request_feature = int64_column(&batch, "request_feature")
            .map_err(|err| Status::invalid_argument(format!("missing request_feature: {err}")))?;

        let mut output_builder = Int64Builder::new();
        for idx in 0..batch.num_rows() {
            output_builder.append_value(rating.value(idx) + request_feature.value(idx));
        }
        let output_array = Arc::new(output_builder.finish());

        let mut extra_builder = Int64Builder::new();
        for _ in 0..batch.num_rows() {
            extra_builder.append_value(1);
        }
        let extra_array = Arc::new(extra_builder.finish());

        let schema = Arc::new(Schema::new(vec![
            Field::new("odfv_feature", DataType::Int64, true),
            Field::new("extra", DataType::Int64, true),
        ]));
        let output_batch = RecordBatch::try_new(schema.clone(), vec![output_array, extra_array])
            .map_err(|err| Status::internal(format!("output batch error: {err}")))?;
        let bytes = write_record_batch(&schema, &output_batch)
            .map_err(|err| Status::internal(format!("output IPC error: {err}")))?;

        Ok(Response::new(serving::TransformFeaturesResponse {
            transformation_output: Some(serving::ValueType {
                value: Some(serving::value_type::Value::ArrowValue(bytes)),
            }),
        }))
    }
}

#[tokio::test]
async fn on_demand_transformation_roundtrip() -> Result<()> {
    if std::env::var("FEAST_TRANSFORM_TESTS").is_err() {
        eprintln!("set FEAST_TRANSFORM_TESTS=1 to enable transformation service tests");
        return Ok(());
    }

    let (endpoint, shutdown_tx, server_handle) = match start_test_server().await {
        Ok(result) => result,
        Err(err) => {
            eprintln!("skipping transformation test (server error: {err})");
            return Ok(());
        }
    };

    let config = repo_config("test_project", &endpoint);
    let mut service = GrpcTransformationService::from_config(&config)?
        .context("missing transformation service")?;

    let odfv = build_odfv();
    let request_data = HashMap::from([(
        "request_feature".to_string(),
        vec![
            types::Value {
                val: Some(types::value::Val::Int64Val(10)),
            },
            types::Value {
                val: Some(types::value::Val::Int64Val(20)),
            },
        ],
    )]);
    let entity_rows = HashMap::from([(
        "driver_id".to_string(),
        vec![
            types::Value {
                val: Some(types::value::Val::Int64Val(1001)),
            },
            types::Value {
                val: Some(types::value::Val::Int64Val(1002)),
            },
        ],
    )]);

    let features = vec![FeatureVector {
        name: "rating".to_string(),
        values: vec![
            types::Value {
                val: Some(types::value::Val::Int64Val(1)),
            },
            types::Value {
                val: Some(types::value::Val::Int64Val(2)),
            },
        ],
        statuses: vec![serving::FieldStatus::Present; 2],
        timestamps: vec![
            prost_types::Timestamp {
                seconds: 0,
                nanos: 0,
            };
            2
        ],
    }];

    let mut vectors = transformation::augment_response_with_on_demand_transforms(
        &mut service,
        &[odfv],
        &request_data,
        &entity_rows,
        &features,
        2,
        false,
    )
    .await?;

    let _ = shutdown_tx.send(());
    let _ = server_handle.await;

    assert_eq!(vectors.len(), 1);
    let vector = vectors.pop().expect("odfv vector");
    assert_eq!(vector.name, "odfv_feature");
    assert_eq!(vector.values.len(), 2);
    assert_eq!(
        vector.values[0].val,
        Some(types::value::Val::Int64Val(11))
    );
    assert_eq!(
        vector.values[1].val,
        Some(types::value::Val::Int64Val(22))
    );

    Ok(())
}

async fn start_test_server() -> Result<(String, oneshot::Sender<()>, tokio::task::JoinHandle<Result<(), tonic::transport::Error>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let endpoint = format!("http://{addr}");
    let incoming = TcpListenerStream::new(listener);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(
                serving::transformation_service_server::TransformationServiceServer::new(
                    TestTransformationService::default(),
                ),
            )
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    Ok((endpoint, shutdown_tx, handle))
}

fn repo_config(project: &str, endpoint: &str) -> RepoConfig {
    let mut feature_server = HashMap::new();
    feature_server.insert(
        "transformation_service_endpoint".to_string(),
        serde_yaml::Value::String(endpoint.to_string()),
    );

    RepoConfig {
        project: project.to_string(),
        provider: String::new(),
        registry: None,
        online_store: HashMap::new(),
        offline_store: HashMap::new(),
        feature_server,
        flags: HashMap::new(),
        entity_key_serialization_version: 3,
        repo_path: PathBuf::new(),
    }
}

fn build_odfv() -> OnDemandFeatureView {
    let output_feature = core::FeatureSpecV2 {
        name: "odfv_feature".to_string(),
        value_type: types::value_type::Enum::Int64 as i32,
        ..Default::default()
    };
    let base = BaseFeatureView::new("odfv_view".to_string(), &[output_feature]);
    let mut view = OnDemandFeatureView {
        base,
        source_feature_view_projections: HashMap::new(),
        source_request_data_sources: HashMap::new(),
    };
    view.source_request_data_sources.insert(
        "request".to_string(),
        vec![core::FeatureSpecV2 {
            name: "request_feature".to_string(),
            value_type: types::value_type::Enum::Int64 as i32,
            ..Default::default()
        }],
    );
    view
}

fn read_record_batch(bytes: &[u8]) -> Result<RecordBatch> {
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    reader
        .next()
        .transpose()?
        .context("missing input batch")
}

fn write_record_batch(schema: &Schema, batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    let mut writer = FileWriter::try_new(&mut buffer, schema)?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(buffer)
}

fn int64_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int64Array> {
    let (idx, _field) = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .find(|(_, field)| field.name() == name)
        .context("missing column")?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .context("column is not int64")
}
