use anyhow::{Context, Result};
use feast_rust::config;
use feast_rust::onlineserving::{now_timestamp, FeatureVector};
use feast_rust::proto::feast::serving;
use feast_rust::proto::feast::types;
use feast_rust::registry::Registry;
use feast_rust::transformation::{self, GrpcTransformationService};
use std::collections::HashMap;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

#[tokio::test]
async fn odfv_python_e2e() -> Result<()> {
    if std::env::var("FEAST_PY_TRANSFORM_TESTS").is_err() {
        eprintln!("set FEAST_PY_TRANSFORM_TESTS=1 to enable python ODFV E2E test");
        return Ok(());
    }

    let repo_path = resolve_repo_path()?;
    let registry_path = repo_path.join("data/registry.db");
    if !registry_path.exists() {
        anyhow::bail!(
            "registry.db missing at {} (run feast apply in the repo first)",
            registry_path.display()
        );
    }

    let port = find_free_port()?;
    let python = resolve_python();
    let mut server = PythonTransformServer::start(&python, &repo_path, port)?;
    server.wait_ready().await?;

    let mut repo_config = config::load_repo_config(&repo_path)?;
    repo_config.feature_server.insert(
        "transformation_service_endpoint".to_string(),
        serde_yaml::Value::String(format!("http://127.0.0.1:{port}")),
    );

    let registry_config = repo_config.registry_config()?;
    let mut registry = Registry::new(
        &registry_config,
        &repo_config.repo_path,
        repo_config.project.clone(),
    )?;
    registry.initialize()?;
    let odfv_name = std::env::var("FEAST_PY_ODFV_NAME")
        .unwrap_or_else(|_| "transformed_conv_rate".to_string());
    let odfv = registry
        .list_on_demand_feature_views()?
        .into_iter()
        .find(|view| view.base.name == odfv_name)
        .with_context(|| format!("ODFV {odfv_name} not found in registry"))?;

    let mut service = GrpcTransformationService::from_config(&repo_config)?
        .context("missing transformation service endpoint")?;

    let num_rows = 2;
    let feature_values = vec![float_val(0.25), float_val(0.5)];
    let features = vec![FeatureVector {
        name: "conv_rate".to_string(),
        values: feature_values,
        statuses: vec![serving::FieldStatus::Present; num_rows],
        timestamps: vec![now_timestamp(); num_rows],
    }];

    let request_data = HashMap::from([
        (
            "val_to_add".to_string(),
            vec![int64_val(10), int64_val(20)],
        ),
        (
            "val_to_add_2".to_string(),
            vec![int64_val(1), int64_val(2)],
        ),
    ]);
    let entity_rows = HashMap::from([(
        "driver_id".to_string(),
        vec![int64_val(1001), int64_val(1002)],
    )]);

    let vectors = transformation::augment_response_with_on_demand_transforms(
        &mut service,
        &[odfv],
        &request_data,
        &entity_rows,
        &features,
        num_rows,
        false,
    )
    .await?;

    if odfv_name == "transformed_conv_rate" {
        let mut outputs = HashMap::new();
        for vector in vectors {
            outputs.insert(vector.name.clone(), vector);
        }
        assert_close(
            value_as_f64(&outputs["conv_rate_plus_val1"].values[0])?,
            10.25,
        );
        assert_close(
            value_as_f64(&outputs["conv_rate_plus_val1"].values[1])?,
            20.5,
        );
        assert_close(
            value_as_f64(&outputs["conv_rate_plus_val2"].values[0])?,
            1.25,
        );
        assert_close(
            value_as_f64(&outputs["conv_rate_plus_val2"].values[1])?,
            2.5,
        );
    } else {
        assert!(!vectors.is_empty());
    }

    drop(server);
    Ok(())
}

fn resolve_repo_path() -> Result<PathBuf> {
    if let Ok(path) = std::env::var("FEAST_PY_TRANSFORM_REPO") {
        let repo = PathBuf::from(path);
        let repo = repo.canonicalize().unwrap_or(repo);
        return Ok(repo);
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo = manifest_dir.join("../../feast-compat-sample/feature_repo");
    if repo.exists() {
        let repo = repo.canonicalize().unwrap_or(repo);
        return Ok(repo);
    }

    anyhow::bail!(
        "set FEAST_PY_TRANSFORM_REPO to a feature repo with an ODFV registry"
    );
}

fn resolve_python() -> String {
    std::env::var("FEAST_PYTHON")
        .unwrap_or_else(|_| "python3".to_string())
}

fn find_free_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

fn float_val(value: f32) -> types::Value {
    types::Value {
        val: Some(types::value::Val::FloatVal(value)),
    }
}

fn int64_val(value: i64) -> types::Value {
    types::Value {
        val: Some(types::value::Val::Int64Val(value)),
    }
}

fn value_as_f64(value: &types::Value) -> Result<f64> {
    match value.val.as_ref() {
        Some(types::value::Val::FloatVal(val)) => Ok(f64::from(*val)),
        Some(types::value::Val::DoubleVal(val)) => Ok(*val),
        other => anyhow::bail!("unexpected value type: {other:?}"),
    }
}

fn assert_close(actual: f64, expected: f64) {
    let diff = (actual - expected).abs();
    assert!(
        diff <= 1e-6,
        "value mismatch: got {actual}, expected {expected}"
    );
}

struct PythonTransformServer {
    child: Child,
    port: u16,
}

impl PythonTransformServer {
    fn start(python: &str, repo_path: &Path, port: u16) -> Result<Self> {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let feast_root = manifest_dir
            .parent()
            .context("missing feast root")?
            .to_path_buf();
        let python_path = feast_root.join("sdk/python");
        let mut paths = vec![python_path];
        if let Ok(existing) = std::env::var("PYTHONPATH") {
            paths.extend(std::env::split_paths(&existing));
        }
        let joined = std::env::join_paths(paths)?;

        let script = format!(
            r#"
from feast import FeatureStore
fs = FeatureStore(repo_path=r"{repo}")
fs.serve_transformations(port={port})
"#,
            repo = repo_path.display(),
            port = port,
        );

        let child = Command::new(python)
            .arg("-c")
            .arg(script)
            .env("PYTHONPATH", joined)
            .env("KMP_USE_SHM", "0")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("failed to start python with {python}"))?;

        Ok(Self { child, port })
    }

    async fn wait_ready(&mut self) -> Result<()> {
        let start = Instant::now();
        let timeout = Duration::from_secs(10);
        loop {
            if start.elapsed() > timeout {
                anyhow::bail!("python transformation server did not start in time");
            }

            if let Ok(status) = self.child.try_wait() {
                if let Some(status) = status {
                    anyhow::bail!("python transformation server exited: {status}");
                }
            }

            if tokio::net::TcpStream::connect(("127.0.0.1", self.port))
                .await
                .is_ok()
            {
                return Ok(());
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

impl Drop for PythonTransformServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
