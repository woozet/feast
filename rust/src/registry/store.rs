use crate::config::RegistryConfig;
use crate::proto::feast::core;
use anyhow::{Context, Result};
use prost::Message;
use std::path::{Path, PathBuf};

pub(super) enum RegistryStore {
    File(FileRegistryStore),
    S3(S3RegistryStore),
}

impl RegistryStore {
    pub(super) fn new(config: &RegistryConfig, repo_path: &Path) -> Result<Self> {
        let store_type = config.registry_store_type.as_deref().unwrap_or("");
        let path = config.path.as_str();
        let is_s3_path = path.starts_with("s3://");

        match store_type {
            "" | "file" if !is_s3_path => Ok(Self::File(FileRegistryStore::new(config, repo_path))),
            "" | "s3" | "S3RegistryStore"
                if is_s3_path || store_type == "s3" || store_type == "S3RegistryStore" =>
            {
                Ok(Self::S3(S3RegistryStore::new(config)?))
            }
            "file" => Ok(Self::File(FileRegistryStore::new(config, repo_path))),
            other => anyhow::bail!("registry_store_type {other} is not supported"),
        }
    }

    pub(super) fn get_registry_proto(&self) -> Result<core::Registry> {
        match self {
            Self::File(store) => store.get_registry_proto(),
            Self::S3(store) => store.get_registry_proto(),
        }
    }
}

pub(super) struct FileRegistryStore {
    file_path: PathBuf,
}

impl FileRegistryStore {
    fn new(config: &RegistryConfig, repo_path: &Path) -> Self {
        let registry_path = Path::new(&config.path);
        let file_path = if registry_path.is_absolute() {
            registry_path.to_path_buf()
        } else {
            repo_path.join(registry_path)
        };

        Self { file_path }
    }

    fn get_registry_proto(&self) -> Result<core::Registry> {
        let data = std::fs::read(&self.file_path)
            .with_context(|| format!("failed to read registry at {}", self.file_path.display()))?;
        let registry = core::Registry::decode(data.as_slice())?;
        Ok(registry)
    }
}

pub(super) struct S3RegistryStore {
    client: aws_sdk_s3::Client,
    bucket: String,
    key: String,
    runtime: tokio::runtime::Runtime,
}

impl S3RegistryStore {
    fn new(config: &RegistryConfig) -> Result<Self> {
        let (bucket, key) = parse_s3_path(&config.path)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let sdk_config = runtime.block_on(async { aws_config::load_from_env().await });
        let client = aws_sdk_s3::Client::new(&sdk_config);
        Ok(Self {
            client,
            bucket,
            key,
            runtime,
        })
    }

    fn get_registry_proto(&self) -> Result<core::Registry> {
        let data = self.runtime.block_on(async {
            let response = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(&self.key)
                .send()
                .await
                .context("failed to fetch registry from s3")?;
            let bytes = response
                .body
                .collect()
                .await
                .context("failed to read s3 response body")?
                .into_bytes();
            Ok::<_, anyhow::Error>(bytes)
        })?;
        let registry = core::Registry::decode(data.as_ref())?;
        Ok(registry)
    }
}

fn parse_s3_path(path: &str) -> Result<(String, String)> {
    let stripped = path
        .strip_prefix("s3://")
        .with_context(|| format!("invalid s3 path: {path}"))?;
    let mut parts = stripped.splitn(2, '/');
    let bucket = parts
        .next()
        .filter(|value| !value.is_empty())
        .context("missing s3 bucket")?;
    let key = parts
        .next()
        .filter(|value| !value.is_empty())
        .context("missing s3 key")?;
    Ok((bucket.to_string(), key.to_string()))
}
