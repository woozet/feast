use clap::Parser;
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

use feast_rust::config;
use feast_rust::featurestore::FeatureStore;
use feast_rust::server;

#[derive(Debug, Parser)]
#[command(name = "feast-rust")]
struct Args {
    #[arg(long = "type", default_value = "http")]
    server_type: String,
    #[arg(long, default_value = "")]
    host: String,
    #[arg(long, default_value_t = 8080)]
    port: u16,
    /// How often to refresh the registry out-of-band (seconds). Set to 0 to disable.
    #[arg(long = "registry-ttl-sec", default_value_t = 60)]
    registry_ttl_sec: u64,
    #[arg(long = "chdir", default_value = ".")]
    repo_path: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let repo_path = args
        .repo_path
        .canonicalize()
        .unwrap_or_else(|_| args.repo_path.clone());

    let config = config::load_repo_config(&repo_path)?;
    let store = FeatureStore::new(config)?;

    match args.server_type.to_lowercase().as_str() {
        "http" => server::start_http(store, &args.host, args.port, args.registry_ttl_sec).await?,
        "grpc" => server::start_grpc(store, &args.host, args.port, args.registry_ttl_sec).await?,
        other => anyhow::bail!("unknown server type: {other}"),
    }

    Ok(())
}
