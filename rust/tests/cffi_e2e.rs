use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::process::Command;

#[test]
fn cffi_embedded_rust_smoke() -> Result<()> {
    if std::env::var("FEAST_CFFI_TESTS").is_err() {
        eprintln!("set FEAST_CFFI_TESTS=1 to enable CFFI smoke test");
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

    let python = resolve_python();
    let feast_root = resolve_feast_root()?;
    let python_path = feast_root.join("sdk/python");
    let lib_path = resolve_shared_lib(&feast_root)?;

    let script = format!(
        r#"
import importlib.abc
import sys

class BlockTorch(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname.startswith("torch"):
            raise ModuleNotFoundError("torch import blocked for CFFI test")
        return None

sys.meta_path.insert(0, BlockTorch())

from feast.embedded_rust.online_features_service import EmbeddedRustOnlineFeatureServer

server = EmbeddedRustOnlineFeatureServer(r"{repo}")
resp = server.get_online_features(
    [
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
    ],
    None,
    {{"driver_id": [1005]}},
    {{}},
    full_feature_names=False,
)
data = resp.to_dict()
expected = ["driver_hourly_stats:conv_rate", "driver_hourly_stats:acc_rate", "driver_hourly_stats:avg_daily_trips"]
for key in expected:
    if key not in data:
        raise SystemExit("missing feature in response: " + key)
    if len(data[key]) != 1:
        raise SystemExit("unexpected feature vector length")
print("ok")
"#,
        repo = repo_path.display(),
    );

    let mut env_paths = vec![python_path];
    if let Ok(existing) = std::env::var("PYTHONPATH") {
        env_paths.extend(std::env::split_paths(&existing));
    }
    let joined_paths = std::env::join_paths(env_paths)?;

    let output = Command::new(python)
        .arg("-c")
        .arg(script)
        .env("PYTHONPATH", joined_paths)
        .env("FEAST_RUST_LIB_PATH", lib_path)
        .env("KMP_USE_SHM", "0")
        .output()
        .with_context(|| "failed to launch python for CFFI test")?;

    if !output.status.success() {
        anyhow::bail!(
            "CFFI test failed: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    Ok(())
}

fn resolve_repo_path() -> Result<PathBuf> {
    if let Ok(path) = std::env::var("FEAST_CFFI_REPO") {
        let repo = PathBuf::from(path);
        return Ok(repo.canonicalize().unwrap_or(repo));
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo = manifest_dir.join("../../feast-compat-sample/feature_repo");
    if repo.exists() {
        return Ok(repo.canonicalize().unwrap_or(repo));
    }

    anyhow::bail!("set FEAST_CFFI_REPO to a feature repo path")
}

fn resolve_python() -> String {
    std::env::var("FEAST_PYTHON").unwrap_or_else(|_| "python3".to_string())
}

fn resolve_feast_root() -> Result<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .context("missing feast root")
        .map(|path| path.to_path_buf())
}

fn resolve_shared_lib(feast_root: &Path) -> Result<String> {
    if let Ok(path) = std::env::var("FEAST_RUST_LIB_PATH") {
        return Ok(path);
    }

    let base = feast_root.join("sdk/python/feast/embedded_rust/lib");
    let candidates = [
        base.join("libfeast_rust.dylib"),
        base.join("libfeast_rust.so"),
        base.join("feast_rust.dll"),
        feast_root.join("rust/target/release/libfeast_rust.dylib"),
        feast_root.join("rust/target/release/libfeast_rust.so"),
        feast_root.join("rust/target/release/feast_rust.dll"),
    ];

    for candidate in candidates {
        if candidate.exists() {
            return Ok(candidate.to_string_lossy().to_string());
        }
    }

    anyhow::bail!("shared library not found; set FEAST_RUST_LIB_PATH")
}
