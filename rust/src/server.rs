mod grpc;
mod http;

pub use grpc::start_grpc;
pub use http::start_http;

const FEAST_SERVER_VERSION: &str = "0.0.1";

fn bind_addr(host: &str, port: u16) -> anyhow::Result<std::net::SocketAddr> {
    let bind_host = if host.is_empty() { "0.0.0.0" } else { host };
    let addr: std::net::SocketAddr = format!("{bind_host}:{port}").parse()?;
    Ok(addr)
}
