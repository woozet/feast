pub mod config;
pub mod encoding;
pub mod featurestore;
pub mod model;
pub mod onlineserving;
pub mod onlinestore;
pub mod registry;
pub mod server;
pub mod transformation;

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/feast.rs"));
}
