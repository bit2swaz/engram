pub mod app;
pub mod assembler;
pub mod cluster;
pub mod config;
pub mod core;
pub mod embedding;
pub mod knowledge;
pub mod logging;
pub mod metrics;
pub mod models;
pub mod server;
pub mod stores;
pub mod worker;
pub mod raft;

pub mod proto {
    pub mod raft {
        tonic::include_proto!("engram.raft");
    }
}
