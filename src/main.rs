use std::env;

use engram::app::build_real_app_state;
use engram::config::Config;
use engram::logging::init_tracing;
use engram::server::build_router;

fn bind_address() -> String {
    env::var("ENGRAM_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".to_string())
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    init_tracing();

    let config = Config::from_env().map_err(std::io::Error::other)?;
    let state = build_real_app_state(&config)
        .await
        .map_err(std::io::Error::other)?;

    if let Some(raft) = &state.raft {
        let raft_addr = config
            .raft_addr
            .as_ref()
            .expect("RAFT_ADDR must be set in cluster mode");
        let grpc_server = engram::raft::grpc_server::RaftGrpcServer { raft: raft.clone() };
        let svc = engram::proto::raft::raft_service_server::RaftServiceServer::new(grpc_server);
        let addr: std::net::SocketAddr = raft_addr
            .parse()
            .map_err(std::io::Error::other)?;
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve(addr)
                .await
                .expect("gRPC Raft server failed");
        });
        tracing::info!(addr = %raft_addr, "gRPC Raft server listening");
    }

    let router = build_router(state);
    let listener = tokio::net::TcpListener::bind(bind_address()).await?;

    axum::serve(listener, router).await
}
