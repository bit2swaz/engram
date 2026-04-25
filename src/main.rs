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

    let router = build_router(state);
    let listener = tokio::net::TcpListener::bind(bind_address()).await?;

    axum::serve(listener, router).await
}
