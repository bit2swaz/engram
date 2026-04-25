use std::env;
use std::sync::Arc;

use engram::assembler::ContextAssembler;
use engram::core::OpenAITokenCounter;
use engram::embedding::OpenAIEmbedder;
use engram::stores::{LanceDBStore, RedisCoreMemoryStore, RedisShortTermMemory};
use engram::server::{AppState, build_router};
use engram::worker::{default_channel_size, default_worker_count, embedding_job_channel, spawn_embedding_workers};

fn bind_address() -> String {
    env::var("ENGRAM_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".to_string())
}

fn redis_url() -> String {
    env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string())
}

fn lancedb_path() -> String {
    env::var("LANCEDB_PATH").unwrap_or_else(|_| "./data/lancedb".to_string())
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let redis_url = redis_url();
    let short_term_memory = Arc::new(
        RedisShortTermMemory::connect(&redis_url)
            .await
            .map_err(std::io::Error::other)?,
    );
    let vector_store = Arc::new(
        LanceDBStore::connect(lancedb_path())
            .await
            .map_err(std::io::Error::other)?,
    );
    let embedding_provider = Arc::new(OpenAIEmbedder::new().map_err(std::io::Error::other)?);
    let token_counter =
        Arc::new(OpenAITokenCounter::new().expect("cl100k_base tokenizer should initialize"));
    let core_memory_store = Arc::new(
        RedisCoreMemoryStore::connect(&redis_url)
            .await
            .map_err(std::io::Error::other)?,
    );
    let context_assembler = Arc::new(ContextAssembler::new(
        short_term_memory.clone(),
        vector_store.clone(),
        embedding_provider.clone(),
        token_counter.clone(),
        core_memory_store.clone(),
    ));
    let (embedding_job_sender, receiver) = embedding_job_channel(default_channel_size());
    let _worker_handles = spawn_embedding_workers(
        short_term_memory.clone(),
        vector_store.clone(),
        embedding_provider.clone(),
        receiver,
        default_worker_count(),
    );

    let state = Arc::new(AppState {
        short_term_memory,
        vector_store,
        embedding_provider,
        token_counter,
        core_memory_store,
        context_assembler,
        embedding_job_sender,
    });

    let router = build_router(state);
    let listener = tokio::net::TcpListener::bind(bind_address()).await?;

    axum::serve(listener, router).await
}
