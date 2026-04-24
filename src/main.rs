use std::env;
use std::sync::Arc;

use engram::assembler::ContextAssembler;
use engram::core::{
    DummyTokenCounter, InMemoryCoreMemoryStore, InMemoryStore, InMemoryVectorStore,
    RandomEmbeddingProvider,
};
use engram::server::{AppState, build_router};

fn bind_address() -> String {
    env::var("ENGRAM_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".to_string())
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let short_term_memory = Arc::new(InMemoryStore::default());
    let vector_store = Arc::new(InMemoryVectorStore::default());
    let embedding_provider = Arc::new(RandomEmbeddingProvider);
    let token_counter = Arc::new(DummyTokenCounter);
    let core_memory_store = Arc::new(InMemoryCoreMemoryStore::default());
    let context_assembler = Arc::new(ContextAssembler::new(
        short_term_memory.clone(),
        vector_store.clone(),
        embedding_provider.clone(),
        token_counter.clone(),
        core_memory_store.clone(),
    ));

    let state = Arc::new(AppState {
        short_term_memory,
        vector_store,
        embedding_provider,
        token_counter,
        core_memory_store,
        context_assembler,
    });

    let router = build_router(state);
    let listener = tokio::net::TcpListener::bind(bind_address()).await?;

    axum::serve(listener, router).await
}
