use std::sync::Arc;

use axum::{Router, http::StatusCode, routing::get};

use crate::assembler::ContextAssembler;
use crate::core::{CoreMemoryStore, EmbeddingProvider, ShortTermMemory, TokenCounter, VectorStore};

pub struct AppState {
    pub short_term_memory: Arc<dyn ShortTermMemory>,
    pub vector_store: Arc<dyn VectorStore>,
    pub embedding_provider: Arc<dyn EmbeddingProvider>,
    pub token_counter: Arc<dyn TokenCounter>,
    pub core_memory_store: Arc<dyn CoreMemoryStore>,
    pub context_assembler: Arc<ContextAssembler>,
}

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health_check))
        .with_state(state)
}

async fn health_check() -> StatusCode {
    StatusCode::OK
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum_test::TestServer;

    use super::{AppState, build_router};
    use crate::assembler::ContextAssembler;
    use crate::core::{
        DummyTokenCounter, InMemoryCoreMemoryStore, InMemoryStore, InMemoryVectorStore,
        RandomEmbeddingProvider,
    };

    fn build_test_state() -> Arc<AppState> {
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

        Arc::new(AppState {
            short_term_memory,
            vector_store,
            embedding_provider,
            token_counter,
            core_memory_store,
            context_assembler,
        })
    }

    #[tokio::test]
    async fn health_route_returns_ok() {
        let server = TestServer::new(build_router(build_test_state())).unwrap();

        server.get("/health").await.assert_status_ok();
    }
}
