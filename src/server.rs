use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::assembler::ContextAssembler;
use crate::core::{CoreMemoryStore, EmbeddingProvider, ShortTermMemory, TokenCounter, VectorStore};
use crate::models::Message;

pub struct AppState {
    pub short_term_memory: Arc<dyn ShortTermMemory>,
    pub vector_store: Arc<dyn VectorStore>,
    pub embedding_provider: Arc<dyn EmbeddingProvider>,
    pub token_counter: Arc<dyn TokenCounter>,
    pub core_memory_store: Arc<dyn CoreMemoryStore>,
    pub context_assembler: Arc<ContextAssembler>,
}

#[derive(Debug, Serialize)]
struct CreateSessionResponse {
    session_id: String,
}

#[derive(Debug, Deserialize)]
struct AddMessageRequest {
    id: Option<String>,
    role: String,
    content: String,
}

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/sessions", post(create_session))
        .route("/sessions/{session_id}/messages", post(add_message))
        .route("/health", get(health_check))
        .with_state(state)
}

async fn health_check() -> StatusCode {
    StatusCode::OK
}

async fn create_session() -> Json<CreateSessionResponse> {
    Json(CreateSessionResponse {
        session_id: Uuid::new_v4().to_string(),
    })
}

async fn add_message(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
    Json(payload): Json<AddMessageRequest>,
) -> Result<StatusCode, StatusCode> {
    let message = Message {
        id: Some(payload.id.unwrap_or_else(|| Uuid::new_v4().to_string())),
        role: payload.role,
        content: payload.content,
        timestamp: Some(Utc::now()),
        embedding_status: None,
    };

    state
        .short_term_memory
        .add_message(&session_id, message)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::http::StatusCode;
    use axum_test::TestServer;
    use serde::Deserialize;
    use serde_json::json;
    use uuid::Uuid;

    use super::{AppState, build_router};
    use crate::assembler::ContextAssembler;
    use crate::core::{
        DummyTokenCounter, InMemoryCoreMemoryStore, InMemoryStore, InMemoryVectorStore,
        RandomEmbeddingProvider,
    };

    #[derive(Debug, Deserialize)]
    struct CreateSessionResponse {
        session_id: String,
    }

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

    #[tokio::test]
    async fn create_session_returns_valid_uuid() {
        let server = TestServer::new(build_router(build_test_state())).unwrap();

        let response = server.post("/sessions").await;
        response.assert_status_ok();

        let body: CreateSessionResponse = response.json();
        assert!(Uuid::parse_str(&body.session_id).is_ok());
    }

    #[tokio::test]
    async fn add_message_stores_message_and_generates_uuid_when_missing() {
        let state = build_test_state();
        let server = TestServer::new(build_router(state.clone())).unwrap();
        let session_id = Uuid::new_v4().to_string();

        let response = server
            .post(&format!("/sessions/{session_id}/messages"))
            .json(&json!({
                "role": "user",
                "content": "Hello"
            }))
            .await;

        response.assert_status(StatusCode::NO_CONTENT);

        let messages = state
            .short_term_memory
            .get_recent(&session_id, 10)
            .await
            .unwrap();
        assert_eq!(messages.len(), 1);

        let stored = &messages[0];
        assert_eq!(stored.role, "user");
        assert_eq!(stored.content, "Hello");
        assert!(stored.timestamp.is_some());
        assert!(stored.embedding_status.is_none());
        assert!(Uuid::parse_str(stored.id.as_deref().unwrap()).is_ok());
    }

    #[tokio::test]
    async fn add_message_preserves_custom_id() {
        let state = build_test_state();
        let server = TestServer::new(build_router(state.clone())).unwrap();
        let session_id = Uuid::new_v4().to_string();
        let custom_id = Uuid::new_v4().to_string();

        let response = server
            .post(&format!("/sessions/{session_id}/messages"))
            .json(&json!({
                "id": custom_id,
                "role": "assistant",
                "content": "Hi there"
            }))
            .await;

        response.assert_status(StatusCode::NO_CONTENT);

        let messages = state
            .short_term_memory
            .get_recent(&session_id, 10)
            .await
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id.as_deref(), Some(custom_id.as_str()));
        assert_eq!(messages[0].role, "assistant");
        assert_eq!(messages[0].content, "Hi there");
    }

    #[tokio::test]
    async fn add_message_with_missing_required_fields_returns_unprocessable_entity() {
        let server = TestServer::new(build_router(build_test_state())).unwrap();
        let session_id = Uuid::new_v4().to_string();

        let response = server
            .post(&format!("/sessions/{session_id}/messages"))
            .json(&json!({
                "content": "Hello"
            }))
            .await;

        response.assert_status_unprocessable_entity();
    }
}
