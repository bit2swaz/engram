use std::sync::{Arc, OnceLock};

use axum::{
    Json, Router,
    extract::{
        Path, Query, State,
        rejection::{JsonRejection, QueryRejection},
    },
    http::{StatusCode, header},
    response::IntoResponse,
    routing::{delete, get, post, put},
};
use axum_prometheus::{PrometheusMetricLayer, PrometheusMetricLayerBuilder};
use axum_prometheus::metrics_exporter_prometheus::PrometheusHandle;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::assembler::ContextAssembler;
use crate::core::{CoreMemoryStore, EmbeddingProvider, ShortTermMemory, TokenCounter, VectorStore};
use crate::metrics::{
    AppMetrics, DEFAULT_EMBEDDING_MODEL_LABEL, DEFAULT_VECTOR_STORE_LABEL,
};
use crate::models::{EmbeddingStatus, Message};
use crate::worker::EmbeddingJob;

pub struct AppState {
    pub short_term_memory: Arc<dyn ShortTermMemory>,
    pub vector_store: Arc<dyn VectorStore>,
    pub embedding_provider: Arc<dyn EmbeddingProvider>,
    pub token_counter: Arc<dyn TokenCounter>,
    pub core_memory_store: Arc<dyn CoreMemoryStore>,
    pub context_assembler: Arc<ContextAssembler>,
    pub metrics: Arc<AppMetrics>,
    pub embedding_job_sender: mpsc::Sender<EmbeddingJob>,
    pub short_term_count: usize,
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

#[derive(Debug, Serialize)]
struct ContextResponse {
    context: String,
}

#[derive(Debug, Deserialize, Default)]
struct ContextQueryParams {
    max_tokens: Option<usize>,
    similarity_threshold: Option<f32>,
    long_term_top_k: Option<usize>,
}

#[derive(Debug)]
struct ResolvedContextQueryParams {
    max_tokens: usize,
    similarity_threshold: f32,
    long_term_top_k: usize,
}

#[derive(Debug, Deserialize)]
struct SearchRequest {
    query: String,
    top_k: usize,
}

#[derive(Debug, Deserialize)]
struct CoreMemoryRequest {
    fact: String,
}

#[derive(Debug, Serialize)]
struct SearchResultResponse {
    text: String,
    score: f32,
}

#[derive(Debug, Serialize)]
struct SearchResponse {
    results: Vec<SearchResultResponse>,
}

pub fn build_router(state: Arc<AppState>) -> Router {
    let (prometheus_layer, prometheus_handle) = http_metrics_layer();
    let metrics = state.metrics.clone();

    Router::new()
        .route("/sessions", post(create_session))
        .route("/sessions/{session_id}", delete(delete_session))
        .route("/sessions/{session_id}/messages", post(add_message))
        .route("/sessions/{session_id}/context", get(get_context))
        .route("/sessions/{session_id}/search", post(search_session))
        .route("/sessions/{session_id}/core-memory", put(put_core_memory))
        .route("/health", get(health_check))
        .route(
            "/metrics",
            get(move || metrics_endpoint(metrics.clone(), prometheus_handle.clone())),
        )
        .layer(prometheus_layer)
        .with_state(state)
}

fn http_metrics_layer() -> (PrometheusMetricLayer<'static>, Arc<PrometheusHandle>) {
    static HTTP_METRICS: OnceLock<(PrometheusMetricLayer<'static>, Arc<PrometheusHandle>)> =
        OnceLock::new();

    let (layer, handle) = HTTP_METRICS.get_or_init(|| {
        let (layer, handle) = PrometheusMetricLayerBuilder::new()
            .with_ignore_pattern("/metrics")
            .with_default_metrics()
            .build_pair();
        (layer, Arc::new(handle))
    });

    (layer.clone(), handle.clone())
}

async fn metrics_endpoint(
    metrics: Arc<AppMetrics>,
    prometheus_handle: Arc<PrometheusHandle>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let mut body = prometheus_handle.render();
    let custom_metrics = metrics.render().map_err(internal_server_error)?;

    if !custom_metrics.is_empty() {
        if !body.ends_with('\n') {
            body.push('\n');
        }
        body.push_str(&custom_metrics);
    }

    Ok((
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    ))
}

#[tracing::instrument]
async fn health_check() -> StatusCode {
    tracing::info!("health check completed");
    StatusCode::OK
}

#[tracing::instrument]
async fn create_session() -> Json<CreateSessionResponse> {
    let session_id = Uuid::new_v4().to_string();
    tracing::info!(session_id = %session_id, "created session");

    Json(CreateSessionResponse { session_id })
}

#[tracing::instrument(skip(state, payload), fields(session_id = %session_id))]
async fn add_message(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
    Json(payload): Json<AddMessageRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let message_id = payload.id.unwrap_or_else(|| Uuid::new_v4().to_string());
    let role = payload.role;
    let content = payload.content;
    let message = Message {
        id: Some(message_id.clone()),
        role: role.clone(),
        content: content.clone(),
        timestamp: Some(Utc::now()),
        embedding_status: Some(EmbeddingStatus::Pending),
    };
    tracing::info!(
        message_id = %message_id,
        role = %message.role,
        embedding_status = ?message.embedding_status,
        "storing message"
    );

    state
        .short_term_memory
        .add_message(&session_id, message)
        .await
        .map_err(|error| {
            state.metrics.increment_short_term_store_error("add_message");
            tracing::error!(error = %error, "failed to add message to short-term store");
            internal_server_error(error)
        })?;

    state.metrics.increment_messages_added(&role);

    state
        .short_term_memory
        .trim(&session_id, state.short_term_count)
        .await
        .map_err(|error| {
            state.metrics.increment_short_term_store_error("trim");
            tracing::error!(error = %error, "failed to trim short-term store");
            internal_server_error(error)
        })?;

    let queue_result = state
        .embedding_job_sender
        .try_send(EmbeddingJob::new(&session_id, &message_id, content));
    state
        .metrics
        .set_embedding_queue_size(embedding_queue_size(&state.embedding_job_sender));

    queue_result
        .map_err(|error| {
            tracing::error!(error = %error, message_id = %message_id, "failed to queue embedding job");
            service_unavailable(format!("embedding queue unavailable: {error}"))
        })?;

    tracing::info!(message_id = %message_id, "queued embedding job");

    Ok(StatusCode::NO_CONTENT)
}

#[tracing::instrument(skip(state, query), fields(session_id = %session_id))]
async fn get_context(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
    query: Result<Query<ContextQueryParams>, QueryRejection>,
) -> Result<Json<ContextResponse>, (StatusCode, String)> {
    let params = parse_context_query(query)?;
    state.metrics.increment_context_requests();
    tracing::info!(
        max_tokens = params.max_tokens,
        similarity_threshold = params.similarity_threshold,
        long_term_top_k = params.long_term_top_k,
        "assembling context"
    );

    if !session_exists(&state, &session_id).await? {
        tracing::info!("session not found for context request");
        return Err(not_found(format!("session not found: {session_id}")));
    }

    let context = state
        .context_assembler
        .assemble_context(
            &session_id,
            params.max_tokens,
            params.similarity_threshold,
            params.long_term_top_k,
        )
        .await
        .map_err(|error| {
            tracing::error!(error = %error, "failed to assemble context");
            internal_server_error(error)
        })?;

    tracing::info!(context_len = context.len(), "assembled context");

    Ok(Json(ContextResponse { context }))
}

#[tracing::instrument(skip(state, payload), fields(session_id = %session_id))]
async fn search_session(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
    payload: Result<Json<SearchRequest>, JsonRejection>,
) -> Result<Json<SearchResponse>, (StatusCode, String)> {
    let payload = parse_search_request(payload)?;
    let query = payload.query;
    tracing::info!(query_len = query.len(), top_k = payload.top_k, "searching session");

    let embedding_timer = state
        .metrics
        .start_embedding_timer(DEFAULT_EMBEDDING_MODEL_LABEL);
    let embeddings = state
        .embedding_provider
        .embed(std::slice::from_ref(&query))
        .await
        .map_err(|error| {
            tracing::error!(error = %error, "failed to embed search query");
            internal_server_error(error)
        })?;
    drop(embedding_timer);

    let query_embedding = embeddings
        .first()
        .ok_or_else(|| internal_server_error("embedding provider returned no embeddings"))?;

    let vector_search_timer = state
        .metrics
        .start_vector_search_timer(DEFAULT_VECTOR_STORE_LABEL);
    let results = state
        .vector_store
        .search(&session_id, query_embedding, payload.top_k)
        .await
        .map_err(|error| {
            tracing::error!(error = %error, "failed to search vector store");
            internal_server_error(error)
        })?
        .into_iter()
        .map(|result| SearchResultResponse {
            text: result.text,
            score: result.score,
        })
        .collect();
    drop(vector_search_timer);

    tracing::info!("completed session search");

    Ok(Json(SearchResponse { results }))
}

#[tracing::instrument(skip(state), fields(session_id = %session_id))]
async fn delete_session(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
) -> StatusCode {
    if let Err(error) = state.short_term_memory.delete_session(&session_id).await {
        tracing::error!(error = %error, "failed to delete short-term session data");
    }
    if let Err(error) = state.vector_store.delete_session(&session_id).await {
        tracing::error!(error = %error, "failed to delete vector-store session data");
    }
    if let Err(error) = state.core_memory_store.delete_session(&session_id).await {
        tracing::error!(error = %error, "failed to delete core-memory session data");
    }

    tracing::info!("deleted session data");

    StatusCode::NO_CONTENT
}

#[tracing::instrument(skip(state, payload), fields(session_id = %session_id))]
async fn put_core_memory(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
    payload: Result<Json<CoreMemoryRequest>, JsonRejection>,
) -> Result<StatusCode, (StatusCode, String)> {
    let payload = parse_core_memory_request(payload)?;
    tracing::info!(fact_len = payload.fact.len(), "adding core memory fact");

    state
        .core_memory_store
        .add_fact(&session_id, &payload.fact)
        .await
        .map_err(|error| {
            tracing::error!(error = %error, "failed to add core memory fact");
            internal_server_error(error)
        })?;

    tracing::info!("added core memory fact");

    Ok(StatusCode::NO_CONTENT)
}

fn parse_context_query(
    query: Result<Query<ContextQueryParams>, QueryRejection>,
) -> Result<ResolvedContextQueryParams, (StatusCode, String)> {
    let Query(query) = query.map_err(|rejection| {
        bad_request(format!("invalid context query parameters: {rejection}"))
    })?;

    Ok(ResolvedContextQueryParams {
        max_tokens: query.max_tokens.unwrap_or(8_000),
        similarity_threshold: query.similarity_threshold.unwrap_or(0.7),
        long_term_top_k: query.long_term_top_k.unwrap_or(10),
    })
}

fn parse_search_request(
    payload: Result<Json<SearchRequest>, JsonRejection>,
) -> Result<SearchRequest, (StatusCode, String)> {
    let Json(payload) = payload
        .map_err(|rejection| bad_request(format!("invalid search request body: {rejection}")))?;

    if payload.query.trim().is_empty() {
        return Err(bad_request("query must not be empty"));
    }

    if payload.top_k == 0 {
        return Err(bad_request("top_k must be greater than 0"));
    }

    Ok(payload)
}

fn parse_core_memory_request(
    payload: Result<Json<CoreMemoryRequest>, JsonRejection>,
) -> Result<CoreMemoryRequest, (StatusCode, String)> {
    let Json(payload) = payload.map_err(|rejection| {
        bad_request(format!("invalid core memory request body: {rejection}"))
    })?;

    if payload.fact.trim().is_empty() {
        return Err(bad_request("fact must not be empty"));
    }

    Ok(payload)
}

async fn session_exists(state: &AppState, session_id: &str) -> Result<bool, (StatusCode, String)> {
    let messages = state
        .short_term_memory
        .get_recent(session_id, 1)
        .await
        .map_err(|error| {
            state.metrics.increment_short_term_store_error("get_recent");
            internal_server_error(error)
        })?;
    let facts = state
        .core_memory_store
        .get_facts(session_id)
        .await
        .map_err(internal_server_error)?;

    Ok(!messages.is_empty() || !facts.is_empty())
}

fn bad_request(message: impl Into<String>) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, message.into())
}

fn not_found(message: impl Into<String>) -> (StatusCode, String) {
    (StatusCode::NOT_FOUND, message.into())
}

fn internal_server_error(error: impl std::fmt::Display) -> (StatusCode, String) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("internal server error: {error}"),
    )
}

fn service_unavailable(message: impl Into<String>) -> (StatusCode, String) {
    (StatusCode::SERVICE_UNAVAILABLE, message.into())
}

fn embedding_queue_size(sender: &mpsc::Sender<EmbeddingJob>) -> usize {
    sender.max_capacity().saturating_sub(sender.capacity())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::http::StatusCode;
    use axum_test::TestServer;
    use serde::Deserialize;
    use serde_json::json;
    use tracing_test::traced_test;
    use uuid::Uuid;

    use super::{AppState, build_router};
    use crate::assembler::ContextAssembler;
    use crate::core::{
        InMemoryCoreMemoryStore, InMemoryStore, InMemoryVectorStore, OpenAITokenCounter,
        RandomEmbeddingProvider,
    };
    use crate::metrics::AppMetrics;
    use crate::models::EmbeddingStatus;
    use crate::worker::{EmbeddingJob, embedding_job_channel};
    use tokio::sync::mpsc;

    #[derive(Debug, Deserialize)]
    struct CreateSessionResponse {
        session_id: String,
    }

    #[derive(Debug, Deserialize)]
    struct ContextResponse {
        context: String,
    }

    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    struct SearchResultResponse {
        text: String,
        score: f32,
    }

    #[derive(Debug, Deserialize)]
    struct SearchResponse {
        results: Vec<SearchResultResponse>,
    }

    fn build_test_state() -> Arc<AppState> {
        let (embedding_job_sender, mut receiver) = embedding_job_channel(16);
        tokio::spawn(async move { while receiver.recv().await.is_some() {} });

        build_test_state_with_sender(embedding_job_sender)
    }

    fn build_test_state_with_sender(
        embedding_job_sender: mpsc::Sender<EmbeddingJob>,
    ) -> Arc<AppState> {
        let short_term_memory = Arc::new(InMemoryStore::default());
        let vector_store = Arc::new(InMemoryVectorStore::default());
        let embedding_provider = Arc::new(RandomEmbeddingProvider);
        let token_counter = Arc::new(OpenAITokenCounter::new().unwrap());
        let core_memory_store = Arc::new(InMemoryCoreMemoryStore::default());
        let metrics = Arc::new(AppMetrics::new().unwrap());
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
            metrics,
            embedding_job_sender,
            short_term_count: 20,
        })
    }

    #[tokio::test]
    async fn health_route_returns_ok() {
        let server = TestServer::new(build_router(build_test_state())).unwrap();

        server.get("/health").await.assert_status_ok();
    }

    #[tokio::test]
    async fn metrics_route_exposes_http_and_custom_metrics() {
        let state = build_test_state();
        let server = TestServer::new(build_router(state.clone())).unwrap();

        let session = server.post("/sessions").await;
        session.assert_status_ok();
        let session_body: CreateSessionResponse = session.json();
        let session_id = session_body.session_id;

        server
            .post(&format!("/sessions/{session_id}/messages"))
            .json(&json!({
                "role": "user",
                "content": "Track this message"
            }))
            .await
            .assert_status(StatusCode::NO_CONTENT);

        server
            .post(&format!("/sessions/{session_id}/search"))
            .json(&json!({
                "query": "Track this search",
                "top_k": 3
            }))
            .await
            .assert_status_ok();

        state.metrics.observe_embedding_duration("test", 0.025);
        state.metrics.observe_vector_search_duration("in_memory", 0.010);
        state.metrics.set_embedding_queue_size(1);

        let response = server.get("/metrics").await;
        response.assert_status_ok();

        let body = response.text();
        assert!(body.contains("engram_memory_embedding_duration_seconds"));
        assert!(body.contains("engram_memory_vector_search_duration_seconds"));
        assert!(body.contains("engram_memory_embedding_queue_size"));
        assert!(body.contains("engram_memory_messages_added_total"));
        assert!(body.contains("axum_http_requests_total"));
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
        assert!(matches!(
            stored.embedding_status,
            Some(EmbeddingStatus::Pending)
        ));
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
        assert!(matches!(
            messages[0].embedding_status,
            Some(EmbeddingStatus::Pending)
        ));
    }

    #[tokio::test]
    async fn add_message_returns_service_unavailable_when_queue_is_full() {
        let (embedding_job_sender, receiver) = embedding_job_channel(1);
        embedding_job_sender
            .try_send(EmbeddingJob::new("prefill-session", "prefill-message", "prefill"))
            .unwrap();

        let _receiver = receiver;
        let server = TestServer::new(build_router(build_test_state_with_sender(
            embedding_job_sender,
        )))
        .unwrap();
        let session_id = Uuid::new_v4().to_string();

        let response = server
            .post(&format!("/sessions/{session_id}/messages"))
            .json(&json!({
                "role": "user",
                "content": "Hello"
            }))
            .await;

        response.assert_status(StatusCode::SERVICE_UNAVAILABLE);
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

    #[tokio::test]
    async fn context_route_returns_assembled_context_with_default_parameters() {
        let state = build_test_state();
        let server = TestServer::new(build_router(state.clone())).unwrap();

        let session = server.post("/sessions").await;
        session.assert_status_ok();
        let session_body: CreateSessionResponse = session.json();
        let session_id = session_body.session_id;

        state
            .core_memory_store
            .add_fact(&session_id, "User likes Rust")
            .await
            .unwrap();

        server
            .post(&format!("/sessions/{session_id}/messages"))
            .json(&json!({
                "role": "user",
                "content": "Tell me about Rust async"
            }))
            .await
            .assert_status(StatusCode::NO_CONTENT);

        server
            .post(&format!("/sessions/{session_id}/messages"))
            .json(&json!({
                "role": "assistant",
                "content": "Rust async uses futures and executors"
            }))
            .await
            .assert_status(StatusCode::NO_CONTENT);

        let response = server.get(&format!("/sessions/{session_id}/context")).await;

        response.assert_status_ok();

        let body: ContextResponse = response.json();
        assert!(body.context.contains("User likes Rust"));
        assert!(body.context.contains("Tell me about Rust async"));
        assert!(
            body.context
                .contains("Rust async uses futures and executors")
        );
    }

    #[tokio::test]
    async fn context_route_with_max_tokens_one_returns_non_empty_context() {
        let state = build_test_state();
        let server = TestServer::new(build_router(state.clone())).unwrap();
        let session_id = Uuid::new_v4().to_string();

        state
            .core_memory_store
            .add_fact(&session_id, "Pinned fact")
            .await
            .unwrap();

        server
            .post(&format!("/sessions/{session_id}/messages"))
            .json(&json!({
                "role": "user",
                "content": "Hello"
            }))
            .await
            .assert_status(StatusCode::NO_CONTENT);

        let response = server
            .get(&format!("/sessions/{session_id}/context?max_tokens=1"))
            .await;

        response.assert_status_ok();

        let body: ContextResponse = response.json();
        assert!(!body.context.is_empty());
    }

    #[tokio::test]
    async fn context_route_for_missing_session_returns_not_found() {
        let server = TestServer::new(build_router(build_test_state())).unwrap();
        let session_id = Uuid::new_v4().to_string();

        let response = server.get(&format!("/sessions/{session_id}/context")).await;

        response.assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn context_route_with_invalid_query_parameter_returns_bad_request() {
        let server = TestServer::new(build_router(build_test_state())).unwrap();
        let session_id = Uuid::new_v4().to_string();

        let response = server
            .get(&format!(
                "/sessions/{session_id}/context?max_tokens=invalid"
            ))
            .await;

        response.assert_status(StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn search_route_returns_empty_results_when_session_has_no_memories() {
        let server = TestServer::new(build_router(build_test_state())).unwrap();
        let session_id = Uuid::new_v4().to_string();

        let response = server
            .post(&format!("/sessions/{session_id}/search"))
            .json(&json!({
                "query": "rust async",
                "top_k": 5
            }))
            .await;

        response.assert_status_ok();

        let body: SearchResponse = response.json();
        assert!(body.results.is_empty());
    }

    #[tokio::test]
    async fn search_route_with_missing_query_returns_bad_request() {
        let server = TestServer::new(build_router(build_test_state())).unwrap();
        let session_id = Uuid::new_v4().to_string();

        let response = server
            .post(&format!("/sessions/{session_id}/search"))
            .json(&json!({
                "top_k": 5
            }))
            .await;

        response.assert_status(StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn search_route_with_zero_top_k_returns_bad_request() {
        let server = TestServer::new(build_router(build_test_state())).unwrap();
        let session_id = Uuid::new_v4().to_string();

        let response = server
            .post(&format!("/sessions/{session_id}/search"))
            .json(&json!({
                "query": "rust async",
                "top_k": 0
            }))
            .await;

        response.assert_status(StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn delete_session_removes_messages_and_core_memory_and_context_returns_not_found() {
        let server = TestServer::new(build_router(build_test_state())).unwrap();

        let session = server.post("/sessions").await;
        session.assert_status_ok();
        let session_body: CreateSessionResponse = session.json();
        let session_id = session_body.session_id;

        server
            .post(&format!("/sessions/{session_id}/messages"))
            .json(&json!({
                "role": "user",
                "content": "Remember this"
            }))
            .await
            .assert_status(StatusCode::NO_CONTENT);

        server
            .put(&format!("/sessions/{session_id}/core-memory"))
            .json(&json!({
                "fact": "User prefers dark mode"
            }))
            .await
            .assert_status(StatusCode::NO_CONTENT);

        server
            .delete(&format!("/sessions/{session_id}"))
            .await
            .assert_status(StatusCode::NO_CONTENT);

        server
            .get(&format!("/sessions/{session_id}/context"))
            .await
            .assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn delete_session_is_idempotent() {
        let server = TestServer::new(build_router(build_test_state())).unwrap();
        let session_id = Uuid::new_v4().to_string();

        server
            .delete(&format!("/sessions/{session_id}"))
            .await
            .assert_status(StatusCode::NO_CONTENT);

        server
            .delete(&format!("/sessions/{session_id}"))
            .await
            .assert_status(StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn put_core_memory_adds_fact_and_context_includes_it() {
        let server = TestServer::new(build_router(build_test_state())).unwrap();
        let session_id = Uuid::new_v4().to_string();

        server
            .put(&format!("/sessions/{session_id}/core-memory"))
            .json(&json!({
                "fact": "User prefers dark mode"
            }))
            .await
            .assert_status(StatusCode::NO_CONTENT);

        let response = server.get(&format!("/sessions/{session_id}/context")).await;
        response.assert_status_ok();

        let body: ContextResponse = response.json();
        assert!(body.context.contains("User prefers dark mode"));
    }

    #[tokio::test]
    async fn put_core_memory_with_empty_fact_returns_bad_request() {
        let server = TestServer::new(build_router(build_test_state())).unwrap();
        let session_id = Uuid::new_v4().to_string();

        let response = server
            .put(&format!("/sessions/{session_id}/core-memory"))
            .json(&json!({
                "fact": "   "
            }))
            .await;

        response.assert_status(StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn put_core_memory_without_fact_returns_bad_request() {
        let server = TestServer::new(build_router(build_test_state())).unwrap();
        let session_id = Uuid::new_v4().to_string();

        let response = server
            .put(&format!("/sessions/{session_id}/core-memory"))
            .json(&json!({}))
            .await;

        response.assert_status(StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    #[traced_test]
    async fn handler_spans_are_logged_without_content_fields() {
        let server = TestServer::new(build_router(build_test_state())).unwrap();
        let session_id = Uuid::new_v4().to_string();
        let secret_content = "sensitive-message-body";

        server.get("/health").await.assert_status_ok();

        server
            .post(&format!("/sessions/{session_id}/messages"))
            .json(&json!({
                "role": "user",
                "content": secret_content
            }))
            .await
            .assert_status(StatusCode::NO_CONTENT);

        assert!(logs_contain("health_check"));
        logs_assert(|lines: &[&str]| {
            if !lines
                .iter()
                .any(|line| line.contains("add_message") && line.contains(&session_id))
            {
                return Err("expected add_message span logs with matching session_id".to_string());
            }

            if lines
                .iter()
                .any(|line| line.contains("content=") || line.contains(secret_content))
            {
                return Err("expected logs to exclude content fields and values".to_string());
            }

            Ok(())
        });
    }
}
