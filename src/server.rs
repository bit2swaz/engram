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
use crate::knowledge::global_handler::{
    get_global, get_global_conflicts, get_global_entity, get_global_entity_sources,
    get_global_export, get_global_path, set_visibility,
};
use crate::knowledge::handler::{export_knowledge, find_path, get_knowledge, get_related};
use axum_prometheus::{PrometheusMetricLayer, PrometheusMetricLayerBuilder};
use axum_prometheus::metrics_exporter_prometheus::PrometheusHandle;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;
use uuid::Uuid;

use crate::assembler::ContextAssembler;
use crate::core::{
    CoreMemoryStore, EmbeddingProvider, MemoryServerError, SearchResult, ShortTermMemory,
    TokenCounter, VectorStore,
};
use crate::metrics::{
    AppMetrics, DEFAULT_EMBEDDING_MODEL_LABEL, DEFAULT_VECTOR_STORE_LABEL,
};
use crate::models::{EmbeddingStatus, Message};
use crate::raft::types::{MemoryCommand, MessagePayload};
use crate::worker::EmbeddingJob;

impl axum::response::IntoResponse for MemoryServerError {
    fn into_response(self) -> axum::response::Response {
        match self {
            MemoryServerError::RedirectToLeader(location) => {
                axum::response::Redirect::temporary(&location).into_response()
            }
            MemoryServerError::NoLeader => {
                (StatusCode::SERVICE_UNAVAILABLE, "no leader elected").into_response()
            }
            MemoryServerError::QueueFull => {
                (StatusCode::SERVICE_UNAVAILABLE, "embedding queue unavailable").into_response()
            }
            MemoryServerError::BadRequest(msg) => {
                (StatusCode::BAD_REQUEST, msg).into_response()
            }
            MemoryServerError::Internal(msg)
            | MemoryServerError::Embed(crate::core::EmbedError::Message(msg))
            | MemoryServerError::Store(crate::core::StoreError::Message(msg))
            | MemoryServerError::Memory(crate::core::MemoryError::Message(msg)) => {
                (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response()
            }
            other => {
                (StatusCode::INTERNAL_SERVER_ERROR, other.to_string()).into_response()
            }
        }
    }
}

/// Converts a Raft client-write error into a `MemoryServerError`.
///
/// When the node is a follower, the leader's HTTP address is looked up in `peer_http_addrs`
/// and a `RedirectToLeader` error is returned so the handler can issue a 307.
fn forward_to_redirect(
    e: &openraft::error::RaftError<
        u64,
        openraft::error::ClientWriteError<u64, openraft::BasicNode>,
    >,
    peer_http_addrs: &std::collections::HashMap<u64, String>,
    path: &str,
) -> MemoryServerError {
    if let Some(fwd) = e.forward_to_leader::<openraft::BasicNode>() {
        if let Some(leader_id) = fwd.leader_id {
            if let Some(http_addr) = peer_http_addrs.get(&leader_id) {
                let location = format!("http://{}{}", http_addr, path);
                return MemoryServerError::RedirectToLeader(location);
            }
        }
        return MemoryServerError::NoLeader;
    }
    MemoryServerError::Internal(format!("raft error: {e}"))
}

/// Returns a `RedirectToLeader` error if this node is a follower.
///
/// Unlike [`forward_to_redirect`], this consults the leader directly from Raft metrics
/// rather than a `client_write` rejection for handlers that don't submit a command
/// synchronously but still must route mutations to the leader (e.g. manual consolidate,
/// which enqueues an async job the leader runs). Returns `None` when this node is the
/// leader (or standalone), so the caller proceeds locally.
pub(crate) fn redirect_if_follower(
    raft: &crate::raft::types::RaftHandle,
    node_id: u64,
    peer_http_addrs: &std::collections::HashMap<u64, String>,
    path: &str,
) -> Option<MemoryServerError> {
    let leader = raft.metrics().borrow().current_leader;
    if leader == Some(node_id) {
        return None;
    }
    Some(match leader.and_then(|id| peer_http_addrs.get(&id)) {
        Some(http_addr) => {
            MemoryServerError::RedirectToLeader(format!("http://{http_addr}{path}"))
        }
        None => MemoryServerError::NoLeader,
    })
}

/// Submits a command to Raft and maps the result to a handler-ready `StatusCode`.
///
/// Called only when `state.raft.is_some()`. Centralises the `client_write` call and
/// the follower-redirect error mapping so write handlers don't repeat the pattern.
async fn raft_write(
    raft: &crate::raft::types::RaftHandle,
    cmd: MemoryCommand,
    peer_http_addrs: &std::collections::HashMap<u64, String>,
    path: &str,
) -> Result<StatusCode, MemoryServerError> {
    raft.client_write(cmd)
        .await
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(|e| forward_to_redirect(&e, peer_http_addrs, path))
}

#[derive(Clone)]
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
    /// None in standalone mode (no NODE_ID env var). Some when running as a cluster node.
    pub raft: Option<Arc<crate::raft::types::RaftHandle>>,
    /// This node's Raft node ID. 0 in standalone mode.
    pub node_id: u64,
    /// HTTP addresses of peer nodes keyed by node ID, for follower redirects.
    pub peer_http_addrs: std::collections::HashMap<u64, String>,
    /// This node's gRPC listen address, e.g. "0.0.0.0:9001". Used by init_cluster.
    pub raft_addr: Option<String>,
    /// Routable gRPC address advertised to peers and stored in cluster membership.
    /// E.g. "node-1:9001". Falls back to raft_addr when not set.
    pub raft_advertise_addr: Option<String>,
    /// gRPC addresses of peer nodes, used to build the initial cluster membership.
    pub cluster_peers: Vec<crate::config::PeerConfig>,
    /// Per-session in-memory knowledge graph, shared with the Raft state machine.
    pub knowledge_graph: Arc<tokio::sync::RwLock<crate::knowledge::graph::KnowledgeGraph>>,
    /// Channel for sending knowledge extraction jobs to the worker pool.
    pub knowledge_job_sender: tokio::sync::mpsc::Sender<crate::knowledge::types::KnowledgeJob>,
    /// Cluster-wide knowledge graph aggregating all Shared sessions.
    pub global_graph: Arc<tokio::sync::RwLock<crate::knowledge::global::GlobalGraph>>,
    /// Per-session consolidated summaries, shared with the Raft state machine.
    pub consolidated: Arc<dyn crate::consolidation::store::ConsolidatedMemoryStore>,
    /// Channel for handing consolidation jobs to the scheduler worker pool.
    pub consolidation_tx: mpsc::Sender<crate::consolidation::scheduler::ConsolidationJob>,
}


#[derive(Debug, Serialize, utoipa::ToSchema)]
struct CreateSessionResponse {
    session_id: String,
}

#[derive(Debug, Deserialize, Default, utoipa::ToSchema)]
struct CreateSessionRequest {
    agent_id: Option<String>,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct AddMessageRequest {
    id: Option<String>,
    role: String,
    content: String,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
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

#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct SearchRequest {
    query: String,
    top_k: usize,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct CoreMemoryRequest {
    fact: String,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
struct SearchResponse {
    results: Vec<SearchResult>,
}

#[derive(OpenApi)]
#[openapi(
    paths(
        health_check,
        create_session,
        add_message,
        get_context,
        search_session,
        delete_session,
        put_core_memory,
    ),
    components(schemas(
        Message,
        EmbeddingStatus,
        AddMessageRequest,
        SearchRequest,
        CoreMemoryRequest,
        CreateSessionResponse,
        ContextResponse,
        SearchResponse,
        SearchResult,
    )),
    tags(
        (name = "sessions", description = "Session lifecycle and message ingestion"),
        (name = "memory", description = "Context assembly and memory operations"),
        (name = "health", description = "Service health checks"),
    )
)]
struct ApiDoc;

pub fn build_router(state: Arc<AppState>) -> Router {
    let (prometheus_layer, prometheus_handle) = http_metrics_layer();
    let metrics = state.metrics.clone();

    Router::new()
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .route("/sessions", post(create_session))
        .route("/sessions/{session_id}", delete(delete_session))
        .route("/sessions/{session_id}/messages", post(add_message))
        .route("/sessions/{session_id}/context", get(get_context))
        .route("/sessions/{session_id}/search", post(search_session))
        .route("/sessions/{session_id}/core-memory", put(put_core_memory))
        .route("/sessions/{session_id}/knowledge", get(get_knowledge))
        .route("/sessions/{session_id}/knowledge/entities/{entity_name}", get(get_related))
        .route("/sessions/{session_id}/knowledge/path", get(find_path))
        .route("/sessions/{session_id}/knowledge/export", get(export_knowledge))
        .route("/sessions/{session_id}/visibility", put(set_visibility))
        .route(
            "/sessions/{session_id}/summaries",
            get(crate::consolidation::handler::get_summaries),
        )
        .route(
            "/sessions/{session_id}/consolidate",
            post(crate::consolidation::handler::post_consolidate),
        )
        .route("/knowledge/global", get(get_global))
        .route("/knowledge/global/entities/{name}", get(get_global_entity))
        .route("/knowledge/global/entities/{name}/sources", get(get_global_entity_sources))
        .route("/knowledge/global/path", get(get_global_path))
        .route("/knowledge/global/export", get(get_global_export))
        .route("/knowledge/global/conflicts", get(get_global_conflicts))
        .route("/cluster", get(crate::cluster::get_cluster_status))
        .route("/cluster/init", post(crate::cluster::init_cluster))
        .route("/cluster/add-learner", post(crate::cluster::add_learner))
        .route(
            "/cluster/change-membership",
            post(crate::cluster::change_membership),
        )
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
#[utoipa::path(
    get,
    path = "/health",
    tag = "health",
    responses(
        (status = 200, description = "Service is healthy")
    )
)]
async fn health_check() -> StatusCode {
    tracing::info!("health check completed");
    StatusCode::OK
}

#[tracing::instrument(skip(state, body))]
#[utoipa::path(
    post,
    path = "/sessions",
    tag = "sessions",
    responses(
        (status = 200, description = "Session created successfully", body = CreateSessionResponse)
    )
)]
async fn create_session(
    State(state): State<Arc<AppState>>,
    body: Option<Json<CreateSessionRequest>>,
) -> Result<Json<CreateSessionResponse>, MemoryServerError> {
    let session_id = Uuid::new_v4().to_string();

    if let Some(agent_id) = body.and_then(|b| b.0.agent_id) {
        if let Some(raft) = &state.raft {
            raft_write(
                raft,
                MemoryCommand::RegisterSession {
                    session_id: session_id.clone(),
                    agent_id: Some(agent_id),
                },
                &state.peer_http_addrs,
                "/sessions",
            )
            .await?;
        }
    }

    tracing::info!(session_id = %session_id, "created session");
    Ok(Json(CreateSessionResponse { session_id }))
}

#[tracing::instrument(skip(state, payload), fields(session_id = %session_id))]
#[utoipa::path(
    post,
    path = "/sessions/{session_id}/messages",
    tag = "sessions",
    params(
        ("session_id" = String, Path, description = "Session identifier")
    ),
    request_body = AddMessageRequest,
    responses(
        (status = 204, description = "Message accepted for storage and embedding"),
        (status = 422, description = "Malformed message payload", body = String),
        (status = 500, description = "Failed to store the message", body = String),
        (status = 503, description = "Embedding queue unavailable", body = String)
    )
)]
async fn add_message(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
    Json(payload): Json<AddMessageRequest>,
) -> Result<StatusCode, MemoryServerError> {
    let message_id = payload.id.unwrap_or_else(|| Uuid::new_v4().to_string());
    let role = payload.role;
    let content = payload.content;

    // Cluster mode: replicate through Raft. The state machine applies the command to
    // Redis and enqueues the embedding job on every node independently.
    if let Some(raft) = &state.raft {
        let status = raft_write(
            raft,
            MemoryCommand::AddMessage {
                session_id: session_id.clone(),
                message: MessagePayload { id: message_id, role, content, timestamp: Utc::now() },
            },
            &state.peer_http_addrs,
            &format!("/sessions/{session_id}/messages"),
        )
        .await?;
        // Nudge the consolidation scheduler. The worker re-checks leadership, the count
        // threshold, and the in-flight guard, so a full queue or a follower is harmless.
        let _ = state
            .consolidation_tx
            .try_send(crate::consolidation::scheduler::ConsolidationJob {
                session_id: session_id.clone(),
            });
        return Ok(status);
    }

    // Standalone mode: write directly to Redis and enqueue embedding.
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
            MemoryServerError::from(error)
        })?;

    state.metrics.increment_messages_added(&role);

    state
        .short_term_memory
        .trim(&session_id, state.short_term_count)
        .await
        .map_err(|error| {
            state.metrics.increment_short_term_store_error("trim");
            tracing::error!(error = %error, "failed to trim short-term store");
            MemoryServerError::from(error)
        })?;

    let queue_result = state
        .embedding_job_sender
        .try_send(EmbeddingJob::new(&session_id, &message_id, content));
    state
        .metrics
        .set_embedding_queue_size(embedding_queue_size(&state.embedding_job_sender));

    queue_result.map_err(|error| {
        tracing::error!(error = %error, message_id = %message_id, "failed to queue embedding job");
        MemoryServerError::QueueFull
    })?;

    tracing::info!(message_id = %message_id, "queued embedding job");

    Ok(StatusCode::NO_CONTENT)
}

#[tracing::instrument(skip(state, query), fields(session_id = %session_id))]
#[utoipa::path(
    get,
    path = "/sessions/{session_id}/context",
    tag = "memory",
    params(
        ("session_id" = String, Path, description = "Session identifier"),
        ("max_tokens" = Option<usize>, Query, description = "Maximum tokens for assembled context"),
        ("similarity_threshold" = Option<f32>, Query, description = "Similarity threshold for long-term memories"),
        ("long_term_top_k" = Option<usize>, Query, description = "Maximum number of long-term memories to include")
    ),
    responses(
        (status = 200, description = "Context assembled successfully", body = ContextResponse),
        (status = 400, description = "Invalid context query parameters", body = String),
        (status = 404, description = "Session not found", body = String),
        (status = 500, description = "Failed to assemble context", body = String)
    )
)]
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
#[utoipa::path(
    post,
    path = "/sessions/{session_id}/search",
    tag = "memory",
    params(
        ("session_id" = String, Path, description = "Session identifier")
    ),
    request_body = SearchRequest,
    responses(
        (status = 200, description = "Search completed successfully", body = SearchResponse),
        (status = 400, description = "Invalid search request body", body = String),
        (status = 500, description = "Failed to search session memory", body = String)
    )
)]
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
        })?;
    drop(vector_search_timer);

    tracing::info!("completed session search");

    Ok(Json(SearchResponse { results }))
}

#[tracing::instrument(skip(state), fields(session_id = %session_id))]
#[utoipa::path(
    delete,
    path = "/sessions/{session_id}",
    tag = "sessions",
    params(
        ("session_id" = String, Path, description = "Session identifier")
    ),
    responses(
        (status = 204, description = "Session data deleted")
    )
)]
async fn delete_session(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
) -> Result<StatusCode, MemoryServerError> {
    if let Some(raft) = &state.raft {
        return raft_write(
            raft,
            MemoryCommand::DeleteSession { session_id: session_id.clone() },
            &state.peer_http_addrs,
            &format!("/sessions/{session_id}"),
        )
        .await;
    }

    // Standalone mode: delete directly; errors are logged but not surfaced.
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

    Ok(StatusCode::NO_CONTENT)
}

#[tracing::instrument(skip(state, payload), fields(session_id = %session_id))]
#[utoipa::path(
    put,
    path = "/sessions/{session_id}/core-memory",
    tag = "memory",
    params(
        ("session_id" = String, Path, description = "Session identifier")
    ),
    request_body = CoreMemoryRequest,
    responses(
        (status = 204, description = "Core memory fact stored"),
        (status = 400, description = "Invalid core memory request body", body = String),
        (status = 500, description = "Failed to store core memory fact", body = String)
    )
)]
async fn put_core_memory(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
    payload: Result<Json<CoreMemoryRequest>, JsonRejection>,
) -> Result<StatusCode, MemoryServerError> {
    let payload = parse_core_memory_request(payload)?;
    tracing::info!(fact_len = payload.fact.len(), "adding core memory fact");

    if let Some(raft) = &state.raft {
        return raft_write(
            raft,
            MemoryCommand::AddFact { session_id: session_id.clone(), fact: payload.fact },
            &state.peer_http_addrs,
            &format!("/sessions/{session_id}/core-memory"),
        )
        .await;
    }

    state
        .core_memory_store
        .add_fact(&session_id, &payload.fact)
        .await
        .map_err(|error| {
            tracing::error!(error = %error, "failed to add core memory fact");
            MemoryServerError::from(error)
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
) -> Result<CoreMemoryRequest, MemoryServerError> {
    let Json(payload) = payload.map_err(|rejection| {
        MemoryServerError::BadRequest(format!("invalid core memory request body: {rejection}"))
    })?;

    if payload.fact.trim().is_empty() {
        return Err(MemoryServerError::BadRequest("fact must not be empty".to_string()));
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

fn embedding_queue_size(sender: &mpsc::Sender<EmbeddingJob>) -> usize {
    sender.max_capacity().saturating_sub(sender.capacity())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::http::StatusCode;
    use axum_test::TestServer;
    use serde::Deserialize;
    use serde_json::{Value, json};
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
            raft: None,
            node_id: 0,
            peer_http_addrs: std::collections::HashMap::new(),
            raft_addr: None,
            raft_advertise_addr: None,
            cluster_peers: vec![],
            knowledge_graph: Arc::new(tokio::sync::RwLock::new(
                crate::knowledge::graph::KnowledgeGraph::new(),
            )),
            knowledge_job_sender: {
                let (tx, mut rx) = tokio::sync::mpsc::channel::<crate::knowledge::types::KnowledgeJob>(16);
                tokio::spawn(async move { while rx.recv().await.is_some() {} });
                tx
            },
            global_graph: Arc::new(tokio::sync::RwLock::new(
                crate::knowledge::global::GlobalGraph::new(),
            )),
            consolidated: Arc::new(crate::consolidation::store::InMemoryConsolidatedStore::default()),
            consolidation_tx: {
                let (tx, mut rx) =
                    tokio::sync::mpsc::channel::<crate::consolidation::scheduler::ConsolidationJob>(16);
                tokio::spawn(async move { while rx.recv().await.is_some() {} });
                tx
            },
        })
    }

    #[tokio::test]
    async fn appstate_has_raft_field() {
        let s = build_test_state();
        let _ = s.raft.is_none();
        let _ = s.node_id;
    }

    #[tokio::test]
    async fn appstate_has_knowledge_fields() {
        // If AppState is missing the new fields, this won't compile.
        let state = build_test_state();
        let _ = state.knowledge_graph.read().await;
        let _ = state.knowledge_job_sender.capacity();
    }

    #[tokio::test]
    async fn health_route_returns_ok() {
        let server = TestServer::new(build_router(build_test_state())).unwrap();

        server.get("/health").await.assert_status_ok();
    }

    #[tokio::test]
    async fn openapi_routes_expose_generated_spec_and_swagger_ui() {
        let server = TestServer::new(build_router(build_test_state())).unwrap();

        let spec_response = server.get("/api-docs/openapi.json").await;
        spec_response.assert_status_ok();

        let spec: Value = spec_response.json();
        assert_eq!(spec["openapi"], "3.1.0");
        assert!(spec["paths"].get("/health").is_some());
        assert!(spec["paths"].get("/sessions").is_some());
        assert!(spec["paths"].get("/sessions/{session_id}/messages").is_some());
        assert!(spec["paths"].get("/sessions/{session_id}/context").is_some());
        assert!(spec["paths"].get("/sessions/{session_id}/search").is_some());
        assert!(spec["paths"].get("/sessions/{session_id}/core-memory").is_some());

        server.get("/swagger-ui/").await.assert_status_ok();
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
    async fn put_visibility_route_exists_and_validates_body() {
        let state = build_test_state();
        let server = TestServer::new(build_router(state)).unwrap();
        let resp = server
            .put("/sessions/s1/visibility")
            .json(&json!({ "visibility": "Shared" }))
            .await;
        assert!(
            resp.status_code().is_success() || resp.status_code().as_u16() == 307,
            "expected 2xx or 307 but got {}",
            resp.status_code()
        );
    }

    #[tokio::test]
    async fn summaries_and_consolidate_routes_exist() {
        let state = build_test_state();
        {
            let s = crate::consolidation::store::Summary {
                id: "u1".into(),
                text: "kept".into(),
                created_at_index: 1,
                consumed_message_ids: vec!["m1".into()],
                consumed_count: 1,
                model: "mock".into(),
                prompt_version: "summarize_v1".into(),
            };
            state.consolidated.add_summary("s1", s).await.unwrap();
        }
        let server = TestServer::new(build_router(state)).unwrap();

        let got = server.get("/sessions/s1/summaries").await;
        got.assert_status_ok();
        assert!(got.text().contains("kept"));

        // standalone (no raft): leader path always accepts.
        let resp = server.post("/sessions/s1/consolidate").await;
        assert!(
            resp.status_code().is_success() || resp.status_code().as_u16() == 307,
            "expected 2xx or 307 but got {}",
            resp.status_code()
        );
    }

    #[tokio::test]
    async fn post_consolidate_drives_full_flow_to_summary_and_trim() {
        // Wire a real scheduler to the same stores the router serves, mirroring app.rs
        // standalone wiring, then drive the whole path over HTTP: POST /consolidate ->
        // job -> mock summarize -> trim -> GET /summaries shows the result.
        use crate::consolidation::scheduler::{
            consolidation_job_channel, spawn_consolidation_workers,
        };
        use crate::consolidation::store::{ConsolidatedMemoryStore, InMemoryConsolidatedStore};
        use crate::core::ShortTermMemory;
        use crate::knowledge::summarizer::{MockSummarizer, Summarizer};

        let short_term = Arc::new(InMemoryStore::default());
        for i in 0..6 {
            short_term
                .add_message(
                    "s1",
                    crate::models::Message {
                        id: Some(format!("m{i}")),
                        role: "user".into(),
                        content: format!("content {i}"),
                        timestamp: None,
                        embedding_status: None,
                    },
                )
                .await
                .unwrap();
        }
        let consolidated = Arc::new(InMemoryConsolidatedStore::default());
        let metrics = Arc::new(AppMetrics::new().unwrap());
        let (consolidation_tx, rx) = consolidation_job_channel(16);
        let summarizer: Arc<dyn Summarizer> = Arc::new(MockSummarizer);
        // threshold 4, window 2: with 6 messages, summarize oldest 4, keep newest 2.
        spawn_consolidation_workers(
            summarizer,
            None,
            0,
            short_term.clone(),
            consolidated.clone(),
            metrics.clone(),
            4,
            2,
            rx,
            1,
        );

        let base = build_test_state();
        let state = Arc::new(AppState {
            short_term_memory: short_term.clone(),
            consolidated: consolidated.clone(),
            consolidation_tx,
            ..(*base).clone()
        });
        let server = TestServer::new(build_router(state)).unwrap();

        server.post("/sessions/s1/consolidate").await.assert_status(StatusCode::ACCEPTED);

        // Worker runs async; poll until the summary lands.
        let mut summaries: Vec<crate::consolidation::store::Summary> = vec![];
        for _ in 0..40 {
            summaries = consolidated.get_summaries("s1").await.unwrap();
            if !summaries.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert_eq!(summaries.len(), 1, "consolidation produced a summary");
        assert_eq!(summaries[0].consumed_count, 4);

        let remaining = short_term.get_recent("s1", 10).await.unwrap();
        assert_eq!(remaining.len(), 2, "session trimmed back to the window");

        let got = server.get("/sessions/s1/summaries").await;
        got.assert_status_ok();
        assert!(got.text().contains(&summaries[0].id));
    }

    #[tokio::test]
    async fn knowledge_routes_are_registered() {
        let server = TestServer::new(build_router(build_test_state())).unwrap();
        server.get("/sessions/test-session/knowledge").await.assert_status_ok();
        server
            .get("/sessions/test-session/knowledge/export?format=json")
            .await
            .assert_status_ok();
    }

    #[tokio::test]
    async fn global_endpoints_return_aggregated_view() {
        use crate::knowledge::types::{Entity, Relationship};
        use std::collections::HashMap;

        let state = build_test_state();
        {
            let mut g = state.global_graph.write().await;
            g.merge(
                "s1",
                0,
                vec![
                    Entity { name: "Alice".into(), entity_type: "Person".into(), attributes: HashMap::new() },
                    Entity { name: "OpenAI".into(), entity_type: "Organization".into(), attributes: HashMap::new() },
                ],
                vec![Relationship { from: "Alice".into(), to: "OpenAI".into(), relationship_type: "works_at".into() }],
            );
        }
        let server = TestServer::new(build_router(state)).unwrap();

        let all = server.get("/knowledge/global").await;
        all.assert_status_ok();

        let neighbors = server.get("/knowledge/global/entities/OpenAI").await;
        neighbors.assert_status_ok();
        assert!(neighbors.text().contains("Alice"));

        let sources = server.get("/knowledge/global/entities/OpenAI/sources").await;
        assert!(sources.text().contains("s1"));

        let path = server.get("/knowledge/global/path?from=Alice&to=OpenAI").await;
        path.assert_status_ok();
        assert!(path.text().contains("works_at"));

        let dot = server.get("/knowledge/global/export?format=dot").await;
        assert!(dot.text().contains("digraph"));
    }

    #[tokio::test]
    async fn knowledge_metrics_appear_in_prometheus_scrape() {
        let state = build_test_state();
        // Observe each metric so the HistogramVec emits output (Vec types only appear
        // in Prometheus text format once at least one label set has been recorded).
        let timer = state.metrics.start_knowledge_extraction_timer();
        drop(timer);
        state.metrics.increment_knowledge_entities(1);
        state.metrics.increment_knowledge_relationships(1);
        state.metrics.set_knowledge_queue_size(0);
        let server = TestServer::new(build_router(state)).unwrap();
        let body = server.get("/metrics").await.text();
        assert!(
            body.contains("engram_knowledge_extraction_duration_seconds"),
            "missing knowledge_extraction_duration_seconds"
        );
        assert!(
            body.contains("engram_knowledge_entities_extracted_total"),
            "missing knowledge_entities_extracted_total"
        );
        assert!(
            body.contains("engram_knowledge_relationships_extracted_total"),
            "missing knowledge_relationships_extracted_total"
        );
        assert!(
            body.contains("engram_knowledge_queue_size"),
            "missing knowledge_queue_size"
        );
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
