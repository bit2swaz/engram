use axum::{extract::State, http::StatusCode, Json};
use openraft::BasicNode;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::raft::types::RaftHandle;
use crate::server::AppState;

#[derive(Serialize)]
pub struct ClusterStatus {
    node_id: u64,
    role: String,
    leader_id: Option<u64>,
    term: u64,
    last_applied_index: Option<u64>,
    members: Vec<MemberStatus>,
}

#[derive(Serialize)]
pub struct MemberStatus {
    id: u64,
    addr: String,
    last_log_index: Option<u64>,
}

fn require_raft(state: &AppState) -> Result<&Arc<RaftHandle>, (StatusCode, String)> {
    state.raft.as_ref().ok_or_else(|| (
        StatusCode::SERVICE_UNAVAILABLE,
        "cluster mode not enabled (set NODE_ID to enable)".to_string(),
    ))
}

pub async fn get_cluster_status(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ClusterStatus>, (StatusCode, String)> {
    let raft = require_raft(&state)?;
    let m = raft.metrics().borrow().clone();
    let members = m
        .membership_config
        .membership()
        .nodes()
        .map(|(id, node)| {
            let last_log_index = m
                .replication
                .as_ref()
                .and_then(|r| r.get(id))
                .and_then(|opt| opt.as_ref())
                .map(|log_id| log_id.index);
            MemberStatus {
                id: *id,
                addr: node.addr.clone(),
                last_log_index,
            }
        })
        .collect();
    Ok(Json(ClusterStatus {
        node_id: m.id,
        role: format!("{:?}", m.state),
        leader_id: m.current_leader,
        term: m.current_term,
        last_applied_index: m.last_applied.as_ref().map(|l| l.index),
        members,
    }))
}

// SECURITY NOTE (Stage 1 known gap): init_cluster, add_learner, and change_membership
// are unauthenticated. Before exposing these outside a trusted network, add an
// Authorization middleware (e.g. shared admin bearer token or mTLS).
// Tracked for Stage 2 alongside multi-tenant auth.

pub async fn init_cluster(
    State(state): State<Arc<AppState>>,
) -> Result<StatusCode, (StatusCode, String)> {
    let raft = require_raft(&state)?;
    let mut members = std::collections::BTreeMap::new();
    // Use raft_advertise_addr as the address stored in cluster membership so peers can
    // reach this node. raft_addr (e.g. "0.0.0.0:9001") is the bind address and must NOT
    // be used here. 0.0.0.0 routes back to the caller's own loopback in Docker networking.
    let local_raft_addr = state
        .raft_advertise_addr
        .as_deref()
        .or(state.raft_addr.as_deref())
        .unwrap_or("127.0.0.1:9001");
    members.insert(state.node_id, BasicNode::new(local_raft_addr));
    for peer in &state.cluster_peers {
        members.insert(peer.id, BasicNode::new(&peer.addr));
    }
    raft.initialize(members)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
pub struct AddLearnerRequest {
    pub node_id: u64,
    pub addr: String,
}

pub async fn add_learner(
    State(state): State<Arc<AppState>>,
    Json(body): Json<AddLearnerRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let raft = require_raft(&state)?;

    // Allowlist check: only permit addresses already declared in cluster_peers config.
    // This prevents SSRF via a client-supplied addr pointing at an arbitrary host.
    let known = state.cluster_peers.iter().any(|p| p.id == body.node_id && p.addr == body.addr);
    if !known {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("node {}@{} is not in the configured cluster peers", body.node_id, body.addr),
        ));
    }

    raft.add_learner(body.node_id, BasicNode::new(&body.addr), true)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
pub struct ChangeMembershipRequest {
    pub members: Vec<u64>,
}

pub async fn change_membership(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ChangeMembershipRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let raft = require_raft(&state)?;
    let members: std::collections::BTreeSet<u64> = body.members.into_iter().collect();
    raft.change_membership(members, true)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use axum_test::TestServer;
    use serde_json::Value;

    use crate::app::{build_raft_node, spawn_raft_metrics_watcher};
    use crate::assembler::ContextAssembler;
    use crate::config::Config;
    use crate::core::{
        CoreMemoryStore, EmbeddingProvider, InMemoryCoreMemoryStore, InMemoryStore,
        InMemoryVectorStore, OpenAITokenCounter, RandomEmbeddingProvider, ShortTermMemory,
        TokenCounter, VectorStore,
    };
    use crate::metrics::AppMetrics;
    use crate::server::{AppState, build_router};
    use crate::worker::{EmbeddingJob, embedding_job_channel};
    use tokio::sync::mpsc;

    struct TestComponents {
        short_term: Arc<dyn ShortTermMemory>,
        vector_store: Arc<dyn VectorStore>,
        core_memory: Arc<dyn CoreMemoryStore>,
        embedding_provider: Arc<dyn EmbeddingProvider>,
        token_counter: Arc<dyn TokenCounter>,
        metrics: Arc<AppMetrics>,
        context_assembler: Arc<ContextAssembler>,
        embedding_job_sender: mpsc::Sender<EmbeddingJob>,
    }

    fn build_test_components() -> TestComponents {
        let (embedding_job_sender, mut rx) = embedding_job_channel(16);
        tokio::spawn(async move {
            while rx.recv().await.is_some() {}
        });
        let short_term: Arc<dyn ShortTermMemory> = Arc::new(InMemoryStore::default());
        let vector_store: Arc<dyn VectorStore> = Arc::new(InMemoryVectorStore::default());
        let core_memory: Arc<dyn CoreMemoryStore> = Arc::new(InMemoryCoreMemoryStore::default());
        let embedding_provider: Arc<dyn EmbeddingProvider> = Arc::new(RandomEmbeddingProvider);
        let token_counter: Arc<dyn TokenCounter> = Arc::new(OpenAITokenCounter::new().unwrap());
        let metrics = Arc::new(AppMetrics::new().unwrap());
        let context_assembler = Arc::new(ContextAssembler::new(
            short_term.clone(),
            vector_store.clone(),
            embedding_provider.clone(),
            token_counter.clone(),
            core_memory.clone(),
        ));
        TestComponents {
            short_term,
            vector_store,
            core_memory,
            embedding_provider,
            token_counter,
            metrics,
            context_assembler,
            embedding_job_sender,
        }
    }

    async fn build_test_app_with_single_node_raft() -> (TestServer, tempfile::TempDir) {
        let c = build_test_components();
        let raft_dir = tempfile::tempdir().unwrap();
        let config = Config {
            node_id: Some(1),
            raft_db_path: raft_dir.path().join("engram.redb"),
            ..Config::default()
        };
        let knowledge_graph = Arc::new(tokio::sync::RwLock::new(crate::knowledge::graph::KnowledgeGraph::new()));
        let global_graph = Arc::new(tokio::sync::RwLock::new(crate::knowledge::global::GlobalGraph::new()));
        let (knowledge_tx, mut knowledge_rx) = tokio::sync::mpsc::channel::<crate::knowledge::types::KnowledgeJob>(500);
        tokio::spawn(async move { while knowledge_rx.recv().await.is_some() {} });
        let raft = build_raft_node(
            &config,
            c.short_term.clone(),
            c.core_memory.clone(),
            c.vector_store.clone(),
            c.embedding_job_sender.clone(),
            knowledge_graph.clone(),
            knowledge_tx.clone(),
            global_graph,
        )
        .await
        .unwrap();

        let mut members = BTreeMap::new();
        members.insert(1u64, openraft::BasicNode::new("127.0.0.1:0"));
        raft.initialize(members).await.unwrap();
        tokio::time::sleep(Duration::from_millis(600)).await;
        spawn_raft_metrics_watcher(raft.clone(), c.metrics.clone());

        let state = Arc::new(AppState {
            short_term_memory: c.short_term,
            vector_store: c.vector_store,
            embedding_provider: c.embedding_provider,
            token_counter: c.token_counter,
            core_memory_store: c.core_memory,
            context_assembler: c.context_assembler,
            metrics: c.metrics,
            embedding_job_sender: c.embedding_job_sender,
            short_term_count: 20,
            raft: Some(raft),
            node_id: 1,
            peer_http_addrs: std::collections::HashMap::new(),
            raft_addr: Some("127.0.0.1:0".to_string()),
            raft_advertise_addr: None,
            cluster_peers: vec![],
            knowledge_graph,
            knowledge_job_sender: knowledge_tx,
        });
        (TestServer::new(build_router(state)).unwrap(), raft_dir)
    }

    fn build_test_app_standalone() -> TestServer {
        let c = build_test_components();
        let (knowledge_job_sender, mut krx) = tokio::sync::mpsc::channel::<crate::knowledge::types::KnowledgeJob>(16);
        tokio::spawn(async move { while krx.recv().await.is_some() {} });
        let state = Arc::new(AppState {
            short_term_memory: c.short_term,
            vector_store: c.vector_store,
            embedding_provider: c.embedding_provider,
            token_counter: c.token_counter,
            core_memory_store: c.core_memory,
            context_assembler: c.context_assembler,
            metrics: c.metrics,
            embedding_job_sender: c.embedding_job_sender,
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
            knowledge_job_sender,
        });
        TestServer::new(build_router(state)).unwrap()
    }

    #[tokio::test]
    async fn cluster_endpoint_returns_200_with_node_info() {
        let (app, _dir) = build_test_app_with_single_node_raft().await;
        let resp = app.get("/cluster").await;
        assert_eq!(resp.status_code(), 200);
        let body: Value = resp.json();
        assert!(body["node_id"].is_u64());
        assert!(body["role"].is_string());
        assert!(body["members"].is_array());
    }

    #[tokio::test]
    async fn cluster_endpoint_without_raft_returns_503() {
        let app = build_test_app_standalone();
        let resp = app.get("/cluster").await;
        assert_eq!(resp.status_code(), 503);
    }

    #[tokio::test]
    async fn raft_metrics_appear_in_prometheus_scrape() {
        let (app, _dir) = build_test_app_with_single_node_raft().await;
        tokio::time::sleep(Duration::from_millis(700)).await;
        let resp = app.get("/metrics").await;
        let body = resp.text();
        assert!(body.contains("engram_raft_term"), "missing engram_raft_term");
        assert!(body.contains("engram_raft_commit_index"), "missing engram_raft_commit_index");
        assert!(body.contains("engram_raft_is_leader"), "missing engram_raft_is_leader");
    }
}
