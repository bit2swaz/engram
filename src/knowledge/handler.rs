use std::sync::Arc;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};

use crate::knowledge::export::{to_dot, GraphExport};
use crate::knowledge::graph::RelatedEntity;
use crate::server::AppState;

#[derive(Serialize)]
pub struct KnowledgeResponse {
    session_id: String,
    entities: Vec<crate::knowledge::types::Entity>,
    edges: Vec<crate::knowledge::types::Relationship>,
}

pub async fn get_knowledge(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
) -> Json<KnowledgeResponse> {
    let kg = state.knowledge_graph.read().await;
    Json(KnowledgeResponse {
        session_id: session_id.clone(),
        entities: kg.all_entities(&session_id),
        edges: kg.all_relationships(&session_id),
    })
}

#[derive(Serialize)]
pub struct RelatedResponse {
    entity_name: String,
    related: Vec<RelatedEntity>,
}

pub async fn get_related(
    State(state): State<Arc<AppState>>,
    Path((session_id, entity_name)): Path<(String, String)>,
) -> Result<Json<RelatedResponse>, StatusCode> {
    let kg = state.knowledge_graph.read().await;
    let exists = kg.all_entities(&session_id).iter().any(|e| e.name == entity_name);
    if !exists {
        return Err(StatusCode::NOT_FOUND);
    }
    let related = kg.get_related(&session_id, &entity_name);
    Ok(Json(RelatedResponse { entity_name, related }))
}

#[derive(Deserialize)]
pub struct PathQuery {
    from: String,
    to: String,
}

#[derive(Serialize)]
pub struct PathResponse {
    from: String,
    to: String,
    path: Option<Vec<crate::knowledge::graph::PathEdge>>,
}

pub async fn find_path(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
    Query(params): Query<PathQuery>,
) -> Json<PathResponse> {
    let kg = state.knowledge_graph.read().await;
    let path = kg.find_path(&session_id, &params.from, &params.to);
    Json(PathResponse { from: params.from, to: params.to, path })
}

#[derive(Deserialize)]
pub struct ExportQuery {
    #[serde(default = "default_format")]
    format: String,
}

fn default_format() -> String { "json".to_string() }

pub async fn export_knowledge(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
    Query(params): Query<ExportQuery>,
) -> (StatusCode, [(axum::http::header::HeaderName, &'static str); 1], String) {
    let kg = state.knowledge_graph.read().await;
    let export = GraphExport::new(
        session_id.clone(),
        kg.all_entities(&session_id),
        kg.all_relationships(&session_id),
    );
    match params.format.as_str() {
        "dot" => (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "text/vnd.graphviz")],
            to_dot(&export),
        ),
        _ => (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            serde_json::to_string(&export).unwrap_or_default(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use axum_test::TestServer;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    use crate::knowledge::graph::KnowledgeGraph;
    use crate::knowledge::types::{Entity, KnowledgeJob, Relationship};
    use crate::server::{AppState, build_router};

    fn make_state_with_graph(kg: Arc<RwLock<KnowledgeGraph>>) -> Arc<AppState> {
        use crate::assembler::ContextAssembler;
        use crate::core::{InMemoryCoreMemoryStore, InMemoryStore, InMemoryVectorStore,
                          OpenAITokenCounter, RandomEmbeddingProvider};
        use crate::metrics::AppMetrics;
        use crate::worker::embedding_job_channel;

        let short_term_memory    = Arc::new(InMemoryStore::default());
        let vector_store         = Arc::new(InMemoryVectorStore::default());
        let embedding_provider   = Arc::new(RandomEmbeddingProvider);
        let token_counter        = Arc::new(OpenAITokenCounter::new().unwrap());
        let core_memory_store    = Arc::new(InMemoryCoreMemoryStore::default());
        let metrics              = Arc::new(AppMetrics::new().unwrap());
        let context_assembler    = Arc::new(ContextAssembler::new(
            short_term_memory.clone(), vector_store.clone(),
            embedding_provider.clone(), token_counter.clone(), core_memory_store.clone(),
        ));
        let (embedding_job_sender, mut rx) = embedding_job_channel(16);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let (knowledge_job_sender, mut krx) = tokio::sync::mpsc::channel::<KnowledgeJob>(16);
        tokio::spawn(async move { while krx.recv().await.is_some() {} });

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
            knowledge_graph: kg,
            knowledge_job_sender,
            global_graph: Arc::new(tokio::sync::RwLock::new(
                crate::knowledge::global::GlobalGraph::new(),
            )),
        })
    }

    #[tokio::test]
    async fn get_knowledge_returns_empty_for_new_session() {
        let kg = Arc::new(RwLock::new(KnowledgeGraph::new()));
        let server = TestServer::new(build_router(make_state_with_graph(kg))).unwrap();

        let resp = server.get("/sessions/s1/knowledge").await;
        resp.assert_status_ok();
        let body: serde_json::Value = resp.json();
        assert!(body["entities"].as_array().unwrap().is_empty());
        assert!(body["edges"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn get_knowledge_returns_graph_state() {
        let kg = Arc::new(RwLock::new(KnowledgeGraph::new()));
        {
            let mut graph = kg.write().await;
            graph.apply_extraction("s1", "m1",
                vec![
                    Entity { name: "Alice".into(), entity_type: "Person".into(), attributes: HashMap::new() },
                    Entity { name: "OpenAI".into(), entity_type: "Organization".into(), attributes: HashMap::new() },
                ],
                vec![Relationship { from: "Alice".into(), to: "OpenAI".into(), relationship_type: "works_at".into() }],
            );
        }
        let server = TestServer::new(build_router(make_state_with_graph(kg))).unwrap();

        let resp = server.get("/sessions/s1/knowledge").await;
        resp.assert_status_ok();
        let body: serde_json::Value = resp.json();
        assert_eq!(body["entities"].as_array().unwrap().len(), 2);
        assert_eq!(body["edges"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn get_related_returns_connections() {
        let kg = Arc::new(RwLock::new(KnowledgeGraph::new()));
        {
            let mut graph = kg.write().await;
            graph.apply_extraction("s1", "m1",
                vec![
                    Entity { name: "Alice".into(), entity_type: "Person".into(), attributes: HashMap::new() },
                    Entity { name: "OpenAI".into(), entity_type: "Organization".into(), attributes: HashMap::new() },
                ],
                vec![Relationship { from: "Alice".into(), to: "OpenAI".into(), relationship_type: "works_at".into() }],
            );
        }
        let server = TestServer::new(build_router(make_state_with_graph(kg))).unwrap();

        let resp = server.get("/sessions/s1/knowledge/entities/Alice").await;
        resp.assert_status_ok();
        let body: serde_json::Value = resp.json();
        let related = body["related"].as_array().unwrap();
        assert!(!related.is_empty());
        assert_eq!(related[0]["name"], "OpenAI");
        assert_eq!(related[0]["relationship_type"], "works_at");
    }

    #[tokio::test]
    async fn get_related_returns_404_for_unknown_entity() {
        let kg = Arc::new(RwLock::new(KnowledgeGraph::new()));
        let server = TestServer::new(build_router(make_state_with_graph(kg))).unwrap();

        let resp = server.get("/sessions/s1/knowledge/entities/nobody").await;
        resp.assert_status(axum::http::StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn export_dot_returns_dot_string() {
        let kg = Arc::new(RwLock::new(KnowledgeGraph::new()));
        {
            let mut graph = kg.write().await;
            graph.apply_extraction("s1", "m1",
                vec![Entity { name: "Alice".into(), entity_type: "Person".into(), attributes: HashMap::new() }],
                vec![],
            );
        }
        let server = TestServer::new(build_router(make_state_with_graph(kg))).unwrap();

        let resp = server.get("/sessions/s1/knowledge/export?format=dot").await;
        resp.assert_status_ok();
        let body = resp.text();
        assert!(body.contains("digraph knowledge"));
        assert!(body.contains("Alice"));
    }
}
