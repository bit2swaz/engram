use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};

use crate::core::MemoryServerError;
use crate::knowledge::export::{GraphExport, to_dot};
use crate::knowledge::global::{Conflict, Visibility};
use crate::knowledge::graph::{PathEdge, RelatedEntity};
use crate::knowledge::types::{Entity, Relationship};
use crate::raft::types::MemoryCommand;
use crate::server::AppState;

#[derive(Debug, Deserialize)]
pub struct SetVisibilityRequest {
    pub visibility: Visibility,
}

pub async fn set_visibility(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
    Json(body): Json<SetVisibilityRequest>,
) -> Result<StatusCode, MemoryServerError> {
    if let Some(raft) = &state.raft {
        return raft
            .client_write(MemoryCommand::SetSessionVisibility {
                session_id: session_id.clone(),
                visibility: body.visibility,
            })
            .await
            .map(|_| StatusCode::NO_CONTENT)
            .map_err(|e| {
                if let Some(fwd) = e.forward_to_leader::<openraft::BasicNode>() {
                    if let Some(leader_id) = fwd.leader_id {
                        if let Some(http_addr) = state.peer_http_addrs.get(&leader_id) {
                            let location = format!(
                                "http://{}/sessions/{}/visibility",
                                http_addr, session_id
                            );
                            return MemoryServerError::RedirectToLeader(location);
                        }
                    }
                    return MemoryServerError::NoLeader;
                }
                MemoryServerError::Internal(format!("raft error: {e}"))
            });
    }

    // Standalone mode: no cluster to coordinate; accept and no-op.
    Ok(StatusCode::NO_CONTENT)
}

// ---- Read-only global graph handlers ----------------------------------------

#[derive(Serialize)]
pub struct GlobalKnowledgeResponse {
    entities: Vec<Entity>,
    edges: Vec<Relationship>,
}

pub async fn get_global(
    State(state): State<Arc<AppState>>,
) -> Json<GlobalKnowledgeResponse> {
    let g = state.global_graph.read().await;
    Json(GlobalKnowledgeResponse {
        entities: g.all_entities(),
        edges: g.all_relationships(),
    })
}

#[derive(Serialize)]
pub struct RelatedResponse {
    entity_name: String,
    related: Vec<RelatedEntity>,
}

pub async fn get_global_entity(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Json<RelatedResponse> {
    let g = state.global_graph.read().await;
    Json(RelatedResponse {
        entity_name: name.clone(),
        related: g.get_related(&name),
    })
}

#[derive(Serialize)]
pub struct SourcesResponse {
    entity_name: String,
    sources: Vec<String>,
}

pub async fn get_global_entity_sources(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Json<SourcesResponse> {
    let g = state.global_graph.read().await;
    Json(SourcesResponse {
        entity_name: name.clone(),
        sources: g.entity_sources(&name),
    })
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
    path: Option<Vec<PathEdge>>,
}

pub async fn get_global_path(
    State(state): State<Arc<AppState>>,
    Query(params): Query<PathQuery>,
) -> Json<PathResponse> {
    let g = state.global_graph.read().await;
    let path = g.find_path(&params.from, &params.to);
    Json(PathResponse { from: params.from, to: params.to, path })
}

#[derive(Deserialize)]
pub struct ExportQuery {
    #[serde(default = "default_format")]
    format: String,
}

fn default_format() -> String {
    "json".to_string()
}

pub async fn get_global_export(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ExportQuery>,
) -> (StatusCode, [(axum::http::header::HeaderName, &'static str); 1], String) {
    let g = state.global_graph.read().await;
    let export = GraphExport::new("global", g.all_entities(), g.all_relationships());
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

#[derive(Serialize)]
pub struct ConflictsResponse {
    conflicts: Vec<Conflict>,
}

pub async fn get_global_conflicts(
    State(state): State<Arc<AppState>>,
) -> Json<ConflictsResponse> {
    let g = state.global_graph.read().await;
    Json(ConflictsResponse { conflicts: g.conflicts() })
}
