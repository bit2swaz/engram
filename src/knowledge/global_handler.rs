use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde::Deserialize;

use crate::core::MemoryServerError;
use crate::knowledge::global::Visibility;
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

// Stub handlers for Task 6: routes registered here so the router compiles.
pub async fn get_global(State(_state): State<Arc<AppState>>) -> StatusCode {
    StatusCode::NOT_IMPLEMENTED
}

pub async fn get_global_entity(
    State(_state): State<Arc<AppState>>,
    Path(_name): Path<String>,
) -> StatusCode {
    StatusCode::NOT_IMPLEMENTED
}

pub async fn get_global_entity_sources(
    State(_state): State<Arc<AppState>>,
    Path(_name): Path<String>,
) -> StatusCode {
    StatusCode::NOT_IMPLEMENTED
}

pub async fn get_global_path(State(_state): State<Arc<AppState>>) -> StatusCode {
    StatusCode::NOT_IMPLEMENTED
}

pub async fn get_global_export(State(_state): State<Arc<AppState>>) -> StatusCode {
    StatusCode::NOT_IMPLEMENTED
}

pub async fn get_global_conflicts(State(_state): State<Arc<AppState>>) -> StatusCode {
    StatusCode::NOT_IMPLEMENTED
}
