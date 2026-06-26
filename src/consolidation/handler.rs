use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde_json::json;

use crate::consolidation::scheduler::ConsolidationJob;
use crate::server::{AppState, redirect_if_follower};

/// Returns a session's consolidated summaries as JSON.
pub async fn get_summaries(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
) -> impl IntoResponse {
    match state.consolidated.get_summaries(&session_id).await {
        Ok(summaries) => (StatusCode::OK, Json(json!({ "summaries": summaries }))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Manually triggers consolidation for a session.
///
/// A consolidate is a cluster mutation, so a follower 307s to the leader exactly like
/// `add_message`. The leader can't summarize synchronously (it needs an LLM call), so it
/// enqueues a job for the scheduler instead of writing a command here.
pub async fn post_consolidate(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
) -> impl IntoResponse {
    if let Some(raft) = &state.raft {
        if let Some(err) = redirect_if_follower(
            raft,
            state.node_id,
            &state.peer_http_addrs,
            &format!("/sessions/{session_id}/consolidate"),
        ) {
            return err.into_response();
        }
    }

    match state.consolidation_tx.try_send(ConsolidationJob { session_id }) {
        Ok(_) => {
            (StatusCode::ACCEPTED, Json(json!({ "status": "consolidation enqueued" }))).into_response()
        }
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "consolidation queue full").into_response(),
    }
}
