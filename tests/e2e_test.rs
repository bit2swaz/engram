use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use axum_test::TestServer;
use engram::app::build_app_state_with_embedding_provider;
use engram::config::Config;
use engram::core::{EmbeddingProvider, ShortTermMemory};
use engram::embedding::OpenAIEmbedder;
use engram::models::EmbeddingStatus;
use engram::server::build_router;
use serde::Deserialize;
use serde_json::json;
use tempfile::TempDir;
use testcontainers::{
    GenericImage,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::time::sleep;
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const REDIS_PORT: u16 = 6379;

#[derive(Debug, Deserialize)]
struct CreateSessionResponse {
    session_id: String,
}

#[derive(Debug, Deserialize)]
struct ContextResponse {
    context: String,
}

#[derive(Debug, Deserialize)]
struct SearchResultResponse {
    text: String,
    score: f32,
}

#[derive(Debug, Deserialize)]
struct SearchResponse {
    results: Vec<SearchResultResponse>,
}

async fn start_redis() -> (testcontainers::ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("redis", "7.2.4")
        .with_exposed_port(REDIS_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
        .start()
        .await
        .unwrap();

    let host = container.get_host().await.unwrap();
    let port = container
        .get_host_port_ipv4(REDIS_PORT.tcp())
        .await
        .unwrap();

    (container, format!("redis://{host}:{port}/"))
}

fn embedding_payload() -> serde_json::Value {
    json!({
        "data": [
            {
                "embedding": vec![0.5_f32; 1536],
                "index": 0,
                "object": "embedding"
            }
        ],
        "model": "text-embedding-3-small",
        "object": "list",
        "usage": {
            "prompt_tokens": 4,
            "total_tokens": 4
        }
    })
}

async fn wait_for_terminal_status(
    store: &dyn ShortTermMemory,
    session_id: &str,
    message_id: &str,
) -> EmbeddingStatus {
    for _ in 0..100 {
        if let Some(message) = store.get_message_by_id(session_id, message_id).await.unwrap() {
            match message.embedding_status {
                Some(EmbeddingStatus::Completed) => return EmbeddingStatus::Completed,
                Some(EmbeddingStatus::Failed(error)) => {
                    return EmbeddingStatus::Failed(error);
                }
                _ => {}
            }
        }

        sleep(Duration::from_millis(50)).await;
    }

    panic!("message did not reach a terminal embedding status in time");
}

#[tokio::test]
async fn e2e_flow_uses_real_stores_and_background_worker() {
    let (_redis_container, redis_url) = start_redis().await;
    let mock_server = MockServer::start().await;
    let lance_db_dir = TempDir::new().unwrap();

    Mock::given(method("POST"))
        .and(path("/v1/embeddings"))
        .respond_with(ResponseTemplate::new(200).set_body_json(embedding_payload()))
        .mount(&mock_server)
        .await;

    let config = Config {
        redis_url,
        openai_api_key: "test-key".to_string(),
        openai_base_url: None,
        lance_db_path: lance_db_dir.path().to_path_buf(),
        embedding_dimension: 1536,
        embedding_max_concurrency: 2,
        mpsc_channel_size: 8,
        short_term_count: 20,
        node_id: None,
        raft_addr: None,
        raft_advertise_addr: None,
        cluster_peers: vec![],
        cluster_http_peers: std::collections::HashMap::new(),
    };
    let embedding_provider: Arc<dyn EmbeddingProvider> = Arc::new(
        OpenAIEmbedder::new_with_base_url("test-key", mock_server.uri()).unwrap(),
    );
    let state = build_app_state_with_embedding_provider(&config, embedding_provider)
        .await
        .unwrap();
    let server = TestServer::new(build_router(state.clone())).unwrap();

    let session_response = server.post("/sessions").await;
    session_response.assert_status_ok();
    let session: CreateSessionResponse = session_response.json();

    server
        .put(&format!("/sessions/{}/core-memory", session.session_id))
        .json(&json!({
            "fact": "User likes integration tests"
        }))
        .await
        .assert_status(StatusCode::NO_CONTENT);

    let message_id = Uuid::new_v4().to_string();
    server
        .post(&format!("/sessions/{}/messages", session.session_id))
        .json(&json!({
            "id": message_id,
            "role": "user",
            "content": "Remember that Rust async is powerful"
        }))
        .await
        .assert_status(StatusCode::NO_CONTENT);

    let status = wait_for_terminal_status(
        state.short_term_memory.as_ref(),
        &session.session_id,
        &message_id,
    )
    .await;
    match status {
        EmbeddingStatus::Completed => {}
        EmbeddingStatus::Failed(error) => panic!("embedding job failed: {error}"),
        other => panic!("unexpected terminal status: {other:?}"),
    }

    let context_response = server
        .get(&format!("/sessions/{}/context?max_tokens=1000", session.session_id))
        .await;
    context_response.assert_status_ok();
    let context: ContextResponse = context_response.json();
    assert!(context.context.contains("Remember that Rust async is powerful"));
    assert!(context.context.contains("User likes integration tests"));

    let search_response = server
        .post(&format!("/sessions/{}/search", session.session_id))
        .json(&json!({
            "query": "Rust async",
            "top_k": 5
        }))
        .await;
    search_response.assert_status_ok();
    let search: SearchResponse = search_response.json();
    assert!(!search.results.is_empty());
    assert!(search
        .results
        .iter()
        .any(|result| result.text.contains("Remember that Rust async is powerful")));
    assert!(search.results.iter().all(|result| result.score.is_finite()));
}