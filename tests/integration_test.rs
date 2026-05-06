use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use axum_test::TestServer;
use engram::app::build_app_state_with_embedding_provider;
use engram::config::Config;
use engram::core::{EmbeddingProvider, ShortTermMemory};
use engram::embedding::OpenAIEmbedder;
use engram::models::EmbeddingStatus;
use engram::server::AppState;
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

struct TestApp {
    server: TestServer,
    state: Arc<AppState>,
    _lance_db_dir: TempDir,
    _redis_container: testcontainers::ContainerAsync<GenericImage>,
    _mock_server: MockServer,
}

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

async fn setup_test_app() -> TestApp {
    let redis_container = GenericImage::new("redis", "7.2.4")
        .with_exposed_port(REDIS_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
        .start()
        .await
        .unwrap();

    let host = redis_container.get_host().await.unwrap();
    let port = redis_container
        .get_host_port_ipv4(REDIS_PORT.tcp())
        .await
        .unwrap();
    let redis_url = format!("redis://{host}:{port}/");

    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/embeddings"))
        .respond_with(ResponseTemplate::new(200).set_body_json(embedding_payload()))
        .mount(&mock_server)
        .await;

    let lance_db_dir = TempDir::new().unwrap();
    let config = Config {
        redis_url,
        openai_api_key: "test-key".to_string(),
        openai_base_url: None,
        lance_db_path: lance_db_dir.path().to_path_buf(),
        embedding_dimension: 1536,
        embedding_max_concurrency: 1,
        mpsc_channel_size: 4,
        short_term_count: 20,
    };

    let embedding_provider: Arc<dyn EmbeddingProvider> = Arc::new(
        OpenAIEmbedder::new_with_base_url("test-key", mock_server.uri()).unwrap(),
    );
    let state = build_app_state_with_embedding_provider(&config, embedding_provider)
        .await
        .unwrap();
    let server = TestServer::new(build_router(state.clone())).unwrap();

    TestApp {
        server,
        state,
        _lance_db_dir: lance_db_dir,
        _redis_container: redis_container,
        _mock_server: mock_server,
    }
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

fn test_query_embedding() -> Vec<f32> {
    vec![0.5_f32; 1536]
}

async fn create_session(server: &TestServer) -> String {
    let response = server.post("/sessions").await;
    response.assert_status_ok();
    let body: CreateSessionResponse = response.json();
    body.session_id
}

async fn add_user_message(
    server: &TestServer,
    session_id: &str,
    message_id: &str,
    content: &str,
) {
    server
        .post(&format!("/sessions/{session_id}/messages"))
        .json(&json!({
            "id": message_id,
            "role": "user",
            "content": content
        }))
        .await
        .assert_status(StatusCode::NO_CONTENT);
}

async fn wait_for_terminal_status(
    store: &dyn ShortTermMemory,
    session_id: &str,
    message_id: &str,
) -> EmbeddingStatus {
    for _ in 0..200 {
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

async fn wait_for_message_count(
    store: &dyn ShortTermMemory,
    session_id: &str,
    expected_count: usize,
) {
    for _ in 0..100 {
        let messages = store.get_recent(session_id, expected_count + 5).await.unwrap();
        if messages.len() == expected_count {
            return;
        }

        sleep(Duration::from_millis(25)).await;
    }

    panic!("session did not reach the expected message count in time");
}

#[tokio::test]
async fn full_pipeline_with_context_assembly_uses_real_infrastructure() {
    let app = setup_test_app().await;
    let session_id = create_session(&app.server).await;

    let first_message_id = Uuid::new_v4().to_string();
    let second_message_id = Uuid::new_v4().to_string();
    let first_content = "Rust async keeps the API responsive";
    let second_content = "Tokio workers can process embeddings in the background";

    add_user_message(&app.server, &session_id, &first_message_id, first_content).await;
    add_user_message(&app.server, &session_id, &second_message_id, second_content).await;

    match wait_for_terminal_status(
        app.state.short_term_memory.as_ref(),
        &session_id,
        &first_message_id,
    )
    .await
    {
        EmbeddingStatus::Completed => {}
        EmbeddingStatus::Failed(error) => panic!("first embedding job failed: {error}"),
        other => panic!("unexpected first terminal status: {other:?}"),
    }

    match wait_for_terminal_status(
        app.state.short_term_memory.as_ref(),
        &session_id,
        &second_message_id,
    )
    .await
    {
        EmbeddingStatus::Completed => {}
        EmbeddingStatus::Failed(error) => panic!("second embedding job failed: {error}"),
        other => panic!("unexpected second terminal status: {other:?}"),
    }

    app.server
        .put(&format!("/sessions/{session_id}/core-memory"))
        .json(&json!({
            "fact": "User prefers durable memory tests"
        }))
        .await
        .assert_status(StatusCode::NO_CONTENT);

    let response = app
        .server
        .get(&format!("/sessions/{session_id}/context?max_tokens=500"))
        .await;
    response.assert_status_ok();
    let body: ContextResponse = response.json();

    assert!(body.context.contains(first_content));
    assert!(body.context.contains(second_content));
    assert!(body.context.contains("User prefers durable memory tests"));
}

#[tokio::test]
async fn semantic_search_endpoint_returns_matching_message_with_high_score() {
    let app = setup_test_app().await;
    let session_id = create_session(&app.server).await;
    let message_id = Uuid::new_v4().to_string();
    let content = "Semantic search should find this Rust async memory";

    add_user_message(&app.server, &session_id, &message_id, content).await;

    match wait_for_terminal_status(
        app.state.short_term_memory.as_ref(),
        &session_id,
        &message_id,
    )
    .await
    {
        EmbeddingStatus::Completed => {}
        EmbeddingStatus::Failed(error) => panic!("embedding job failed: {error}"),
        other => panic!("unexpected terminal status: {other:?}"),
    }

    let response = app
        .server
        .post(&format!("/sessions/{session_id}/search"))
        .json(&json!({
            "query": "Rust async memory",
            "top_k": 5
        }))
        .await;
    response.assert_status_ok();
    let body: SearchResponse = response.json();

    assert!(!body.results.is_empty());
    assert!(body.results.iter().any(|result| {
        result.text.contains(content) && result.score >= 0.9 && result.score.is_finite()
    }));
}

#[tokio::test]
async fn worker_idempotency_keeps_duplicate_short_term_messages_but_single_vector_entry() {
    let app = setup_test_app().await;
    let session_id = create_session(&app.server).await;
    let duplicate_message_id = Uuid::new_v4().to_string();
    let original_content = "Original memory to embed once";
    let duplicate_content = "Duplicate short-term message that should not create a second vector";

    add_user_message(
        &app.server,
        &session_id,
        &duplicate_message_id,
        original_content,
    )
    .await;

    match wait_for_terminal_status(
        app.state.short_term_memory.as_ref(),
        &session_id,
        &duplicate_message_id,
    )
    .await
    {
        EmbeddingStatus::Completed => {}
        EmbeddingStatus::Failed(error) => panic!("first embedding job failed: {error}"),
        other => panic!("unexpected first terminal status: {other:?}"),
    }

    add_user_message(
        &app.server,
        &session_id,
        &duplicate_message_id,
        duplicate_content,
    )
    .await;

    wait_for_message_count(app.state.short_term_memory.as_ref(), &session_id, 2).await;
    sleep(Duration::from_millis(250)).await;

    let recent_messages = app
        .state
        .short_term_memory
        .get_recent(&session_id, 10)
        .await
        .unwrap();
    assert_eq!(recent_messages.len(), 2);
    assert_eq!(
        recent_messages
            .iter()
            .filter(|message| message.id.as_deref() == Some(duplicate_message_id.as_str()))
            .count(),
        2
    );

    let vector_results = app
        .state
        .vector_store
        .search(&session_id, &test_query_embedding(), 10)
        .await
        .unwrap();
    assert_eq!(vector_results.len(), 1);
    assert!(vector_results[0].text.contains(original_content));
    assert!(vector_results
        .iter()
        .all(|result| !result.text.contains(duplicate_content)));
}

#[tokio::test]
async fn deleting_a_session_removes_context_and_vector_results() {
    let app = setup_test_app().await;
    let session_id = create_session(&app.server).await;
    let message_id = Uuid::new_v4().to_string();

    add_user_message(
        &app.server,
        &session_id,
        &message_id,
        "Delete this entire session",
    )
    .await;

    match wait_for_terminal_status(
        app.state.short_term_memory.as_ref(),
        &session_id,
        &message_id,
    )
    .await
    {
        EmbeddingStatus::Completed => {}
        EmbeddingStatus::Failed(error) => panic!("embedding job failed: {error}"),
        other => panic!("unexpected terminal status: {other:?}"),
    }

    app.server
        .delete(&format!("/sessions/{session_id}"))
        .await
        .assert_status(StatusCode::NO_CONTENT);

    app.server
        .get(&format!("/sessions/{session_id}/context?max_tokens=500"))
        .await
        .assert_status(StatusCode::NOT_FOUND);

    let recent_messages = app
        .state
        .short_term_memory
        .get_recent(&session_id, 10)
        .await
        .unwrap();
    assert!(recent_messages.is_empty());

    let vector_results = app
        .state
        .vector_store
        .search(&session_id, &test_query_embedding(), 10)
        .await
        .unwrap();
    assert!(vector_results.is_empty());
}