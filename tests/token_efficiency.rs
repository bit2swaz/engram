// Token Efficiency Measurement

use engram::assembler::ContextAssembler;
use engram::core::{
    CoreMemoryStore, EmbedError, EmbeddingProvider, InMemoryCoreMemoryStore, InMemoryStore,
    InMemoryVectorStore, OpenAITokenCounter, ShortTermMemory, TokenCounter, VectorStore,
};
use engram::metrics::AppMetrics;
use engram::models::{EmbeddingStatus, Message};
use engram::server::{AppState, build_router};
use engram::worker::embedding_job_channel;
use std::sync::Arc;

use async_trait::async_trait;
use axum_test::TestServer;
use serde::Deserialize;

const MESSAGE_COUNT: usize = 50;
const TARGET_TOKENS_PER_MESSAGE: usize = 120;
const MAX_TOKENS: usize = 4_000;
const MIN_REDUCTION_RATIO: f64 = 0.25;

#[derive(Debug)]
struct FixedEmbeddingProvider;

#[async_trait]
impl EmbeddingProvider for FixedEmbeddingProvider {
    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        Ok(texts.iter().map(|_| vec![1.0; 1_536]).collect())
    }
}

#[derive(Debug, Deserialize)]
struct CreateSessionResponse {
    session_id: String,
}

#[derive(Debug, Deserialize)]
struct ContextResponse {
    context: String,
}

fn build_test_state() -> Arc<AppState> {
    let (embedding_job_sender, mut receiver) = embedding_job_channel(16);
    tokio::spawn(async move { while receiver.recv().await.is_some() {} });

    let short_term_memory: Arc<dyn ShortTermMemory> = Arc::new(InMemoryStore::default());
    let vector_store: Arc<dyn VectorStore> = Arc::new(InMemoryVectorStore::default());
    let embedding_provider: Arc<dyn EmbeddingProvider> = Arc::new(FixedEmbeddingProvider);
    let token_counter: Arc<dyn TokenCounter> = Arc::new(OpenAITokenCounter::new().unwrap());
    let core_memory_store: Arc<dyn CoreMemoryStore> = Arc::new(InMemoryCoreMemoryStore::default());
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
        short_term_count: MESSAGE_COUNT,
        raft: None,
        node_id: 0,
        peer_http_addrs: std::collections::HashMap::new(),
    })
}

fn build_realistic_message(index: usize, token_counter: &dyn TokenCounter) -> String {
    let sentence = format!(
        "Message {index} covers Rust ownership, async workers, semantic retrieval, benchmark methodology, tracing, Prometheus metrics, token budgeting, retry behavior, LanceDB storage, Redis state, and realistic agent debugging details."
    );
    let mut text = String::new();

    while token_counter.count_tokens(&text) < TARGET_TOKENS_PER_MESSAGE {
        if !text.is_empty() {
            text.push(' ');
        }
        text.push_str(&sentence);
    }

    text
}

fn naive_full_dump(messages: &[Message]) -> String {
    let mut lines = vec!["Conversation:".to_string()];
    lines.extend(
        messages
            .iter()
            .map(|message| format!("{}: {}", message.role, message.content)),
    );
    lines.join("\n")
}

#[tokio::test]
async fn token_efficiency() {
    let state = build_test_state();
    let server = TestServer::new(build_router(state.clone())).unwrap();

    let session_response = server.post("/sessions").await;
    session_response.assert_status_ok();
    let session: CreateSessionResponse = session_response.json();

    let token_counter = OpenAITokenCounter::new().unwrap();
    let mut messages = Vec::with_capacity(MESSAGE_COUNT);
    for index in 0..MESSAGE_COUNT {
        messages.push(Message {
            id: Some(format!("message-{index}")),
            role: if index % 2 == 0 {
                "user".to_string()
            } else {
                "assistant".to_string()
            },
            content: build_realistic_message(index, &token_counter),
            timestamp: None,
            embedding_status: Some(EmbeddingStatus::Completed),
        });
    }

    for message in &messages {
        state
            .short_term_memory
            .add_message(&session.session_id, message.clone())
            .await
            .unwrap();
    }

    let full_history = naive_full_dump(&messages);
    let full_dump_tokens = token_counter.count_tokens(&full_history);

    let context_response = server
        .get(&format!(
            "/sessions/{}/context?max_tokens={MAX_TOKENS}",
            session.session_id
        ))
        .await;
    context_response.assert_status_ok();
    let context: ContextResponse = context_response.json();

    let assembled_tokens = token_counter.count_tokens(&context.context);
    let reduction_ratio = 1.0 - (assembled_tokens as f64 / full_dump_tokens as f64);

    assert!(assembled_tokens <= MAX_TOKENS);
    assert!(assembled_tokens < full_dump_tokens);
    assert!(
        reduction_ratio >= MIN_REDUCTION_RATIO,
        "expected at least {:.0}% reduction, got {:.2}% (full={}, assembled={})",
        MIN_REDUCTION_RATIO * 100.0,
        reduction_ratio * 100.0,
        full_dump_tokens,
        assembled_tokens,
    );

    println!(
        "Full dump: {} tokens, Assembled context: {} tokens, Reduction: {:.2}%",
        full_dump_tokens,
        assembled_tokens,
        reduction_ratio * 100.0,
    );
}
