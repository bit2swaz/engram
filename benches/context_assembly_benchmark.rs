
use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use engram::assembler::ContextAssembler;
use engram::core::{OpenAITokenCounter};
use std::sync::Arc;
use tokio::runtime::Runtime;


use engram::models::{Message, EmbeddingStatus};
use async_trait::async_trait;

// Minimal mocks for benchmarking
struct BenchShortTermMemory {
    messages: Vec<Message>,
}

#[async_trait]
impl engram::core::ShortTermMemory for BenchShortTermMemory {
    async fn add_message(&self, _session_id: &str, _msg: Message) -> Result<(), engram::core::MemoryError> { Ok(()) }
    async fn get_recent(&self, _session_id: &str, _count: usize) -> Result<Vec<Message>, engram::core::MemoryError> { Ok(self.messages.clone()) }
    async fn trim(&self, _session_id: &str, _max_count: usize) -> Result<(), engram::core::MemoryError> { Ok(()) }
    async fn trim_to_token_budget(&self, _session_id: &str, _max_tokens: usize, _token_counter: &dyn engram::core::TokenCounter) -> Result<Vec<Message>, engram::core::MemoryError> { Ok(self.messages.clone()) }
    async fn delete_session(&self, _session_id: &str) -> Result<(), engram::core::MemoryError> { Ok(()) }
}

struct BenchVectorStore;
#[async_trait]
impl engram::core::VectorStore for BenchVectorStore {
    async fn insert(&self, _session_id: &str, _text: &str, _embedding: Vec<f32>, _message_id: &str) -> Result<(), engram::core::StoreError> { Ok(()) }
    async fn search(&self, _session_id: &str, _query_embedding: &[f32], _top_k: usize) -> Result<Vec<engram::core::SearchResult>, engram::core::StoreError> {
        Ok(vec![engram::core::SearchResult { text: "Relevant memory".to_string(), score: 0.9 }; 10])
    }
    async fn delete_session(&self, _session_id: &str) -> Result<(), engram::core::StoreError> { Ok(()) }
}

struct BenchEmbeddingProvider;
#[async_trait]
impl engram::core::EmbeddingProvider for BenchEmbeddingProvider {
    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, engram::core::EmbedError> {
        Ok(texts.iter().map(|_| vec![1.0; 1536]).collect())
    }
}

struct BenchCoreMemoryStore;
#[async_trait]
impl engram::core::CoreMemoryStore for BenchCoreMemoryStore {
    async fn add_fact(&self, _session_id: &str, _fact: &str) -> Result<(), engram::core::MemoryError> { Ok(()) }
    async fn get_facts(&self, _session_id: &str) -> Result<Vec<String>, engram::core::MemoryError> { Ok(vec!["Pinned fact".to_string()]) }
    async fn delete_session(&self, _session_id: &str) -> Result<(), engram::core::MemoryError> { Ok(()) }
}

fn make_assembler(messages: Vec<Message>) -> ContextAssembler {
    ContextAssembler::new(
        Arc::new(BenchShortTermMemory { messages }),
        Arc::new(BenchVectorStore),
        Arc::new(BenchEmbeddingProvider),
        Arc::new(OpenAITokenCounter::new().unwrap()),
        Arc::new(BenchCoreMemoryStore),
    )
}

fn bench_context_assembly(c: &mut Criterion) {
    let mut group = c.benchmark_group("context_assembly");
    let rt = Runtime::new().unwrap();

    let make_messages = |n| (0..n).map(|i| Message {
        id: None,
        role: if i % 2 == 0 { "user".to_string() } else { "assistant".to_string() },
        content: format!("Message {}", i),
        timestamp: None,
        embedding_status: Some(EmbeddingStatus::Pending),
    }).collect::<Vec<_>>();

    let configs = [
        ("small", 10),
        ("medium", 100),
        ("large", 1000),
        ("tight_budget", 50),
        ("long_term", 5000),
    ];

    for (name, size) in configs.iter() {
        let assembler = make_assembler(make_messages(*size));
        group.bench_with_input(BenchmarkId::new(*name, size), size, |b, _| {
            b.iter(|| {
                rt.block_on(assembler.assemble_context("session-1", 2048, 0.7, 10)).unwrap();
            })
        });
    }

    group.finish();
}

criterion_group!(benches, bench_context_assembly);
criterion_main!(benches);
