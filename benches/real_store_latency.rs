// Real-Store Latency Benchmark for Engram
// See ROADMAP.md Phase 6.4 Part B

use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::Arc;
use tempfile::tempdir;

use engram::core::{EmbeddingProvider, ShortTermMemory, VectorStore};

// Mock embedding provider for instant embeddings
struct MockEmbeddingProvider;

#[async_trait::async_trait]
impl EmbeddingProvider for MockEmbeddingProvider {
    async fn embed(
        &self,
        texts: &[String],
    ) -> Result<Vec<Vec<f32>>, engram::core::EmbedError> {
        Ok(texts.iter().map(|_| vec![1.0; 1536]).collect())
    }
}

fn bench_real_store_latency(c: &mut Criterion) {
    use engram::assembler::ContextAssembler;
    use engram::core::OpenAITokenCounter;
    use engram::stores::{LanceDBStore, RedisShortTermMemory};
    use testcontainers::{
        core::{IntoContainerPort, WaitFor},
        runners::AsyncRunner,
        GenericImage,
    };
    use tokio::runtime::Runtime;

    let scenarios = [
        ("small_real", 10usize, 0usize),
        ("medium_real", 100usize, 0usize),
        ("large_real", 1000usize, 0usize),
    ];

    let rt = Runtime::new().unwrap();

    // ---------- async setup ----------
    let (container, tempdir, short_term_memory, vector_store, context_assembler) =
        rt.block_on(async {
            let container = GenericImage::new("redis", "7.2.4")
                .with_exposed_port(6379.tcp())
                .with_wait_for(WaitFor::message_on_stdout(
                    "Ready to accept connections",
                ))
                .start()
                .await
                .unwrap();

            let host_port = container.get_host_port_ipv4(6379).await.unwrap();
            let redis_url = format!("redis://127.0.0.1:{host_port}");

            let redis_store =
                Arc::new(RedisShortTermMemory::connect(&redis_url).await.unwrap());

            let tempdir = tempdir().unwrap();

            let lancedb_store =
                Arc::new(LanceDBStore::connect(tempdir.path()).await.unwrap());

            let token_counter =
                Arc::new(OpenAITokenCounter::new().unwrap());

            let embedding_provider =
                Arc::new(MockEmbeddingProvider);

            let core_memory_store = Arc::new(
                engram::stores::RedisCoreMemoryStore::connect(&redis_url)
                    .await
                    .unwrap(),
            );

            let context_assembler = Arc::new(ContextAssembler::new(
                redis_store.clone(),
                lancedb_store.clone(),
                embedding_provider,
                token_counter,
                core_memory_store,
            ));

            (
                container,
                tempdir,
                redis_store,
                lancedb_store,
                context_assembler,
            )
        });

    // ---------- benchmark phase ----------
    for (name, n_short, n_long) in scenarios {
        let session_id = format!("bench_{name}_session");

        // seed data
        rt.block_on(async {
            for i in 0..n_short {
                let msg = engram::models::Message {
                    id: Some(format!("msg_{i}")),
                    role: "user".to_string(),
                    content: format!("Short-term message {i}"),
                    timestamp: None,
                    embedding_status: Some(
                        engram::models::EmbeddingStatus::Completed,
                    ),
                };

                short_term_memory
                    .add_message(&session_id, msg)
                    .await
                    .unwrap();
            }

            for i in 0..n_long {
                vector_store
                    .insert(
                        &session_id,
                        &format!("Long-term message {i}"),
                        vec![1.0; 1536],
                        &format!("msg_long_{i}"),
                    )
                    .await
                    .unwrap();
            }
        });

        let assembler = context_assembler.clone();
        let sid = session_id.clone();

        c.bench_function(name, |b| {
            b.iter(|| {
                rt.block_on(async {
                    assembler
                        .assemble_context(&sid, 8000, 0.7, 10)
                        .await
                        .unwrap();
                });
            });
        });
    }

    // ---------- explicit cleanup while runtime still exists ----------
    drop(context_assembler);
    drop(vector_store);
    drop(short_term_memory);
    drop(tempdir);

    rt.block_on(async {
        drop(container);
    });
}

criterion_group!(benches, bench_real_store_latency);
criterion_main!(benches);