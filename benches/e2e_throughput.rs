// End-to-End Throughput Benchmark for Engram
// See ROADMAP.md Phase 6.4 Part A

use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::Barrier;

use engram::assembler::ContextAssembler;
use engram::core::{
    CoreMemoryStore, EmbedError, EmbeddingProvider, InMemoryCoreMemoryStore, InMemoryStore,
    OpenAITokenCounter, ShortTermMemory, TokenCounter, VectorStore,
};
use engram::metrics::AppMetrics;
use engram::models::EmbeddingStatus;
use engram::server::{AppState, build_router};
use engram::stores::LanceDBStore;
use engram::worker::{embedding_job_channel, spawn_embedding_workers};

type BenchError = Box<dyn Error + Send + Sync>;

const DEFAULT_ITERATIONS: usize = 10;
const DEFAULT_CONCURRENT_TASKS: usize = 8;
const DEFAULT_MESSAGES_PER_TASK: usize = 250;
const DEFAULT_CONTEXT_SAMPLES: usize = 100;
const DEFAULT_MAX_TOKENS: usize = 8_000;
const DEFAULT_WORKER_COUNT: usize = 4;
const DEFAULT_CHANNEL_SIZE: usize = 1_000;
const DEFAULT_TIMEOUT_SECS: u64 = 30;
const DEFAULT_SIMILARITY_THRESHOLD: f32 = 0.7;
const DEFAULT_LONG_TERM_TOP_K: usize = 10;

#[derive(Clone, Debug)]
struct BenchConfig {
    iterations: usize,
    concurrent_tasks: usize,
    messages_per_task: usize,
    context_samples: usize,
    max_tokens: usize,
    worker_count: usize,
    channel_size: usize,
    timeout: Duration,
}

impl BenchConfig {
    fn from_env() -> Self {
        Self {
            iterations: env_usize("E2E_BENCH_ITERATIONS", DEFAULT_ITERATIONS),
            concurrent_tasks: env_usize("E2E_BENCH_TASKS", DEFAULT_CONCURRENT_TASKS),
            messages_per_task: env_usize(
                "E2E_BENCH_MESSAGES_PER_TASK",
                DEFAULT_MESSAGES_PER_TASK,
            ),
            context_samples: env_usize("E2E_BENCH_CONTEXT_SAMPLES", DEFAULT_CONTEXT_SAMPLES),
            max_tokens: env_usize("E2E_BENCH_MAX_TOKENS", DEFAULT_MAX_TOKENS),
            worker_count: env_usize("E2E_BENCH_WORKERS", DEFAULT_WORKER_COUNT),
            channel_size: env_usize("E2E_BENCH_CHANNEL_SIZE", DEFAULT_CHANNEL_SIZE),
            timeout: Duration::from_secs(env_u64("E2E_BENCH_TIMEOUT_SECS", DEFAULT_TIMEOUT_SECS)),
        }
    }

    fn total_messages(&self) -> usize {
        self.concurrent_tasks * self.messages_per_task
    }
}

#[derive(Debug, Deserialize)]
struct CreateSessionResponse {
    session_id: String,
}

#[derive(Debug, Serialize)]
struct AddMessageRequest {
    id: String,
    role: String,
    content: String,
}

#[derive(Debug)]
struct IterationResult {
    throughput: f64,
    p99_context_latency_ms: f64,
}

struct BenchmarkHarness {
    state: Arc<AppState>,
    client: Client,
    base_url: String,
    _lancedb_dir: TempDir,
}

#[derive(Debug)]
struct MockEmbeddingProvider;

#[async_trait]
impl EmbeddingProvider for MockEmbeddingProvider {
    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        Ok(texts.iter().map(|_| vec![1.0; 1_536]).collect())
    }
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime");

    if let Err(error) = runtime.block_on(async_main()) {
        eprintln!("e2e throughput benchmark failed: {error}");
        std::process::exit(1);
    }
}

async fn async_main() -> Result<(), BenchError> {
    let config = BenchConfig::from_env();
    let harness = BenchmarkHarness::start(&config).await?;

    println!(
        "Running e2e throughput benchmark: iterations={}, tasks={}, messages_per_task={}, context_samples={}",
        config.iterations,
        config.concurrent_tasks,
        config.messages_per_task,
        config.context_samples,
    );

    let mut iteration_results = Vec::with_capacity(config.iterations);
    for iteration in 0..config.iterations {
        let result = run_iteration(&harness, &config, iteration + 1).await?;
        println!(
            "Iteration {}: Throughput = {:.2} msg/s, P99 Context Latency = {:.2} ms",
            iteration + 1,
            result.throughput,
            result.p99_context_latency_ms,
        );
        iteration_results.push(result);
    }

    let throughputs = iteration_results
        .iter()
        .map(|result| result.throughput)
        .collect::<Vec<_>>();
    let p99s = iteration_results
        .iter()
        .map(|result| result.p99_context_latency_ms)
        .collect::<Vec<_>>();

    println!("Throughput: {:.2} msg/s", median(throughputs));
    println!("P99 Context Latency: {:.2} ms", median(p99s));

    Ok(())
}

impl BenchmarkHarness {
    async fn start(config: &BenchConfig) -> Result<Self, BenchError> {
        let lancedb_dir = tempfile::tempdir()?;

        let short_term_memory: Arc<dyn ShortTermMemory> = Arc::new(InMemoryStore::default());
        let vector_store: Arc<dyn VectorStore> = Arc::new(LanceDBStore::connect(lancedb_dir.path()).await?);
        let embedding_provider: Arc<dyn EmbeddingProvider> = Arc::new(MockEmbeddingProvider);
        let token_counter: Arc<dyn TokenCounter> = Arc::new(OpenAITokenCounter::new()?);
        let core_memory_store: Arc<dyn CoreMemoryStore> = Arc::new(InMemoryCoreMemoryStore::default());
        let metrics = Arc::new(AppMetrics::new()?);
        let context_assembler = Arc::new(ContextAssembler::new(
            short_term_memory.clone(),
            vector_store.clone(),
            embedding_provider.clone(),
            token_counter.clone(),
            core_memory_store.clone(),
        ));
        let (embedding_job_sender, receiver) = embedding_job_channel(config.channel_size);

        let (knowledge_job_sender, mut krx) = tokio::sync::mpsc::channel::<engram::knowledge::types::KnowledgeJob>(16);
        tokio::spawn(async move { while krx.recv().await.is_some() {} });
        let state = Arc::new(AppState {
            short_term_memory: short_term_memory.clone(),
            vector_store: vector_store.clone(),
            embedding_provider: embedding_provider.clone(),
            token_counter: token_counter.clone(),
            core_memory_store: core_memory_store.clone(),
            context_assembler,
            metrics: metrics.clone(),
            embedding_job_sender,
            short_term_count: config.total_messages().max(1),
            raft: None,
            node_id: 0,
            peer_http_addrs: std::collections::HashMap::new(),
            raft_addr: None,
            raft_advertise_addr: None,
            cluster_peers: vec![],
            knowledge_graph: Arc::new(tokio::sync::RwLock::new(
                engram::knowledge::graph::KnowledgeGraph::new(),
            )),
            knowledge_job_sender,
        });

        let _worker_handles = spawn_embedding_workers(
            short_term_memory,
            vector_store,
            embedding_provider,
            metrics,
            receiver,
            config.worker_count,
        );

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let app = build_router(state.clone());

        tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, app).await {
                eprintln!("benchmark server failed: {error}");
            }
        });

        let client = Client::builder().pool_idle_timeout(Duration::from_secs(30)).build()?;
        let base_url = format!("http://{address}");
        wait_for_server(&client, &base_url, config.timeout).await?;

        Ok(Self {
            state,
            client,
            base_url,
            _lancedb_dir: lancedb_dir,
        })
    }
}

async fn run_iteration(
    harness: &BenchmarkHarness,
    config: &BenchConfig,
    iteration: usize,
) -> Result<IterationResult, BenchError> {
    let session_id = create_session(&harness.client, &harness.base_url).await?;
    let total_messages = config.total_messages();
    let barrier = Arc::new(Barrier::new(config.concurrent_tasks + 1));
    let mut handles = Vec::with_capacity(config.concurrent_tasks);

    for task_index in 0..config.concurrent_tasks {
        let client = harness.client.clone();
        let base_url = harness.base_url.clone();
        let session_id = session_id.clone();
        let barrier = barrier.clone();
        let messages_per_task = config.messages_per_task;
        let timeout = config.timeout;

        handles.push(tokio::spawn(async move {
            barrier.wait().await;

            for message_index in 0..messages_per_task {
                let payload = AddMessageRequest {
                    id: format!("iter-{iteration}-task-{task_index}-msg-{message_index}"),
                    role: "user".to_string(),
                    content: format!(
                        "Iteration {iteration} task {task_index} message {message_index}: benchmarking Engram throughput"
                    ),
                };

                let deadline = Instant::now() + timeout;
                loop {
                    let response = client
                        .post(format!("{base_url}/sessions/{session_id}/messages"))
                        .json(&payload)
                        .send()
                        .await?;

                    if response.status() == StatusCode::NO_CONTENT {
                        break;
                    }

                    if response.status() == StatusCode::SERVICE_UNAVAILABLE {
                        if Instant::now() >= deadline {
                            return Err(bench_error(format!(
                                "message {} exceeded backpressure timeout",
                                payload.id
                            )));
                        }

                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }

                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    return Err(bench_error(format!(
                        "message request failed with status {}: {}",
                        status, body
                    )));
                }
            }

            Ok::<(), BenchError>(())
        }));
    }

    let start = Instant::now();
    barrier.wait().await;
    for handle in handles {
        let send_result = handle.await.map_err(|error| -> BenchError { Box::new(error) })?;
        send_result?;
    }
    let acknowledged_elapsed = start.elapsed();
    let throughput = total_messages as f64 / acknowledged_elapsed.as_secs_f64();

    wait_for_embeddings(&harness.state, &session_id, total_messages, config.timeout).await?;
    let p99_context_latency_ms = measure_p99_context_latency(
        &harness.client,
        &harness.base_url,
        &session_id,
        config.context_samples,
        config.max_tokens,
    )
    .await?;

    delete_session(&harness.client, &harness.base_url, &session_id).await?;

    Ok(IterationResult {
        throughput,
        p99_context_latency_ms,
    })
}

async fn create_session(client: &Client, base_url: &str) -> Result<String, BenchError> {
    let response = client.post(format!("{base_url}/sessions")).send().await?;
    response.error_for_status_ref()?;
    let body: CreateSessionResponse = response.json().await?;
    Ok(body.session_id)
}

async fn delete_session(
    client: &Client,
    base_url: &str,
    session_id: &str,
) -> Result<(), BenchError> {
    let response = client
        .delete(format!("{base_url}/sessions/{session_id}"))
        .send()
        .await?;
    response.error_for_status()?;
    Ok(())
}

async fn wait_for_server(
    client: &Client,
    base_url: &str,
    timeout: Duration,
) -> Result<(), BenchError> {
    let deadline = Instant::now() + timeout;

    loop {
        match client.get(format!("{base_url}/health")).send().await {
            Ok(response) if response.status() == StatusCode::OK => return Ok(()),
            _ if Instant::now() < deadline => tokio::time::sleep(Duration::from_millis(25)).await,
            _ => return Err(bench_error("benchmark server did not become ready in time")),
        }
    }
}

async fn wait_for_embeddings(
    state: &Arc<AppState>,
    session_id: &str,
    expected_messages: usize,
    timeout: Duration,
) -> Result<(), BenchError> {
    let deadline = Instant::now() + timeout;

    loop {
        let messages = state
            .short_term_memory
            .get_recent(session_id, expected_messages)
            .await?;

        let completed = messages
            .iter()
            .filter(|message| matches!(message.embedding_status, Some(EmbeddingStatus::Completed)))
            .count();
        let failed = messages
            .iter()
            .filter(|message| matches!(message.embedding_status, Some(EmbeddingStatus::Failed(_))))
            .count();

        if failed > 0 {
            let first_error = messages.iter().find_map(|message| match &message.embedding_status {
                Some(EmbeddingStatus::Failed(error)) => Some(error.as_str()),
                _ => None,
            });
            return Err(bench_error(format!(
                "{} embedding jobs failed{}",
                failed,
                first_error
                    .map(|error| format!(": {error}"))
                    .unwrap_or_default()
            )));
        }

        if messages.len() == expected_messages && completed == expected_messages {
            return Ok(());
        }

        if Instant::now() >= deadline {
            let queue_size = state.metrics.embedding_queue_size();
            return Err(bench_error(format!(
                "timed out waiting for embeddings: completed {} / {}, queue size {}",
                completed, expected_messages, queue_size
            )));
        }

        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn measure_p99_context_latency(
    client: &Client,
    base_url: &str,
    session_id: &str,
    samples: usize,
    max_tokens: usize,
) -> Result<f64, BenchError> {
    let mut latencies = Vec::with_capacity(samples);
    let url = format!(
        "{base_url}/sessions/{session_id}/context?max_tokens={max_tokens}&similarity_threshold={DEFAULT_SIMILARITY_THRESHOLD}&long_term_top_k={DEFAULT_LONG_TERM_TOP_K}"
    );

    for _ in 0..samples {
        let start = Instant::now();
        let response = client.get(&url).send().await?;
        response.error_for_status()?;
        latencies.push(start.elapsed().as_secs_f64() * 1_000.0);
    }

    latencies.sort_by(f64::total_cmp);
    Ok(latencies[percentile_index(latencies.len(), 0.99)])
}

fn percentile_index(len: usize, percentile: f64) -> usize {
    if len == 0 {
        return 0;
    }

    ((len as f64 * percentile).ceil() as usize)
        .saturating_sub(1)
        .min(len - 1)
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    let mid = values.len() / 2;

    if values.len() % 2 == 0 {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn bench_error(message: impl Into<String>) -> BenchError {
    Box::new(std::io::Error::other(message.into()))
}
