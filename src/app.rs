use std::error::Error as StdError;
use std::sync::Arc;

use prometheus::Error as PrometheusError;
use thiserror::Error;

use crate::assembler::ContextAssembler;
use crate::config::Config;
use crate::core::{
    CoreMemoryStore, EmbedError, EmbeddingProvider, MemoryError, OpenAITokenCounter, ShortTermMemory,
    StoreError, TokenCounter, VectorStore,
};
use crate::embedding::OpenAIEmbedder;
use crate::config::KnowledgeExtractorType;
use crate::knowledge::extractor::{MockKnowledgeExtractor, OpenAIKnowledgeExtractor};
use crate::knowledge::graph::KnowledgeGraph;
use crate::knowledge::worker::{knowledge_job_channel, spawn_knowledge_workers};
use crate::metrics::AppMetrics;
use crate::server::AppState;
use crate::stores::{LanceDBStore, RedisCoreMemoryStore, RedisShortTermMemory};
use crate::worker::{EmbeddingJob, embedding_job_channel, spawn_embedding_workers};

#[derive(Debug, Error)]
pub enum AppBuildError {
    #[error(transparent)]
    Embed(#[from] EmbedError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Memory(#[from] MemoryError),
    #[error(transparent)]
    Metrics(#[from] PrometheusError),
    #[error(transparent)]
    Other(#[from] Box<dyn StdError + Send + Sync + 'static>),
}

pub async fn build_raft_node(
    config: &Config,
    short_term: Arc<dyn ShortTermMemory>,
    core_memory: Arc<dyn CoreMemoryStore>,
    vector_store: Arc<dyn VectorStore>,
    embedding_tx: tokio::sync::mpsc::Sender<EmbeddingJob>,
    knowledge_graph: Arc<tokio::sync::RwLock<crate::knowledge::graph::KnowledgeGraph>>,
    knowledge_tx: tokio::sync::mpsc::Sender<crate::knowledge::types::KnowledgeJob>,
) -> anyhow::Result<Arc<crate::raft::types::RaftHandle>> {
    use crate::raft::{
        log_store::EngRaftLogStore, network::EngRaftNetwork,
        state_machine::EngStateMachineStore, types::TypeConfig,
    };

    let node_id = config
        .node_id
        .expect("NODE_ID must be set in cluster mode");

    let raft_config = Arc::new(
        openraft::Config {
            heartbeat_interval: 250,
            election_timeout_min: 299,
            election_timeout_max: 500,
            ..Default::default()
        }
        .validate()?,
    );

    if let Some(parent) = config.raft_db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let db = Arc::new(redb::Database::create(&config.raft_db_path)?);

    let raft = openraft::Raft::<TypeConfig>::new(
        node_id,
        raft_config,
        EngRaftNetwork,
        EngRaftLogStore::new(db),
        EngStateMachineStore::new(short_term, core_memory, vector_store, embedding_tx, knowledge_graph, knowledge_tx),
    )
    .await?;

    Ok(Arc::new(raft))
}

/// Spawns a background task that watches the Raft metrics channel and updates
/// Prometheus gauges. Must be called after the Raft node is initialized.
pub fn spawn_raft_metrics_watcher(
    raft: Arc<crate::raft::types::RaftHandle>,
    metrics: Arc<AppMetrics>,
) {
    tokio::spawn(async move {
        let mut rx = raft.metrics();
        let mut last_leader: Option<u64> = None;
        loop {
            if rx.changed().await.is_err() {
                break;
            }
            let m = rx.borrow().clone();
            metrics.raft_term.set(m.current_term as i64);
            if let Some(applied) = &m.last_applied {
                metrics.raft_commit_index.set(applied.index as i64);
            }
            metrics.raft_is_leader.set((m.current_leader == Some(m.id)) as i64);
            if m.current_leader != last_leader {
                metrics.raft_leader_changes_total.inc();
                last_leader = m.current_leader;
            }
        }
    });
}

pub async fn build_real_app_state(config: &Config) -> Result<Arc<AppState>, AppBuildError> {
    let embedding_provider: Arc<dyn EmbeddingProvider> = if config.openai_api_key.is_empty() {
        Arc::new(crate::core::RandomEmbeddingProvider)
    } else {
        match &config.openai_base_url {
            Some(base_url) => Arc::new(OpenAIEmbedder::new_with_base_url(
                config.openai_api_key.clone(),
                base_url.clone(),
            )?),
            None => Arc::new(OpenAIEmbedder::new_with_api_key(config.openai_api_key.clone())?),
        }
    };

    build_app_state_with_embedding_provider(config, embedding_provider).await
}

pub async fn build_app_state_with_embedding_provider(
    config: &Config,
    embedding_provider: Arc<dyn EmbeddingProvider>,
) -> Result<Arc<AppState>, AppBuildError> {
    let short_term_memory: Arc<dyn ShortTermMemory> =
        Arc::new(RedisShortTermMemory::connect(&config.redis_url).await?);
    let vector_store: Arc<dyn VectorStore> =
        Arc::new(LanceDBStore::connect(&config.lance_db_path, config.embedding_dimension).await?);
    let token_counter: Arc<dyn TokenCounter> = Arc::new(OpenAITokenCounter::new()?);
    let core_memory_store: Arc<dyn CoreMemoryStore> =
        Arc::new(RedisCoreMemoryStore::connect(&config.redis_url).await?);
    let metrics = Arc::new(AppMetrics::new()?);
    let context_assembler = Arc::new(ContextAssembler::new(
        short_term_memory.clone(),
        vector_store.clone(),
        embedding_provider.clone(),
        token_counter.clone(),
        core_memory_store.clone(),
    ));
    let (embedding_job_sender, receiver) = embedding_job_channel(config.mpsc_channel_size);
    let _worker_handles = spawn_embedding_workers(
        short_term_memory.clone(),
        vector_store.clone(),
        embedding_provider.clone(),
        metrics.clone(),
        receiver,
        config.embedding_max_concurrency,
    );

    let knowledge_graph = Arc::new(tokio::sync::RwLock::new(KnowledgeGraph::new()));
    let (knowledge_job_sender, knowledge_receiver) = knowledge_job_channel(config.knowledge_channel_size);

    let knowledge_extractor: Arc<dyn crate::knowledge::extractor::KnowledgeExtractor> =
        match config.knowledge_extractor {
            KnowledgeExtractorType::Mock => Arc::new(MockKnowledgeExtractor),
            KnowledgeExtractorType::OpenAI => match &config.openai_base_url {
                Some(base_url) => Arc::new(OpenAIKnowledgeExtractor::new_with_base_url(
                    config.openai_api_key.clone(),
                    base_url.clone(),
                )),
                None => Arc::new(OpenAIKnowledgeExtractor::new(config.openai_api_key.clone())),
            },
        };

    let (raft, node_id, peer_http_addrs, raft_addr, raft_advertise_addr, cluster_peers) = if config.node_id.is_some() {
        let raft = build_raft_node(
            config,
            short_term_memory.clone(),
            core_memory_store.clone(),
            vector_store.clone(),
            embedding_job_sender.clone(),
            knowledge_graph.clone(),
            knowledge_job_sender.clone(),
        )
        .await
        .map_err(|e| AppBuildError::Other(e.into()))?;
        let node_id = config.node_id.unwrap();
        let peer_http_addrs = config.cluster_http_peers.clone();
        let raft_addr = config.raft_addr.clone();
        let raft_advertise_addr = config.raft_advertise_addr.clone();
        let cluster_peers = config.cluster_peers.clone();
        (Some(raft), node_id, peer_http_addrs, raft_addr, raft_advertise_addr, cluster_peers)
    } else {
        (None, 0u64, std::collections::HashMap::new(), None, None, vec![])
    };

    if let Some(raft_handle) = &raft {
        spawn_raft_metrics_watcher(raft_handle.clone(), metrics.clone());
    }

    let _knowledge_worker_handles = spawn_knowledge_workers(
        knowledge_extractor,
        raft.clone(),
        config.node_id.unwrap_or(0),
        knowledge_graph.clone(),
        metrics.clone(),
        knowledge_receiver,
        config.knowledge_max_workers,
    );

    Ok(Arc::new(AppState {
        short_term_memory,
        vector_store,
        embedding_provider,
        token_counter,
        core_memory_store,
        context_assembler,
        metrics,
        embedding_job_sender,
        short_term_count: config.short_term_count,
        raft,
        node_id,
        peer_http_addrs,
        raft_addr,
        raft_advertise_addr,
        cluster_peers,
        knowledge_graph,
        knowledge_job_sender,
    }))
}
