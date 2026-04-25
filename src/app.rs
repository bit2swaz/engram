use std::error::Error as StdError;
use std::sync::Arc;

use thiserror::Error;

use crate::assembler::ContextAssembler;
use crate::config::Config;
use crate::core::{
    EmbedError, EmbeddingProvider, MemoryError, OpenAITokenCounter, ShortTermMemory, StoreError,
    TokenCounter, VectorStore,
};
use crate::embedding::OpenAIEmbedder;
use crate::server::AppState;
use crate::stores::{LanceDBStore, RedisCoreMemoryStore, RedisShortTermMemory};
use crate::worker::{embedding_job_channel, spawn_embedding_workers};

#[derive(Debug, Error)]
pub enum AppBuildError {
    #[error(transparent)]
    Embed(#[from] EmbedError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Memory(#[from] MemoryError),
    #[error(transparent)]
    Other(#[from] Box<dyn StdError + Send + Sync + 'static>),
}

pub async fn build_real_app_state(config: &Config) -> Result<Arc<AppState>, AppBuildError> {
    let embedding_provider: Arc<dyn EmbeddingProvider> =
        Arc::new(OpenAIEmbedder::new_with_api_key(config.openai_api_key.clone())?);

    build_app_state_with_embedding_provider(config, embedding_provider).await
}

pub async fn build_app_state_with_embedding_provider(
    config: &Config,
    embedding_provider: Arc<dyn EmbeddingProvider>,
) -> Result<Arc<AppState>, AppBuildError> {
    let short_term_memory: Arc<dyn ShortTermMemory> =
        Arc::new(RedisShortTermMemory::connect(&config.redis_url).await?);
    let vector_store: Arc<dyn VectorStore> =
        Arc::new(LanceDBStore::connect(&config.lance_db_path).await?);
    let token_counter: Arc<dyn TokenCounter> = Arc::new(OpenAITokenCounter::new()?);
    let core_memory_store = Arc::new(RedisCoreMemoryStore::connect(&config.redis_url).await?);
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
        receiver,
        config.embedding_max_concurrency,
    );

    Ok(Arc::new(AppState {
        short_term_memory,
        vector_store,
        embedding_provider,
        token_counter,
        core_memory_store,
        context_assembler,
        embedding_job_sender,
        short_term_count: config.short_term_count,
    }))
}