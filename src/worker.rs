use std::env;
use std::sync::Arc;

use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;

use crate::core::{EmbeddingProvider, ShortTermMemory, VectorStore};
use crate::models::EmbeddingStatus;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingJob {
    session_id: String,
    message_id: String,
    text: String,
}

impl EmbeddingJob {
    pub fn new(
        session_id: impl Into<String>,
        message_id: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            message_id: message_id.into(),
            text: text.into(),
        }
    }
}

pub fn default_channel_size() -> usize {
    env::var("MPSC_CHANNEL_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(1_000)
}

pub fn default_worker_count() -> usize {
    env::var("EMBEDDING_MAX_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(10)
}

pub fn embedding_job_channel(
    capacity: usize,
) -> (mpsc::Sender<EmbeddingJob>, mpsc::Receiver<EmbeddingJob>) {
    mpsc::channel(capacity.max(1))
}

pub fn spawn_embedding_workers(
    short_term_memory: Arc<dyn ShortTermMemory>,
    vector_store: Arc<dyn VectorStore>,
    embedding_provider: Arc<dyn EmbeddingProvider>,
    receiver: mpsc::Receiver<EmbeddingJob>,
    worker_count: usize,
) -> Vec<JoinHandle<()>> {
    let shared_receiver = Arc::new(Mutex::new(receiver));

    (0..worker_count.max(1))
        .map(|_| {
            let shared_receiver = shared_receiver.clone();
            let short_term_memory = short_term_memory.clone();
            let vector_store = vector_store.clone();
            let embedding_provider = embedding_provider.clone();

            tokio::spawn(async move {
                worker_loop(
                    shared_receiver,
                    short_term_memory,
                    vector_store,
                    embedding_provider,
                )
                .await;
            })
        })
        .collect()
}

async fn worker_loop(
    receiver: Arc<Mutex<mpsc::Receiver<EmbeddingJob>>>,
    short_term_memory: Arc<dyn ShortTermMemory>,
    vector_store: Arc<dyn VectorStore>,
    embedding_provider: Arc<dyn EmbeddingProvider>,
) {
    loop {
        let job = {
            let mut receiver = receiver.lock().await;
            receiver.recv().await
        };

        let Some(job) = job else {
            break;
        };

        process_embedding_job(
            job,
            short_term_memory.as_ref(),
            vector_store.as_ref(),
            embedding_provider.as_ref(),
        )
        .await;
    }
}

async fn process_embedding_job(
    job: EmbeddingJob,
    short_term_memory: &dyn ShortTermMemory,
    vector_store: &dyn VectorStore,
    embedding_provider: &dyn EmbeddingProvider,
) {
    process_embedding_job_traced(job, short_term_memory, vector_store, embedding_provider).await;
}

#[tracing::instrument(
    skip_all,
    fields(
        session_id = %job.session_id,
        message_id = %job.message_id,
        text_len = job.text.len()
    )
)]
async fn process_embedding_job_traced(
    job: EmbeddingJob,
    short_term_memory: &dyn ShortTermMemory,
    vector_store: &dyn VectorStore,
    embedding_provider: &dyn EmbeddingProvider,
) {
    tracing::info!("processing embedding job");

    let current_message = match short_term_memory
        .get_message_by_id(&job.session_id, &job.message_id)
        .await
    {
        Ok(message) => message,
        Err(error) => {
            tracing::error!(error = %error, "failed to load message for embedding job");
            return;
        }
    };

    match current_message.and_then(|message| message.embedding_status) {
        Some(status @ EmbeddingStatus::Completed)
        | Some(status @ EmbeddingStatus::Processing) => {
            tracing::info!(status = ?status, "skipping embedding job due to existing terminal or active status");
            return;
        }
        _ => {}
    }

    if short_term_memory
        .update_message_status(
            &job.session_id,
            &job.message_id,
            EmbeddingStatus::Processing,
        )
        .await
        .is_err()
    {
        tracing::error!("failed to mark message as processing");
        return;
    }

    tracing::info!("marked message as processing");

    let embedding = match embedding_provider.embed(std::slice::from_ref(&job.text)).await {
        Ok(mut embeddings) => match embeddings.pop() {
            Some(embedding) => embedding,
            None => {
                tracing::error!("embedding provider returned no embeddings");
                let _ = short_term_memory
                    .update_message_status(
                        &job.session_id,
                        &job.message_id,
                        EmbeddingStatus::Failed(
                            "embedding provider returned no embeddings".to_string(),
                        ),
                    )
                    .await;
                return;
            }
        },
        Err(error) => {
            tracing::error!(error = %error, "embedding provider failed");
            let _ = short_term_memory
                .update_message_status(
                    &job.session_id,
                    &job.message_id,
                    EmbeddingStatus::Failed(error.to_string()),
                )
                .await;
            return;
        }
    };

    if let Err(error) = vector_store
        .insert(&job.session_id, &job.text, embedding, &job.message_id)
        .await
    {
        tracing::error!(error = %error, "failed to insert embedding into vector store");
        let _ = short_term_memory
            .update_message_status(
                &job.session_id,
                &job.message_id,
                EmbeddingStatus::Failed(error.to_string()),
            )
            .await;
        return;
    }

    if let Err(error) = short_term_memory
        .update_message_status(
            &job.session_id,
            &job.message_id,
            EmbeddingStatus::Completed,
        )
        .await
    {
        tracing::error!(error = %error, "failed to mark message as completed");
        return;
    }

    tracing::info!("completed embedding job");
}