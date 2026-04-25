use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use chrono::Utc;
use engram::core::{
    EmbedError, EmbeddingProvider, InMemoryStore, InMemoryVectorStore, ShortTermMemory,
    VectorStore,
};
use engram::metrics::AppMetrics;
use engram::models::{EmbeddingStatus, Message};
use engram::worker::{EmbeddingJob, embedding_job_channel, spawn_embedding_workers};
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::{Duration, sleep};

struct MockEmbeddingProvider {
    call_count: AtomicUsize,
    error_message: Mutex<Option<String>>,
}

impl MockEmbeddingProvider {
    fn successful() -> Self {
        Self {
            call_count: AtomicUsize::new(0),
            error_message: Mutex::new(None),
        }
    }

    fn failing(message: &str) -> Self {
        Self {
            call_count: AtomicUsize::new(0),
            error_message: Mutex::new(Some(message.to_string())),
        }
    }

    fn call_count(&self) -> usize {
        self.call_count.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl EmbeddingProvider for MockEmbeddingProvider {
    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);

        if let Some(message) = self.error_message.lock().unwrap().clone() {
            return Err(EmbedError::Message(message));
        }

        Ok(texts.iter().map(|_| vec![1.0; 1536]).collect())
    }
}

fn pending_message(message_id: &str, text: &str) -> Message {
    Message {
        id: Some(message_id.to_string()),
        role: "user".to_string(),
        content: text.to_string(),
        timestamp: Some(Utc::now()),
        embedding_status: Some(EmbeddingStatus::Pending),
    }
}

async fn wait_for_message(
    store: &dyn ShortTermMemory,
    session_id: &str,
    message_id: &str,
) -> Message {
    for _ in 0..50 {
        if let Some(message) = store.get_message_by_id(session_id, message_id).await.unwrap() {
            match message.embedding_status.as_ref() {
                Some(EmbeddingStatus::Completed) | Some(EmbeddingStatus::Failed(_)) => {
                    return message;
                }
                _ => {}
            }
        }

        sleep(Duration::from_millis(10)).await;
    }

    panic!("message status did not reach a terminal state in time");
}

#[tokio::test]
async fn worker_processes_job_and_marks_message_completed() {
    let short_term_memory = Arc::new(InMemoryStore::default());
    let vector_store = Arc::new(InMemoryVectorStore::default());
    let embedding_provider = Arc::new(MockEmbeddingProvider::successful());
    let metrics = Arc::new(AppMetrics::new().unwrap());
    let (sender, receiver) = embedding_job_channel(8);

    let workers = spawn_embedding_workers(
        short_term_memory.clone(),
        vector_store.clone(),
        embedding_provider.clone(),
        metrics,
        receiver,
        1,
    );

    short_term_memory
        .add_message("session-1", pending_message("message-1", "hello memory"))
        .await
        .unwrap();

    sender
        .send(EmbeddingJob::new("session-1", "message-1", "hello memory"))
        .await
        .unwrap();

    let message = wait_for_message(short_term_memory.as_ref(), "session-1", "message-1").await;
    assert!(matches!(
        message.embedding_status,
        Some(EmbeddingStatus::Completed)
    ));

    let results = vector_store.search("session-1", &[1.0; 1536], 5).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].text, "hello memory");
    assert_eq!(embedding_provider.call_count(), 1);

    drop(sender);
    for worker in workers {
        worker.abort();
    }
}

#[tokio::test]
async fn worker_skips_duplicate_job_after_completion() {
    let short_term_memory = Arc::new(InMemoryStore::default());
    let vector_store = Arc::new(InMemoryVectorStore::default());
    let embedding_provider = Arc::new(MockEmbeddingProvider::successful());
    let metrics = Arc::new(AppMetrics::new().unwrap());
    let (sender, receiver) = embedding_job_channel(8);

    let workers = spawn_embedding_workers(
        short_term_memory.clone(),
        vector_store,
        embedding_provider.clone(),
        metrics,
        receiver,
        1,
    );

    short_term_memory
        .add_message("session-dup", pending_message("dup", "remember me"))
        .await
        .unwrap();

    sender
        .send(EmbeddingJob::new("session-dup", "dup", "remember me"))
        .await
        .unwrap();
    let _ = wait_for_message(short_term_memory.as_ref(), "session-dup", "dup").await;

    sender
        .send(EmbeddingJob::new("session-dup", "dup", "remember me"))
        .await
        .unwrap();
    sleep(Duration::from_millis(50)).await;

    assert_eq!(embedding_provider.call_count(), 1);

    drop(sender);
    for worker in workers {
        worker.abort();
    }
}

#[tokio::test]
async fn bounded_channel_reports_backpressure_when_full() {
    let (sender, _receiver) = embedding_job_channel(1);

    sender
        .try_send(EmbeddingJob::new("session-1", "message-1", "one"))
        .unwrap();

    let error = sender
        .try_send(EmbeddingJob::new("session-1", "message-2", "two"))
        .unwrap_err();

    assert!(matches!(error, TrySendError::Full(_)));
}

#[tokio::test]
async fn worker_marks_message_failed_when_embedding_errors() {
    let short_term_memory = Arc::new(InMemoryStore::default());
    let vector_store = Arc::new(InMemoryVectorStore::default());
    let embedding_provider = Arc::new(MockEmbeddingProvider::failing("embed failed"));
    let metrics = Arc::new(AppMetrics::new().unwrap());
    let (sender, receiver) = embedding_job_channel(8);

    let workers = spawn_embedding_workers(
        short_term_memory.clone(),
        vector_store.clone(),
        embedding_provider.clone(),
        metrics,
        receiver,
        1,
    );

    short_term_memory
        .add_message("session-err", pending_message("message-err", "oops"))
        .await
        .unwrap();

    sender
        .send(EmbeddingJob::new("session-err", "message-err", "oops"))
        .await
        .unwrap();

    let message = wait_for_message(short_term_memory.as_ref(), "session-err", "message-err").await;
    match message.embedding_status {
        Some(EmbeddingStatus::Failed(error_message)) => {
            assert!(error_message.contains("embed failed"));
        }
        other => panic!("expected failed status, got {other:?}"),
    }

    let results = vector_store.search("session-err", &[1.0; 1536], 5).await.unwrap();
    assert!(results.is_empty());
    assert_eq!(embedding_provider.call_count(), 1);

    drop(sender);
    for worker in workers {
        worker.abort();
    }
}