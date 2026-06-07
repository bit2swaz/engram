use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::task::JoinHandle;

use crate::knowledge::extractor::KnowledgeExtractor;
use crate::knowledge::graph::KnowledgeGraph;
use crate::knowledge::types::KnowledgeJob;
use crate::metrics::AppMetrics;
use crate::raft::types::RaftHandle;

pub fn knowledge_job_channel(capacity: usize) -> (mpsc::Sender<KnowledgeJob>, mpsc::Receiver<KnowledgeJob>) {
    mpsc::channel(capacity.max(1))
}

pub fn spawn_knowledge_workers(
    extractor: Arc<dyn KnowledgeExtractor>,
    raft: Option<Arc<RaftHandle>>,
    node_id: u64,
    knowledge_graph: Arc<RwLock<KnowledgeGraph>>,
    metrics: Arc<AppMetrics>,
    receiver: mpsc::Receiver<KnowledgeJob>,
    worker_count: usize,
) -> Vec<JoinHandle<()>> {
    let shared_receiver = Arc::new(Mutex::new(receiver));

    (0..worker_count.max(1))
        .map(|_| {
            let extractor       = extractor.clone();
            let raft            = raft.clone();
            let knowledge_graph = knowledge_graph.clone();
            let metrics         = metrics.clone();
            let shared_receiver = shared_receiver.clone();

            tokio::spawn(async move {
                worker_loop(extractor, raft, node_id, knowledge_graph, metrics, shared_receiver).await;
            })
        })
        .collect()
}

async fn worker_loop(
    extractor: Arc<dyn KnowledgeExtractor>,
    raft: Option<Arc<RaftHandle>>,
    node_id: u64,
    knowledge_graph: Arc<RwLock<KnowledgeGraph>>,
    metrics: Arc<AppMetrics>,
    receiver: Arc<Mutex<mpsc::Receiver<KnowledgeJob>>>,
) {
    loop {
        let (job, queue_size) = {
            let mut rx = receiver.lock().await;
            let job = rx.recv().await;
            let queue_size = rx.len();
            (job, queue_size)
        };

        metrics.set_knowledge_queue_size(queue_size);

        let Some(job) = job else { break };

        process_knowledge_job(job, &extractor, &raft, node_id, &knowledge_graph, &metrics).await;
    }
}

#[tracing::instrument(
    skip_all,
    fields(session_id = %job.session_id, message_id = %job.message_id)
)]
async fn process_knowledge_job(
    job: KnowledgeJob,
    extractor: &Arc<dyn KnowledgeExtractor>,
    raft: &Option<Arc<RaftHandle>>,
    node_id: u64,
    knowledge_graph: &Arc<RwLock<KnowledgeGraph>>,
    metrics: &AppMetrics,
) {
    // Leader only extraction: only the current Raft leader calls the extractor.
    // Followers skip because they receive AddKnowledge via Raft replication instead.
    // In standalone mode (raft is None), always extract and apply directly.
    //
    // Leader change safety: if this node loses leadership while the HTTP request
    // is in-flight, client_write() will be rejected by Raft. The result is
    // discarded. This check only avoids spending tokens on a write that will
    // almost certainly be rejected anyway.
    if let Some(raft) = raft {
        let current_leader = raft.metrics().borrow().current_leader;
        if current_leader != Some(node_id) {
            tracing::debug!("skipping knowledge extraction: not leader");
            return;
        }
    }

    // Dedup: skip if already processed (guards against replayed jobs).
    if knowledge_graph.read().await.is_processed(&job.session_id, &job.message_id) {
        tracing::debug!("skipping knowledge extraction: already processed");
        return;
    }

    let timer = metrics.start_knowledge_extraction_timer();

    let result = match extractor.extract(&job.text).await {
        Ok(r) => r,
        Err(e) => {
            drop(timer);
            tracing::error!(error = %e, "knowledge extraction failed");
            return;
        }
    };

    let entity_count       = result.entities.len() as u64;
    let relationship_count = result.relationships.len() as u64;
    drop(timer);

    metrics.increment_knowledge_entities(entity_count);
    metrics.increment_knowledge_relationships(relationship_count);

    tracing::info!(entities = entity_count, relationships = relationship_count, "extraction complete");

    let cmd = crate::raft::types::MemoryCommand::AddKnowledge {
        session_id:    job.session_id.clone(),
        message_id:    job.message_id.clone(),
        entities:      result.entities,
        relationships: result.relationships,
    };

    match raft {
        Some(raft) => {
            if let Err(e) = raft.client_write(cmd).await {
                tracing::error!(error = %e, "failed to submit AddKnowledge via Raft");
            }
        }
        None => {
            if let crate::raft::types::MemoryCommand::AddKnowledge {
                session_id, message_id, entities, relationships,
            } = cmd
            {
                knowledge_graph
                    .write()
                    .await
                    .apply_extraction(&session_id, &message_id, entities, relationships);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::knowledge::extractor::{ExtractError, KnowledgeExtractor};
    use crate::knowledge::graph::KnowledgeGraph;
    use crate::knowledge::types::{Entity, ExtractionResult, KnowledgeJob, Relationship};
    use crate::metrics::AppMetrics;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::{RwLock, mpsc};

    struct MockExtractor {
        result: ExtractionResult,
        call_count: Arc<tokio::sync::Mutex<u32>>,
    }

    #[async_trait]
    impl KnowledgeExtractor for MockExtractor {
        async fn extract(&self, _text: &str) -> Result<ExtractionResult, ExtractError> {
            *self.call_count.lock().await += 1;
            Ok(self.result.clone())
        }
    }

    struct FailingExtractor;

    #[async_trait]
    impl KnowledgeExtractor for FailingExtractor {
        async fn extract(&self, _text: &str) -> Result<ExtractionResult, ExtractError> {
            Err(ExtractError::Api("service unavailable".to_string()))
        }
    }

    fn make_result() -> ExtractionResult {
        ExtractionResult {
            entities: vec![
                Entity { name: "Alice".into(), entity_type: "Person".into(), attributes: HashMap::new() },
                Entity { name: "OpenAI".into(), entity_type: "Organization".into(), attributes: HashMap::new() },
            ],
            relationships: vec![
                Relationship { from: "Alice".into(), to: "OpenAI".into(), relationship_type: "works_at".into() },
            ],
        }
    }

    #[tokio::test]
    async fn standalone_mode_applies_directly_to_graph() {
        let call_count = Arc::new(tokio::sync::Mutex::new(0u32));
        let extractor: Arc<dyn KnowledgeExtractor> = Arc::new(MockExtractor {
            result: make_result(),
            call_count: call_count.clone(),
        });
        let kg = Arc::new(RwLock::new(KnowledgeGraph::new()));
        let metrics = Arc::new(AppMetrics::new().unwrap());
        let (tx, rx) = mpsc::channel(10);

        spawn_knowledge_workers(extractor, None, 0, kg.clone(), metrics, rx, 1);

        tx.send(KnowledgeJob { session_id: "s1".into(), message_id: "m1".into(), text: "Alice works at OpenAI".into() })
            .await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        assert_eq!(*call_count.lock().await, 1);
        let graph = kg.read().await;
        assert_eq!(graph.all_entities("s1").len(), 2);
    }

    #[tokio::test]
    async fn dedup_skips_extractor_if_already_processed() {
        let call_count = Arc::new(tokio::sync::Mutex::new(0u32));
        let extractor: Arc<dyn KnowledgeExtractor> = Arc::new(MockExtractor {
            result: make_result(),
            call_count: call_count.clone(),
        });
        let kg = Arc::new(RwLock::new(KnowledgeGraph::new()));
        let metrics = Arc::new(AppMetrics::new().unwrap());
        let (tx, rx) = mpsc::channel(10);

        spawn_knowledge_workers(extractor, None, 0, kg.clone(), metrics, rx, 1);

        let job = KnowledgeJob { session_id: "s1".into(), message_id: "m1".into(), text: "test".into() };
        tx.send(job.clone()).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        tx.send(job).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        assert_eq!(*call_count.lock().await, 1, "extractor should only be called once");
    }

    #[tokio::test]
    async fn extractor_failure_does_not_panic() {
        let extractor: Arc<dyn KnowledgeExtractor> = Arc::new(FailingExtractor);
        let kg = Arc::new(RwLock::new(KnowledgeGraph::new()));
        let metrics = Arc::new(AppMetrics::new().unwrap());
        let (tx, rx) = mpsc::channel(10);

        spawn_knowledge_workers(extractor, None, 0, kg.clone(), metrics, rx, 1);

        tx.send(KnowledgeJob { session_id: "s1".into(), message_id: "m1".into(), text: "test".into() })
            .await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        assert!(kg.read().await.all_entities("s1").is_empty());
    }
}
