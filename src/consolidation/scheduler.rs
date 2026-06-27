use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::consolidation::store::ConsolidatedMemoryStore;
use crate::core::ShortTermMemory;
use crate::knowledge::summarizer::{SUMMARIZE_PROMPT_VERSION, Summarizer};
use crate::metrics::AppMetrics;
use crate::raft::types::{MemoryCommand, RaftHandle};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsolidationJob {
    pub session_id: String,
}

pub fn should_consolidate(message_count: usize, threshold: usize) -> bool {
    message_count > threshold
}

pub fn consolidation_job_channel(
    capacity: usize,
) -> (mpsc::Sender<ConsolidationJob>, mpsc::Receiver<ConsolidationJob>) {
    mpsc::channel(capacity.max(1))
}

#[allow(clippy::too_many_arguments)]
pub fn spawn_consolidation_workers(
    summarizer: Arc<dyn Summarizer>,
    raft: Option<Arc<RaftHandle>>,
    node_id: u64,
    short_term: Arc<dyn ShortTermMemory>,
    consolidated: Arc<dyn ConsolidatedMemoryStore>,
    metrics: Arc<AppMetrics>,
    threshold: usize,
    target_window: usize,
    receiver: mpsc::Receiver<ConsolidationJob>,
    worker_count: usize,
) -> Vec<JoinHandle<()>> {
    let shared_rx = Arc::new(Mutex::new(receiver));
    let in_flight: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

    (0..worker_count.max(1))
        .map(|_| {
            let summarizer = summarizer.clone();
            let raft = raft.clone();
            let short_term = short_term.clone();
            let consolidated = consolidated.clone();
            let metrics = metrics.clone();
            let shared_rx = shared_rx.clone();
            let in_flight = in_flight.clone();
            tokio::spawn(async move {
                loop {
                    let (job, queue_size) = {
                        let mut rx = shared_rx.lock().await;
                        let job = rx.recv().await;
                        let n = rx.len();
                        (job, n)
                    };
                    metrics.set_consolidation_queue_size(queue_size);
                    let Some(job) = job else { break };
                    process_consolidation_job(
                        job,
                        &summarizer,
                        &raft,
                        node_id,
                        &short_term,
                        &consolidated,
                        &metrics,
                        threshold,
                        target_window,
                        &in_flight,
                    )
                    .await;
                }
            })
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
async fn process_consolidation_job(
    job: ConsolidationJob,
    summarizer: &Arc<dyn Summarizer>,
    raft: &Option<Arc<RaftHandle>>,
    node_id: u64,
    short_term: &Arc<dyn ShortTermMemory>,
    consolidated: &Arc<dyn ConsolidatedMemoryStore>,
    metrics: &Arc<AppMetrics>,
    threshold: usize,
    target_window: usize,
    in_flight: &Arc<Mutex<HashSet<String>>>,
) {
    // Only the current leader summarizes. Followers get the result through
    // ApplySummary replication, so they bail out here. Standalone (no raft)
    // always proceeds.
    if let Some(raft) = raft {
        if raft.metrics().borrow().current_leader != Some(node_id) {
            return;
        }
    }

    // At most one consolidation per session at a time. If insert returns false
    // the session is already being consolidated, so drop this duplicate job.
    {
        let mut guard = in_flight.lock().await;
        if !guard.insert(job.session_id.clone()) {
            return;
        }
    }
    // Clear the guard on the way out no matter how we leave this function.
    let _cleanup = ClearOnDrop {
        set: in_flight.clone(),
        key: job.session_id.clone(),
    };

    let messages = match short_term.get_recent(&job.session_id, usize::MAX).await {
        Ok(m) => m,
        Err(e) => {
            tracing::error!(error = %e, "consolidation: failed to read messages");
            return;
        }
    };
    if !should_consolidate(messages.len(), threshold) {
        return;
    }
    // Summarize everything except the newest `target_window` messages, driving
    // the session back down to exactly the window.
    let cut = messages.len().saturating_sub(target_window);
    let to_summarize = &messages[..cut];
    let consumed_ids: Vec<String> = to_summarize.iter().filter_map(|m| m.id.clone()).collect();
    if consumed_ids.is_empty() {
        return;
    }

    let timer = metrics.start_summarization_timer(summarizer.model());
    let summary_text = match summarizer.summarize(to_summarize).await {
        Ok(t) => t,
        Err(e) => {
            drop(timer);
            tracing::error!(error = %e, "consolidation: summarize failed");
            return;
        }
    };
    drop(timer);

    let cmd = MemoryCommand::ApplySummary {
        session_id: job.session_id.clone(),
        summary_id: Uuid::new_v4().to_string(),
        summary_text,
        consumed_message_ids: consumed_ids,
        model: summarizer.model().to_string(),
        prompt_version: SUMMARIZE_PROMPT_VERSION.to_string(),
    };

    match raft {
        Some(raft) => {
            if let Err(e) = raft.client_write(cmd).await {
                tracing::error!(error = %e, "consolidation: client_write failed");
            }
        }
        None => {
            // Standalone: apply straight to the stores, mirroring the knowledge
            // worker's None branch.
            if let MemoryCommand::ApplySummary {
                session_id,
                summary_id,
                summary_text,
                consumed_message_ids,
                model,
                prompt_version,
            } = cmd
            {
                let summary = crate::consolidation::store::Summary {
                    id: summary_id,
                    text: summary_text,
                    created_at_index: 0,
                    consumed_count: consumed_message_ids.len() as u64,
                    consumed_message_ids: consumed_message_ids.clone(),
                    model,
                    prompt_version,
                };
                let _ = consolidated.add_summary(&session_id, summary).await;
                let _ = short_term.remove_messages(&session_id, &consumed_message_ids).await;
                metrics.increment_consolidations();
                metrics.increment_messages_consolidated(consumed_message_ids.len() as u64);
            }
        }
    }
}

struct ClearOnDrop {
    set: Arc<Mutex<HashSet<String>>>,
    key: String,
}
impl Drop for ClearOnDrop {
    fn drop(&mut self) {
        // Clear synchronously if the lock is free; otherwise hand it to a task.
        if let Ok(mut g) = self.set.try_lock() {
            g.remove(&self.key);
        } else {
            let set = self.set.clone();
            let key = self.key.clone();
            tokio::spawn(async move {
                set.lock().await.remove(&key);
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consolidation::store::InMemoryConsolidatedStore;
    use crate::core::InMemoryStore;
    use crate::knowledge::summarizer::MockSummarizer;
    use crate::metrics::AppMetrics;
    use crate::models::Message;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    fn msg(id: &str) -> Message {
        Message {
            id: Some(id.into()),
            role: "user".into(),
            content: format!("content {id}"),
            timestamp: None,
            embedding_status: None,
        }
    }

    #[test]
    fn should_consolidate_threshold() {
        assert!(!should_consolidate(50, 50));
        assert!(should_consolidate(51, 50));
    }

    #[tokio::test]
    async fn standalone_consolidates_oldest_and_keeps_window() {
        // raft = None (standalone): the worker summarizes directly and applies via the store path.
        let short_term = Arc::new(InMemoryStore::default());
        for i in 0..6 {
            short_term.add_message("s1", msg(&format!("m{i}"))).await.unwrap();
        }
        let consolidated = Arc::new(InMemoryConsolidatedStore::default());
        let summarizer: Arc<dyn Summarizer> = Arc::new(MockSummarizer);
        let metrics = Arc::new(AppMetrics::new().unwrap());
        let (tx, rx) = mpsc::channel(10);

        // threshold 4, window 2 -> with 6 messages, summarize oldest 4, keep newest 2.
        spawn_consolidation_workers(summarizer, None, 0, short_term.clone(), consolidated.clone(), metrics, 4, 2, rx, 1);
        tx.send(ConsolidationJob { session_id: "s1".into() }).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

        assert_eq!(consolidated.get_summaries("s1").await.unwrap().len(), 1);
        let remaining = short_term.get_recent("s1", 10).await.unwrap();
        assert_eq!(remaining.len(), 2, "keeps the target window");
        assert_eq!(remaining[1].id.as_deref(), Some("m5"));
    }

    #[tokio::test]
    async fn in_flight_guard_prevents_duplicate_jobs() {
        let short_term = Arc::new(InMemoryStore::default());
        for i in 0..6 {
            short_term.add_message("s1", msg(&format!("m{i}"))).await.unwrap();
        }
        let consolidated = Arc::new(InMemoryConsolidatedStore::default());
        let summarizer: Arc<dyn Summarizer> = Arc::new(MockSummarizer);
        let metrics = Arc::new(AppMetrics::new().unwrap());
        let (tx, rx) = mpsc::channel(10);
        spawn_consolidation_workers(summarizer, None, 0, short_term.clone(), consolidated.clone(), metrics, 4, 2, rx, 1);

        // Two rapid jobs for the same session: only one summary should result.
        tx.send(ConsolidationJob { session_id: "s1".into() }).await.unwrap();
        tx.send(ConsolidationJob { session_id: "s1".into() }).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        assert_eq!(consolidated.get_summaries("s1").await.unwrap().len(), 1, "guard dedups concurrent jobs");
    }
}
