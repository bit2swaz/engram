use std::io;
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use openraft::{
    BasicNode, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, Snapshot, SnapshotMeta,
    StorageError, StoredMembership, RaftSnapshotBuilder,
    storage::RaftStateMachine,
};

use crate::core::{CoreMemoryStore, ShortTermMemory};
use crate::models::{EmbeddingStatus, Message};
use crate::raft::types::{CommandResponse, MemoryCommand, TypeConfig};
use crate::worker::EmbeddingJob;

pub struct EngStateMachineStore {
    inner: Arc<Mutex<SmInner>>,
}

struct SmInner {
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
    short_term: Arc<dyn ShortTermMemory>,
    core_memory: Arc<dyn CoreMemoryStore>,
    embedding_tx: mpsc::Sender<EmbeddingJob>,
}

impl EngStateMachineStore {
    pub fn new(
        short_term: Arc<dyn ShortTermMemory>,
        core_memory: Arc<dyn CoreMemoryStore>,
        // vector_store is not stored here. each node's embedding worker owns
        // the LanceDB handle and handles deletes via the EmbeddingJob channel.
        _vector_store: Arc<dyn crate::core::VectorStore>,
        embedding_tx: mpsc::Sender<EmbeddingJob>,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SmInner {
                last_applied: None,
                last_membership: StoredMembership::default(),
                short_term,
                core_memory,
                embedding_tx,
            })),
        }
    }
}

impl RaftStateMachine<TypeConfig> for EngStateMachineStore {
    // Snapshot not implemented in Stage 1. Nodes that fall too far behind must
    // be manually removed and re-added to the cluster. Stage 2 adds compaction.
    type SnapshotBuilder = NoOpSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let inner = self.inner.lock().await;
        Ok((inner.last_applied.clone(), inner.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<CommandResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        // Clone Arcs once so the lock is not held across async apply_cmd calls.
        let (short_term, core_memory, embedding_tx) = {
            let inner = self.inner.lock().await;
            (inner.short_term.clone(), inner.core_memory.clone(), inner.embedding_tx.clone())
        };

        let mut responses = Vec::new();
        let mut last_applied = None;
        let mut last_membership = None;

        for entry in entries {
            last_applied = Some(entry.log_id.clone());
            if let EntryPayload::Membership(mem) = &entry.payload {
                last_membership = Some(StoredMembership::new(Some(entry.log_id.clone()), mem.clone()));
            }
            if let EntryPayload::Normal(cmd) = entry.payload {
                apply_cmd(cmd, &short_term, &core_memory, &embedding_tx).await;
            }
            responses.push(CommandResponse::default());
        }

        // Commit bookkeeping in a single lock rather than once per entry.
        {
            let mut inner = self.inner.lock().await;
            if let Some(applied) = last_applied {
                inner.last_applied = Some(applied);
            }
            if let Some(membership) = last_membership {
                inner.last_membership = membership;
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        NoOpSnapshotBuilder
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Err(StorageError::from_io_error(
            ErrorSubject::None,
            ErrorVerb::Read,
            io::Error::new(io::ErrorKind::Unsupported, "snapshots not implemented in Stage 1"),
        ))
    }

    async fn install_snapshot(
        &mut self,
        _meta: &SnapshotMeta<u64, BasicNode>,
        _snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        Err(StorageError::from_io_error(
            ErrorSubject::None,
            ErrorVerb::Write,
            io::Error::new(io::ErrorKind::Unsupported, "snapshots not implemented in Stage 1"),
        ))
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        Ok(None)
    }
}

/// Stub snapshot builder. Stage 2 will replace this with a real implementation.
pub struct NoOpSnapshotBuilder;

impl RaftSnapshotBuilder<TypeConfig> for NoOpSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        Err(StorageError::from_io_error(
            ErrorSubject::None,
            ErrorVerb::Read,
            io::Error::new(io::ErrorKind::Unsupported, "snapshot building not implemented in Stage 1"),
        ))
    }
}

async fn apply_cmd(
    cmd: MemoryCommand,
    short_term: &Arc<dyn ShortTermMemory>,
    core_memory: &Arc<dyn CoreMemoryStore>,
    embedding_tx: &mpsc::Sender<EmbeddingJob>,
) {
    match cmd {
        MemoryCommand::AddMessage { session_id, message } => {
            // Store in Redis on this node. LanceDB is updated asynchronously by this node's
            // embedding worker (eventual consistency by design. see architecture notes).
            let msg = Message {
                id: Some(message.id.clone()),
                role: message.role,
                content: message.content.clone(),
                timestamp: Some(message.timestamp),
                embedding_status: Some(EmbeddingStatus::Pending),
            };
            if let Err(e) = short_term.add_message(&session_id, msg).await {
                tracing::error!(error = %e, session_id = %session_id, "failed to add message to short-term store");
            }
            // Drop the job if the channel is full. embedding is eventually consistent.
            let _ = embedding_tx.try_send(EmbeddingJob::new(session_id, message.id, message.content));
        }
        MemoryCommand::AddFact { session_id, fact } => {
            if let Err(e) = core_memory.add_fact(&session_id, &fact).await {
                tracing::error!(error = %e, session_id = %session_id, "failed to add fact to core memory");
            }
        }
        MemoryCommand::DeleteSession { session_id } => {
            if let Err(e) = short_term.delete_session(&session_id).await {
                tracing::error!(error = %e, session_id = %session_id, "failed to delete session from short-term store");
            }
            if let Err(e) = core_memory.delete_session(&session_id).await {
                tracing::error!(error = %e, session_id = %session_id, "failed to delete session from core memory");
            }
            // Signal embedding worker to delete from local LanceDB.
            let _ = embedding_tx.try_send(EmbeddingJob::DeleteSession { session_id });
        }
        MemoryCommand::AddKnowledge { .. } => {
            // Handled in Task 7 when knowledge_graph is wired into the state machine.
        }
        MemoryCommand::NoOp => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{InMemoryCoreMemoryStore, InMemoryStore, InMemoryVectorStore};
    use crate::raft::types::MessagePayload;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    fn make_sm() -> (EngStateMachineStore, Arc<InMemoryStore>, mpsc::Receiver<EmbeddingJob>) {
        let short_term = Arc::new(InMemoryStore::default());
        let core_memory = Arc::new(InMemoryCoreMemoryStore::default());
        let vector_store = Arc::new(InMemoryVectorStore::default());
        let (tx, rx) = mpsc::channel(10);
        let sm = EngStateMachineStore::new(short_term.clone(), core_memory, vector_store, tx);
        (sm, short_term, rx)
    }

    fn make_entry(index: u64, cmd: MemoryCommand) -> openraft::Entry<TypeConfig> {
        openraft::Entry {
            log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
            payload: openraft::EntryPayload::Normal(cmd),
        }
    }

    #[tokio::test]
    async fn add_message_writes_to_short_term() {
        let (mut sm, short_term, _rx) = make_sm();
        sm.apply(vec![make_entry(
            0,
            MemoryCommand::AddMessage {
                session_id: "s1".to_string(),
                message: MessagePayload {
                    id: "m1".to_string(),
                    role: "user".to_string(),
                    content: "hello distributed".to_string(),
                    timestamp: chrono::Utc::now(),
                },
            },
        )])
        .await
        .unwrap();
        let msgs = short_term.get_recent("s1", 10).await.unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].content, "hello distributed");
    }

    #[tokio::test]
    async fn add_message_enqueues_embedding_job() {
        let (mut sm, _st, mut rx) = make_sm();
        sm.apply(vec![make_entry(
            0,
            MemoryCommand::AddMessage {
                session_id: "s1".to_string(),
                message: MessagePayload {
                    id: "m1".to_string(),
                    role: "user".to_string(),
                    content: "embed me".to_string(),
                    timestamp: chrono::Utc::now(),
                },
            },
        )])
        .await
        .unwrap();
        let job = rx.try_recv().expect("embedding job should be enqueued");
        assert!(matches!(job, EmbeddingJob::Embed { text, .. } if text == "embed me"));
    }

    #[tokio::test]
    async fn delete_session_clears_redis_and_enqueues_lancedb_delete() {
        let (mut sm, short_term, mut rx) = make_sm();
        sm.apply(vec![
            make_entry(
                0,
                MemoryCommand::AddMessage {
                    session_id: "s2".to_string(),
                    message: MessagePayload {
                        id: "m1".to_string(),
                        role: "user".to_string(),
                        content: "data".to_string(),
                        timestamp: chrono::Utc::now(),
                    },
                },
            ),
            make_entry(1, MemoryCommand::DeleteSession { session_id: "s2".to_string() }),
        ])
        .await
        .unwrap();
        let msgs = short_term.get_recent("s2", 10).await.unwrap();
        assert_eq!(msgs.len(), 0);
        let _ = rx.try_recv(); // drain the Embed job from AddMessage
        let del_job = rx.try_recv().expect("delete job should be enqueued");
        assert!(matches!(del_job, EmbeddingJob::DeleteSession { session_id } if session_id == "s2"));
    }

    #[tokio::test]
    async fn noop_command_is_ignored() {
        let (mut sm, short_term, _rx) = make_sm();
        sm.apply(vec![make_entry(0, MemoryCommand::NoOp)]).await.unwrap();
        let msgs = short_term.get_recent("any", 10).await.unwrap();
        assert_eq!(msgs.len(), 0);
    }
}
