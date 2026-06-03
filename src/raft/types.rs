use chrono::{DateTime, Utc};
use openraft::BasicNode;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

/// A message payload stripped of node-local mutable state (embedding_status).
/// embedding_status is managed per-node by each node's embedding worker independently.
/// Each node calls OpenAI and inserts vectors into its own local LanceDB. Eventual
/// consistency is achieved because OpenAI embeddings are deterministic for the same input.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessagePayload {
    pub id: String,
    pub role: String,
    pub content: String,
    pub timestamp: DateTime<Utc>,
}

/// Commands replicated through the Raft log to all nodes.
///
/// LanceDB (vector store) is NOT driven by these commands. Each node's embedding
/// worker handles LanceDB inserts independently, achieving eventual consistency via
/// deterministic OpenAI embeddings. This avoids replicating large vector payloads
/// through Raft while still converging to identical vector state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MemoryCommand {
    /// Stores a message in Redis short-term memory on all nodes.
    /// Each node independently queues an embedding job for its local LanceDB.
    AddMessage {
        session_id: String,
        message: MessagePayload,
    },
    /// Stores a fact in Redis core memory on all nodes.
    AddFact { session_id: String, fact: String },
    /// Deletes all Redis short-term + core memory for a session on all nodes.
    /// Also signals each node's embedding worker to delete from local LanceDB.
    DeleteSession { session_id: String },
    /// No-op placeholder. Applied by the state machine without side effects.
    /// Reserved for future cluster operations (e.g., leadership probes).
    NoOp,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CommandResponse {}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = MemoryCommand,
        R = CommandResponse,
        NodeId = u64,
        Node = BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

pub type NodeId = u64;
pub type RaftHandle = openraft::Raft<TypeConfig>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_command_serializes_round_trip() {
        let cmd = MemoryCommand::AddMessage {
            session_id: "sess-1".to_string(),
            message: MessagePayload {
                id: "msg-1".to_string(),
                role: "user".to_string(),
                content: "hello".to_string(),
                timestamp: chrono::Utc::now(),
            },
        };
        let json = serde_json::to_string(&cmd).unwrap();
        let back: MemoryCommand = serde_json::from_str(&json).unwrap();
        match back {
            MemoryCommand::AddMessage { session_id, message } => {
                assert_eq!(session_id, "sess-1");
                assert_eq!(message.content, "hello");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn noop_command_serializes() {
        let cmd = MemoryCommand::NoOp;
        let json = serde_json::to_string(&cmd).unwrap();
        let back: MemoryCommand = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, MemoryCommand::NoOp));
    }
}
