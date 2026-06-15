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
    /// Extract and store knowledge from a message. Idempotent by (session_id, message_id).
    /// Only submitted by the leader's knowledge worker. All nodes receive this command
    /// via Raft replication and apply it to their local KnowledgeGraph.
    AddKnowledge {
        session_id: String,
        message_id: String,
        entities: Vec<crate::knowledge::types::Entity>,
        relationships: Vec<crate::knowledge::types::Relationship>,
    },
    /// Set a session's visibility. Replicated so every node agrees deterministically
    /// on which sessions contribute to the global graph.
    SetSessionVisibility {
        session_id: String,
        visibility: crate::knowledge::global::Visibility,
    },
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
    fn add_knowledge_command_round_trips() {
        use crate::knowledge::types::{Entity, Relationship};
        use std::collections::HashMap;

        let cmd = MemoryCommand::AddKnowledge {
            session_id: "s1".to_string(),
            message_id: "m1".to_string(),
            entities: vec![
                Entity { name: "Alice".into(), entity_type: "Person".into(), attributes: HashMap::new() },
            ],
            relationships: vec![
                Relationship { from: "Alice".into(), to: "OpenAI".into(), relationship_type: "works_at".into() },
            ],
        };
        let json = serde_json::to_string(&cmd).unwrap();
        let back: MemoryCommand = serde_json::from_str(&json).unwrap();
        match back {
            MemoryCommand::AddKnowledge { session_id, message_id, entities, relationships } => {
                assert_eq!(session_id, "s1");
                assert_eq!(message_id, "m1");
                assert_eq!(entities[0].name, "Alice");
                assert_eq!(relationships[0].relationship_type, "works_at");
            }
            _ => panic!("wrong variant"),
        }
    }

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

    #[test]
    fn set_session_visibility_command_round_trips() {
        use crate::knowledge::global::Visibility;
        let cmd = MemoryCommand::SetSessionVisibility {
            session_id: "s1".into(),
            visibility: Visibility::Shared,
        };
        let json = serde_json::to_string(&cmd).unwrap();
        let back: MemoryCommand = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, MemoryCommand::SetSessionVisibility { visibility: Visibility::Shared, .. }));
    }
}
