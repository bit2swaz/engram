use serde::{Deserialize, Serialize};

use crate::knowledge::graph::GraphSnapshot;
use crate::models::Message;

/// Snapshot schema version. Bump when the payload layout changes incompatibly.
pub const SNAPSHOT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionMessages {
    pub session_id: String,
    pub messages: Vec<Message>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionFacts {
    pub session_id: String,
    pub facts: Vec<String>,
}

/// Full applied state captured at a Raft log index.
///
/// LanceDB is intentionally NOT included. It is per-node, on-disk, and
/// deterministically rebuildable from message text.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngramSnapshot {
    pub version: u32,
    pub short_term: Vec<SessionMessages>,
    pub core_memory: Vec<SessionFacts>,
    pub knowledge_graph: GraphSnapshot,
    /// Reserved for Stage 3B (collective/global knowledge graph). Absent in 3A.
    #[serde(default)]
    pub global_graph: Option<GraphSnapshot>,
}

impl EngramSnapshot {
    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> EngramSnapshot {
        EngramSnapshot {
            version: SNAPSHOT_VERSION,
            short_term: vec![SessionMessages {
                session_id: "s1".into(),
                messages: vec![],
            }],
            core_memory: vec![SessionFacts { session_id: "s1".into(), facts: vec!["f".into()] }],
            knowledge_graph: crate::knowledge::graph::GraphSnapshot::default(),
            global_graph: None,
        }
    }

    #[test]
    fn snapshot_carries_version_one() {
        assert_eq!(sample().version, 1);
    }

    #[test]
    fn snapshot_round_trips_through_bytes() {
        let snap = sample();
        let bytes = snap.to_bytes().unwrap();
        let back = EngramSnapshot::from_bytes(&bytes).unwrap();
        assert_eq!(back.version, 1);
        assert_eq!(back.core_memory[0].facts, vec!["f".to_string()]);
        assert!(back.global_graph.is_none());
    }

    #[test]
    fn unknown_global_graph_absent_by_default() {
        let bytes = sample().to_bytes().unwrap();
        // Absent global_graph must deserialize cleanly (forward-compat for 3B).
        let back = EngramSnapshot::from_bytes(&bytes).unwrap();
        assert!(back.global_graph.is_none());
    }
}
