use serde::{Deserialize, Serialize};

use crate::knowledge::graph::GraphSnapshot;
use crate::models::Message;

/// Snapshot schema version. Bump when the payload layout changes incompatibly.
pub const SNAPSHOT_VERSION: u32 = 3;

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
    #[serde(default)]
    pub global_graph: Option<crate::knowledge::global::GlobalGraphSnapshot>,
    #[serde(default)]
    pub visibility: Vec<(String, crate::knowledge::global::Visibility)>,
    #[serde(default)]
    pub session_agents: Vec<(String, String)>,
    /// Per-session consolidated summaries. Added in v3; v1/v2 snapshots load with an empty map.
    #[serde(default)]
    pub consolidated: Vec<(String, Vec<crate::consolidation::store::Summary>)>,
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
            visibility: vec![],
            session_agents: vec![],
            consolidated: vec![],
        }
    }

    #[test]
    fn snapshot_carries_version_three() {
        assert_eq!(sample().version, 3);
    }

    #[test]
    fn snapshot_round_trips_through_bytes() {
        let snap = sample();
        let bytes = snap.to_bytes().unwrap();
        let back = EngramSnapshot::from_bytes(&bytes).unwrap();
        assert_eq!(back.version, 3);
        assert_eq!(back.core_memory[0].facts, vec!["f".to_string()]);
        assert!(back.global_graph.is_none());
        assert!(back.visibility.is_empty());
        assert!(back.consolidated.is_empty());
    }

    #[test]
    fn unknown_global_graph_absent_by_default() {
        let bytes = sample().to_bytes().unwrap();
        // Absent global_graph must deserialize cleanly (forward-compat for 3B).
        let back = EngramSnapshot::from_bytes(&bytes).unwrap();
        assert!(back.global_graph.is_none());
    }

    #[test]
    fn snapshot_carries_global_and_visibility() {
        let snap = EngramSnapshot {
            version: SNAPSHOT_VERSION,
            short_term: vec![],
            core_memory: vec![],
            knowledge_graph: crate::knowledge::graph::GraphSnapshot::default(),
            global_graph: Some(crate::knowledge::global::GlobalGraphSnapshot::default()),
            visibility: vec![("s1".into(), crate::knowledge::global::Visibility::Shared)],
            session_agents: vec![("s1".into(), "agent-7".into())],
            consolidated: vec![],
        };
        assert_eq!(snap.version, 3);
        let bytes = snap.to_bytes().unwrap();
        let back = EngramSnapshot::from_bytes(&bytes).unwrap();
        assert!(back.global_graph.is_some());
        assert_eq!(back.visibility.len(), 1);
        assert_eq!(back.session_agents, vec![("s1".to_string(), "agent-7".to_string())]);
    }

    #[test]
    fn v1_snapshot_without_global_fields_still_loads() {
        let v1 = r#"{"version":1,"short_term":[],"core_memory":[],"knowledge_graph":{"sessions":[],"processed":[]}}"#;
        let back = EngramSnapshot::from_bytes(v1.as_bytes()).unwrap();
        assert!(back.global_graph.is_none());
        assert!(back.visibility.is_empty());
        assert!(back.session_agents.is_empty());
        assert!(back.consolidated.is_empty());
    }

    #[test]
    fn snapshot_version_is_three_and_carries_consolidated() {
        let snap = EngramSnapshot {
            version: SNAPSHOT_VERSION,
            short_term: vec![],
            core_memory: vec![],
            knowledge_graph: crate::knowledge::graph::GraphSnapshot::default(),
            global_graph: None,
            visibility: vec![],
            session_agents: vec![],
            consolidated: vec![(
                "s1".to_string(),
                vec![crate::consolidation::store::Summary {
                    id: "u1".into(),
                    text: "t".into(),
                    created_at_index: 1,
                    consumed_message_ids: vec!["m1".into()],
                    consumed_count: 1,
                    model: "mock".into(),
                    prompt_version: "summarize_v1".into(),
                }],
            )],
        };
        assert_eq!(snap.version, 3);
        let bytes = snap.to_bytes().unwrap();
        let back = EngramSnapshot::from_bytes(&bytes).unwrap();
        assert_eq!(back.consolidated.len(), 1);
        assert_eq!(back.consolidated[0].1[0].id, "u1");
    }

    #[test]
    fn v2_snapshot_without_consolidated_still_loads() {
        // v2 JSON without the consolidated field must load cleanly.
        let v2 = r#"{"version":2,"short_term":[],"core_memory":[],"knowledge_graph":{"sessions":[],"processed":[]},"global_graph":null,"visibility":[],"session_agents":[]}"#;
        let back = EngramSnapshot::from_bytes(v2.as_bytes()).unwrap();
        assert!(back.consolidated.is_empty());
    }
}
