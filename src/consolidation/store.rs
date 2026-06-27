use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;

use crate::core::MemoryError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Summary {
    /// Leader-minted UUID. Idempotency key for ApplySummary. Not derived from inputs.
    pub id: String,
    /// LLM-produced summary text. Immutable once applied.
    pub text: String,
    /// Raft log index that committed this summary. Nodes sort by this, never by wall clock.
    pub created_at_index: u64,
    /// Message ids that were summarized and then trimmed.
    pub consumed_message_ids: Vec<String>,
    /// Count of messages consumed. Redundant with consumed_message_ids.len(), but stored
    /// so metrics and scoring don't need to walk the vec.
    pub consumed_count: u64,
    /// Model that produced this summary. Carried on the command (not read from node-local
    /// config) so the stored artifact is byte-identical on every node.
    pub model: String,
    /// Prompt version used. Same reason as model.
    pub prompt_version: String,
}

#[async_trait]
pub trait ConsolidatedMemoryStore: Send + Sync {
    async fn add_summary(&self, session_id: &str, summary: Summary) -> Result<(), MemoryError>;
    async fn get_summaries(&self, session_id: &str) -> Result<Vec<Summary>, MemoryError>;
    async fn delete_session(&self, _session_id: &str) -> Result<(), MemoryError> {
        Ok(())
    }
    async fn dump_all(&self) -> Result<Vec<(String, Vec<Summary>)>, MemoryError> {
        Ok(vec![])
    }
    async fn restore_all(
        &self,
        _sessions: Vec<(String, Vec<Summary>)>,
    ) -> Result<(), MemoryError> {
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct InMemoryConsolidatedStore {
    summaries: Mutex<HashMap<String, Vec<Summary>>>,
}

#[async_trait]
impl ConsolidatedMemoryStore for InMemoryConsolidatedStore {
    async fn add_summary(&self, session_id: &str, summary: Summary) -> Result<(), MemoryError> {
        let mut map = self
            .summaries
            .lock()
            .map_err(|e| MemoryError::Message(e.to_string()))?;
        let list = map.entry(session_id.to_string()).or_default();
        // Idempotent by summary id: re-applying the same summary is a no-op.
        if list.iter().any(|existing| existing.id == summary.id) {
            return Ok(());
        }
        list.push(summary);
        Ok(())
    }

    async fn get_summaries(&self, session_id: &str) -> Result<Vec<Summary>, MemoryError> {
        let map = self
            .summaries
            .lock()
            .map_err(|e| MemoryError::Message(e.to_string()))?;
        Ok(map.get(session_id).cloned().unwrap_or_default())
    }

    async fn delete_session(&self, session_id: &str) -> Result<(), MemoryError> {
        let mut map = self
            .summaries
            .lock()
            .map_err(|e| MemoryError::Message(e.to_string()))?;
        map.remove(session_id);
        Ok(())
    }

    async fn dump_all(&self) -> Result<Vec<(String, Vec<Summary>)>, MemoryError> {
        let map = self
            .summaries
            .lock()
            .map_err(|e| MemoryError::Message(e.to_string()))?;
        Ok(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }

    async fn restore_all(
        &self,
        sessions: Vec<(String, Vec<Summary>)>,
    ) -> Result<(), MemoryError> {
        let mut map = self
            .summaries
            .lock()
            .map_err(|e| MemoryError::Message(e.to_string()))?;
        map.clear();
        for (session_id, list) in sessions {
            map.insert(session_id, list);
        }
        Ok(())
    }
}

#[cfg(test)]
mod store_tests {
    use super::*;

    fn summary(id: &str, index: u64) -> Summary {
        Summary {
            id: id.into(),
            text: format!("summary {id}"),
            created_at_index: index,
            consumed_message_ids: vec!["m1".into()],
            consumed_count: 1,
            model: "mock".into(),
            prompt_version: "summarize_v1".into(),
        }
    }

    #[tokio::test]
    async fn add_and_get_summaries_per_session() {
        let store = InMemoryConsolidatedStore::default();
        store.add_summary("s1", summary("a", 1)).await.unwrap();
        store.add_summary("s1", summary("b", 2)).await.unwrap();
        store.add_summary("s2", summary("c", 3)).await.unwrap();

        assert_eq!(store.get_summaries("s1").await.unwrap().len(), 2);
        assert_eq!(store.get_summaries("s2").await.unwrap().len(), 1);
        assert!(store.get_summaries("missing").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn add_summary_is_idempotent_by_id() {
        let store = InMemoryConsolidatedStore::default();
        store.add_summary("s1", summary("dup", 1)).await.unwrap();
        store.add_summary("s1", summary("dup", 1)).await.unwrap();
        assert_eq!(store.get_summaries("s1").await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn delete_session_removes_summaries() {
        let store = InMemoryConsolidatedStore::default();
        store.add_summary("s1", summary("a", 1)).await.unwrap();
        store.delete_session("s1").await.unwrap();
        assert!(store.get_summaries("s1").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn dump_and_restore_round_trip() {
        let store = InMemoryConsolidatedStore::default();
        store.add_summary("s1", summary("a", 1)).await.unwrap();
        store.add_summary("s2", summary("b", 2)).await.unwrap();
        let dump = store.dump_all().await.unwrap();

        let fresh = InMemoryConsolidatedStore::default();
        fresh.add_summary("stale", summary("z", 9)).await.unwrap();
        fresh.restore_all(dump).await.unwrap();

        assert!(fresh.get_summaries("stale").await.unwrap().is_empty());
        assert_eq!(fresh.get_summaries("s1").await.unwrap()[0].id, "a");
        assert_eq!(fresh.get_summaries("s2").await.unwrap()[0].id, "b");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_round_trips_with_lineage_and_metadata() {
        let s = Summary {
            id: "11111111-1111-1111-1111-111111111111".into(),
            text: "Alice discussed her work at OpenAI.".into(),
            created_at_index: 42,
            consumed_message_ids: vec!["m1".into(), "m2".into()],
            consumed_count: 2,
            model: "gpt-4o-mini".into(),
            prompt_version: "summarize_v1".into(),
        };
        let json = serde_json::to_string(&s).unwrap();
        let back: Summary = serde_json::from_str(&json).unwrap();
        assert_eq!(back, s);
        assert_eq!(back.consumed_message_ids.len(), 2);
        assert_eq!(back.consumed_count, 2);
        assert_eq!(back.model, "gpt-4o-mini");
    }
}
