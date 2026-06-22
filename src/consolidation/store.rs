use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Summary {
    /// Leader-minted UUID. Idempotency key for ApplySummary. NOT derived from inputs.
    pub id: String,
    /// The LLM-produced summary text. The replicated, immutable artifact.
    pub text: String,
    /// Raft log index that committed this summary. Deterministic ordering across nodes.
    pub created_at_index: u64,
    /// Lineage: the message ids that were summarized and then trimmed.
    pub consumed_message_ids: Vec<String>,
    /// Count of messages this summary consumed. Stored directly so metrics and debugging
    /// don't need to walk the vec.
    pub consumed_count: u64,
    /// Model that produced the summary. This is carried on the command, not read from node-local config.
    pub model: String,
    /// Prompt version that produced the summary. This is carried on the command for the same reason.
    pub prompt_version: String,
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
