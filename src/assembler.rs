use std::sync::Arc;

use crate::core::{
    CoreMemoryStore, EmbedError, EmbeddingProvider, MemoryServerError, ShortTermMemory,
    TokenCounter, VectorStore,
};
use crate::models::Message;

pub struct ContextAssembler {
    short_term_memory: Arc<dyn ShortTermMemory>,
    vector_store: Arc<dyn VectorStore>,
    embedding_provider: Arc<dyn EmbeddingProvider>,
    token_counter: Arc<dyn TokenCounter>,
    core_memory_store: Arc<dyn CoreMemoryStore>,
}

impl ContextAssembler {
    pub fn new(
        short_term_memory: Arc<dyn ShortTermMemory>,
        vector_store: Arc<dyn VectorStore>,
        embedding_provider: Arc<dyn EmbeddingProvider>,
        token_counter: Arc<dyn TokenCounter>,
        core_memory_store: Arc<dyn CoreMemoryStore>,
    ) -> Self {
        Self {
            short_term_memory,
            vector_store,
            embedding_provider,
            token_counter,
            core_memory_store,
        }
    }

    pub async fn assemble_context(
        &self,
        session_id: &str,
        max_tokens: usize,
        similarity_threshold: f32,
        long_term_top_k: usize,
    ) -> Result<String, MemoryServerError> {
        let core_facts = self.core_memory_store.get_facts(session_id).await?;
        let core_text = format_core_memories(&core_facts);
        let core_tokens = count_tokens(&*self.token_counter, &core_text);

        let budget_after_core = max_tokens.saturating_sub(core_tokens);
        let trimmed_short = self
            .short_term_memory
            .trim_to_token_budget(session_id, budget_after_core, &*self.token_counter)
            .await?;
        let short_text = format_conversation(&trimmed_short);
        let used_tokens = core_tokens + count_tokens(&*self.token_counter, &short_text);

        let mut sections = Vec::new();
        if !core_text.is_empty() {
            sections.push(core_text.clone());
        }
        if !short_text.is_empty() {
            sections.push(short_text.clone());
        }

        let mut remaining_budget = max_tokens.saturating_sub(used_tokens);
        if remaining_budget == 0 {
            return Ok(join_sections(&sections));
        }

        let query_text = derive_query_text(&trimmed_short);
        if query_text.is_empty() {
            return Ok(join_sections(&sections));
        }

        let embeddings = self
            .embedding_provider
            .embed(std::slice::from_ref(&query_text))
            .await?;
        let query_embedding = embeddings.first().ok_or_else(|| {
            MemoryServerError::from(EmbedError::Message(
                "embedding provider returned no embeddings".to_string(),
            ))
        })?;

        let candidates = self
            .vector_store
            .search(session_id, query_embedding, long_term_top_k)
            .await?;

        let mut long_term_memories = Vec::new();
        for candidate in candidates {
            if candidate.score < similarity_threshold {
                continue;
            }

            let memory_line = format!("Memory: {}", candidate.text);
            let memory_tokens = self.token_counter.count_tokens(&memory_line);

            if memory_tokens > remaining_budget {
                break;
            }

            remaining_budget -= memory_tokens;
            long_term_memories.push(memory_line);
        }

        let mut final_sections = Vec::new();
        if !core_text.is_empty() {
            final_sections.push(core_text);
        }
        if !long_term_memories.is_empty() {
            final_sections.push(long_term_memories.join("\n"));
        }
        if !short_text.is_empty() {
            final_sections.push(short_text);
        }

        Ok(join_sections(&final_sections))
    }
}

fn format_core_memories(facts: &[String]) -> String {
    if facts.is_empty() {
        return String::new();
    }

    let mut lines = vec!["Core memories:".to_string()];
    lines.extend(facts.iter().map(|fact| format!("- {fact}")));
    lines.join("\n")
}

fn format_conversation(messages: &[Message]) -> String {
    if messages.is_empty() {
        return String::new();
    }

    let mut lines = vec!["Conversation:".to_string()];
    lines.extend(
        messages
            .iter()
            .map(|message| format!("{}: {}", message.role, message.content)),
    );
    lines.join("\n")
}

fn derive_query_text(messages: &[Message]) -> String {
    messages
        .iter()
        .rev()
        .find(|message| message.role == "user")
        .or_else(|| messages.last())
        .map(|message| message.content.clone())
        .unwrap_or_default()
}

fn count_tokens(token_counter: &dyn TokenCounter, text: &str) -> usize {
    if text.is_empty() {
        0
    } else {
        token_counter.count_tokens(text)
    }
}

fn join_sections(sections: &[String]) -> String {
    sections
        .iter()
        .filter(|section| !section.is_empty())
        .cloned()
        .collect::<Vec<_>>()
        .join("\n\n")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use super::ContextAssembler;
    use crate::core::{
        CoreMemoryStore, DummyTokenCounter, EmbedError, EmbeddingProvider, MemoryError,
        SearchResult, ShortTermMemory, StoreError, TokenCounter, VectorStore,
    };
    use crate::models::{EmbeddingStatus, Message};

    fn message(role: &str, content: &str) -> Message {
        Message {
            id: None,
            role: role.to_string(),
            content: content.to_string(),
            timestamp: None,
            embedding_status: Some(EmbeddingStatus::Pending),
        }
    }

    fn format_core(facts: &[&str]) -> String {
        let mut lines = vec!["Core memories:".to_string()];
        lines.extend(facts.iter().map(|fact| format!("- {fact}")));
        lines.join("\n")
    }

    fn format_conversation(messages: &[Message]) -> String {
        let mut lines = vec!["Conversation:".to_string()];
        lines.extend(
            messages
                .iter()
                .map(|message| format!("{}: {}", message.role, message.content)),
        );
        lines.join("\n")
    }

    fn format_memory(text: &str) -> String {
        format!("Memory: {text}")
    }

    fn total_tokens(text: &str) -> usize {
        DummyTokenCounter.count_tokens(text)
    }

    struct MockShortTermMemory {
        recent_by_session: Mutex<HashMap<String, Vec<Message>>>,
        trimmed_override: Mutex<HashMap<String, Vec<Message>>>,
    }

    impl MockShortTermMemory {
        fn new(recent_by_session: HashMap<String, Vec<Message>>) -> Self {
            Self {
                recent_by_session: Mutex::new(recent_by_session),
                trimmed_override: Mutex::new(HashMap::new()),
            }
        }

        fn with_trimmed_override(self, session_id: &str, messages: Vec<Message>) -> Self {
            self.trimmed_override
                .lock()
                .unwrap()
                .insert(session_id.to_string(), messages);
            self
        }
    }

    #[async_trait]
    impl ShortTermMemory for MockShortTermMemory {
        async fn add_message(&self, session_id: &str, msg: Message) -> Result<(), MemoryError> {
            self.recent_by_session
                .lock()
                .map_err(|error| MemoryError::Message(error.to_string()))?
                .entry(session_id.to_string())
                .or_default()
                .push(msg);
            Ok(())
        }

        async fn get_recent(
            &self,
            session_id: &str,
            count: usize,
        ) -> Result<Vec<Message>, MemoryError> {
            let messages = self
                .recent_by_session
                .lock()
                .map_err(|error| MemoryError::Message(error.to_string()))?
                .get(session_id)
                .cloned()
                .unwrap_or_default();

            let start = messages.len().saturating_sub(count);
            Ok(messages[start..].to_vec())
        }

        async fn trim(&self, session_id: &str, max_count: usize) -> Result<(), MemoryError> {
            let mut recent_by_session = self
                .recent_by_session
                .lock()
                .map_err(|error| MemoryError::Message(error.to_string()))?;

            if let Some(messages) = recent_by_session.get_mut(session_id) {
                let start = messages.len().saturating_sub(max_count);
                messages.drain(0..start);
            }

            Ok(())
        }

        async fn trim_to_token_budget(
            &self,
            session_id: &str,
            max_tokens: usize,
            token_counter: &dyn TokenCounter,
        ) -> Result<Vec<Message>, MemoryError> {
            if let Some(messages) = self
                .trimmed_override
                .lock()
                .map_err(|error| MemoryError::Message(error.to_string()))?
                .get(session_id)
                .cloned()
            {
                return Ok(messages);
            }

            let mut messages = self
                .recent_by_session
                .lock()
                .map_err(|error| MemoryError::Message(error.to_string()))?
                .get(session_id)
                .cloned()
                .unwrap_or_default();

            while messages
                .iter()
                .map(|message| token_counter.count_tokens(&message.content))
                .sum::<usize>()
                > max_tokens
                && !messages.is_empty()
            {
                let remove_count = if messages.len() >= 2
                    && messages[0].role == "user"
                    && messages[1].role == "assistant"
                {
                    2
                } else {
                    1
                };
                messages.drain(0..remove_count);
            }

            Ok(messages)
        }

        async fn delete_session(&self, session_id: &str) -> Result<(), MemoryError> {
            self.recent_by_session
                .lock()
                .map_err(|error| MemoryError::Message(error.to_string()))?
                .remove(session_id);
            Ok(())
        }
    }

    struct MockVectorStore {
        results_by_session: Mutex<HashMap<String, Vec<SearchResult>>>,
    }

    impl MockVectorStore {
        fn new(results_by_session: HashMap<String, Vec<SearchResult>>) -> Self {
            Self {
                results_by_session: Mutex::new(results_by_session),
            }
        }
    }

    #[async_trait]
    impl VectorStore for MockVectorStore {
        async fn insert(
            &self,
            _session_id: &str,
            _text: &str,
            _embedding: Vec<f32>,
            _message_id: &str,
        ) -> Result<(), StoreError> {
            Ok(())
        }

        async fn search(
            &self,
            session_id: &str,
            _query_embedding: &[f32],
            top_k: usize,
        ) -> Result<Vec<SearchResult>, StoreError> {
            let mut results = self
                .results_by_session
                .lock()
                .map_err(|error| StoreError::Message(error.to_string()))?
                .get(session_id)
                .cloned()
                .unwrap_or_default();
            results.truncate(top_k);
            Ok(results)
        }

        async fn delete_session(&self, session_id: &str) -> Result<(), StoreError> {
            self.results_by_session
                .lock()
                .map_err(|error| StoreError::Message(error.to_string()))?
                .remove(session_id);
            Ok(())
        }
    }

    struct MockEmbeddingProvider {
        embedding: Vec<f32>,
    }

    #[async_trait]
    impl EmbeddingProvider for MockEmbeddingProvider {
        async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
            Ok(texts.iter().map(|_| self.embedding.clone()).collect())
        }
    }

    struct MockCoreMemoryStore {
        facts_by_session: Mutex<HashMap<String, Vec<String>>>,
    }

    impl MockCoreMemoryStore {
        fn new(facts_by_session: HashMap<String, Vec<String>>) -> Self {
            Self {
                facts_by_session: Mutex::new(facts_by_session),
            }
        }
    }

    #[async_trait]
    impl CoreMemoryStore for MockCoreMemoryStore {
        async fn add_fact(&self, session_id: &str, fact: &str) -> Result<(), MemoryError> {
            self.facts_by_session
                .lock()
                .map_err(|error| MemoryError::Message(error.to_string()))?
                .entry(session_id.to_string())
                .or_default()
                .push(fact.to_string());
            Ok(())
        }

        async fn get_facts(&self, session_id: &str) -> Result<Vec<String>, MemoryError> {
            Ok(self
                .facts_by_session
                .lock()
                .map_err(|error| MemoryError::Message(error.to_string()))?
                .get(session_id)
                .cloned()
                .unwrap_or_default())
        }
    }

    fn assembler(
        short_term_memory: MockShortTermMemory,
        vector_store: MockVectorStore,
        core_memory_store: MockCoreMemoryStore,
    ) -> ContextAssembler {
        ContextAssembler::new(
            Arc::new(short_term_memory),
            Arc::new(vector_store),
            Arc::new(MockEmbeddingProvider {
                embedding: vec![1.0; 1536],
            }),
            Arc::new(DummyTokenCounter),
            Arc::new(core_memory_store),
        )
    }

    #[tokio::test]
    async fn core_memory_always_included() {
        let assembler = assembler(
            MockShortTermMemory::new(HashMap::new()),
            MockVectorStore::new(HashMap::new()),
            MockCoreMemoryStore::new(HashMap::from([(
                "session-1".to_string(),
                vec!["User name is Alex".to_string()],
            )])),
        );

        let context = assembler
            .assemble_context("session-1", 1_000, 0.7, 10)
            .await
            .unwrap();

        assert_eq!(context, format_core(&["User name is Alex"]));
    }

    #[tokio::test]
    async fn short_term_messages_are_included_after_trimming() {
        let session_id = "session-1";
        let trimmed = vec![
            message("user", "keep question"),
            message("assistant", "keep answer"),
        ];
        let assembler = assembler(
            MockShortTermMemory::new(HashMap::from([(
                session_id.to_string(),
                vec![
                    message("user", "drop question"),
                    message("assistant", "drop answer"),
                    trimmed[0].clone(),
                    trimmed[1].clone(),
                ],
            )]))
            .with_trimmed_override(session_id, trimmed.clone()),
            MockVectorStore::new(HashMap::new()),
            MockCoreMemoryStore::new(HashMap::new()),
        );

        let context = assembler
            .assemble_context(session_id, 1_000, 0.7, 10)
            .await
            .unwrap();

        assert_eq!(context, format_conversation(&trimmed));
    }

    #[tokio::test]
    async fn long_term_memories_require_budget_and_threshold() {
        let session_id = "session-1";
        let trimmed = vec![message("user", "rust async")];
        let assembler = assembler(
            MockShortTermMemory::new(HashMap::from([(session_id.to_string(), trimmed.clone())]))
                .with_trimmed_override(session_id, trimmed.clone()),
            MockVectorStore::new(HashMap::from([(
                session_id.to_string(),
                vec![
                    SearchResult {
                        text: "Relevant memory".to_string(),
                        score: 0.8,
                    },
                    SearchResult {
                        text: "Filtered memory".to_string(),
                        score: 0.5,
                    },
                ],
            )])),
            MockCoreMemoryStore::new(HashMap::new()),
        );

        let context = assembler
            .assemble_context(session_id, 1_000, 0.7, 10)
            .await
            .unwrap();

        assert!(context.contains(&format_memory("Relevant memory")));
        assert!(!context.contains(&format_memory("Filtered memory")));
        assert!(context.contains("Conversation:\nuser: rust async"));
    }

    #[tokio::test]
    async fn pair_preservation_keeps_user_assistant_pairs_intact() {
        let session_id = "session-1";
        let assembler = assembler(
            MockShortTermMemory::new(HashMap::from([(
                session_id.to_string(),
                vec![
                    message("user", "aaa"),
                    message("assistant", "bbb"),
                    message("user", "cc"),
                    message("assistant", "dd"),
                    message("user", "eee"),
                ],
            )])),
            MockVectorStore::new(HashMap::new()),
            MockCoreMemoryStore::new(HashMap::new()),
        );

        let context = assembler
            .assemble_context(session_id, 7, 0.7, 10)
            .await
            .unwrap();

        assert_eq!(
            context,
            format_conversation(&[
                message("user", "cc"),
                message("assistant", "dd"),
                message("user", "eee"),
            ])
        );
    }

    #[tokio::test]
    async fn empty_session_returns_empty_string() {
        let assembler = assembler(
            MockShortTermMemory::new(HashMap::new()),
            MockVectorStore::new(HashMap::new()),
            MockCoreMemoryStore::new(HashMap::new()),
        );

        let context = assembler
            .assemble_context("missing-session", 1_000, 0.7, 10)
            .await
            .unwrap();

        assert!(context.is_empty());
    }

    #[tokio::test]
    async fn only_core_memory_returns_only_core_section() {
        let assembler = assembler(
            MockShortTermMemory::new(HashMap::new()),
            MockVectorStore::new(HashMap::new()),
            MockCoreMemoryStore::new(HashMap::from([(
                "session-1".to_string(),
                vec!["User prefers dark mode".to_string()],
            )])),
        );

        let context = assembler
            .assemble_context("session-1", 1_000, 0.7, 10)
            .await
            .unwrap();

        assert_eq!(context, format_core(&["User prefers dark mode"]));
    }

    #[tokio::test]
    async fn budget_exactly_enough_for_core_and_short_skips_long_term() {
        let session_id = "session-1";
        let core = format_core(&["Pinned fact"]);
        let trimmed = vec![message("user", "hi")];
        let short = format_conversation(&trimmed);
        let max_tokens = total_tokens(&core) + total_tokens(&short);

        let assembler = assembler(
            MockShortTermMemory::new(HashMap::from([(session_id.to_string(), trimmed.clone())]))
                .with_trimmed_override(session_id, trimmed.clone()),
            MockVectorStore::new(HashMap::from([(
                session_id.to_string(),
                vec![SearchResult {
                    text: "Should not fit".to_string(),
                    score: 0.9,
                }],
            )])),
            MockCoreMemoryStore::new(HashMap::from([(
                session_id.to_string(),
                vec!["Pinned fact".to_string()],
            )])),
        );

        let context = assembler
            .assemble_context(session_id, max_tokens, 0.7, 10)
            .await
            .unwrap();

        assert_eq!(context, format!("{core}\n\n{short}"));
    }

    #[tokio::test]
    async fn budget_just_enough_for_one_long_term_memory_adds_exactly_one() {
        let session_id = "session-1";
        let trimmed = vec![message("user", "hi")];
        let short = format_conversation(&trimmed);
        let first_memory = format_memory("One memory");
        let second_memory = format_memory("Two memory");
        let max_tokens = total_tokens(&short) + total_tokens(&first_memory);

        let assembler = assembler(
            MockShortTermMemory::new(HashMap::from([(session_id.to_string(), trimmed.clone())]))
                .with_trimmed_override(session_id, trimmed.clone()),
            MockVectorStore::new(HashMap::from([(
                session_id.to_string(),
                vec![
                    SearchResult {
                        text: "One memory".to_string(),
                        score: 0.9,
                    },
                    SearchResult {
                        text: "Two memory".to_string(),
                        score: 0.85,
                    },
                ],
            )])),
            MockCoreMemoryStore::new(HashMap::new()),
        );

        let context = assembler
            .assemble_context(session_id, max_tokens, 0.7, 10)
            .await
            .unwrap();

        assert_eq!(context, format!("{first_memory}\n\n{short}"));
        assert!(!context.contains(&second_memory));
    }
}
