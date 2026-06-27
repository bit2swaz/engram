use std::cmp::Ordering;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::sync::Mutex;

use async_trait::async_trait;
use rand::Rng;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tiktoken_rs::{CoreBPE, cl100k_base};

use crate::models::{EmbeddingStatus, Message};

type BoxError = Box<dyn StdError + Send + Sync + 'static>;

const EMBEDDING_DIMENSION: usize = 1536;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct SearchResult {
    pub text: String,
    pub score: f32,
}

#[derive(Debug, Error)]
pub enum EmbedError {
    #[error("OPENAI_API_KEY is missing")]
    MissingApiKey,
    #[error("OpenAI API rate limit exceeded after retries")]
    RateLimitExceeded,
    #[error("unexpected embedding response: {0}")]
    InvalidResponse(String),
    #[error("OpenAI API returned status {status}: {body}")]
    HttpStatus { status: u16, body: String },
    #[error("{0}")]
    Message(String),
    #[error(transparent)]
    Other(#[from] BoxError),
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("{0}")]
    Message(String),
    #[error(transparent)]
    Other(#[from] BoxError),
}

#[derive(Debug, Error)]
pub enum MemoryError {
    #[error("{0}")]
    Message(String),
    #[error(transparent)]
    Other(#[from] BoxError),
}

#[derive(Debug, Error)]
pub enum MemoryServerError {
    #[error(transparent)]
    Embed(#[from] EmbedError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Memory(#[from] MemoryError),
    /// In cluster mode: this node is a follower; the client should retry at the given URL.
    #[error("redirect to leader at {0}")]
    RedirectToLeader(String),
    /// In cluster mode: no leader is currently elected.
    #[error("no leader elected")]
    NoLeader,
    #[error("internal error: {0}")]
    Internal(String),
    #[error("embedding queue full")]
    QueueFull,
    #[error("bad request: {0}")]
    BadRequest(String),
}

#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError>;
}

#[async_trait]
pub trait VectorStore: Send + Sync {
    async fn insert(
        &self,
        session_id: &str,
        text: &str,
        embedding: Vec<f32>,
        message_id: &str,
    ) -> Result<(), StoreError>;

    async fn search(
        &self,
        session_id: &str,
        query_embedding: &[f32],
        top_k: usize,
    ) -> Result<Vec<SearchResult>, StoreError>;

    async fn delete_session(&self, session_id: &str) -> Result<(), StoreError>;
}

#[async_trait]
pub trait ShortTermMemory: Send + Sync {
    async fn add_message(&self, session_id: &str, msg: Message) -> Result<(), MemoryError>;

    async fn get_recent(&self, session_id: &str, count: usize)
    -> Result<Vec<Message>, MemoryError>;

    async fn trim(&self, session_id: &str, max_count: usize) -> Result<(), MemoryError>;

    async fn trim_to_token_budget(
        &self,
        session_id: &str,
        max_tokens: usize,
        token_counter: &dyn TokenCounter,
    ) -> Result<Vec<Message>, MemoryError>;

    async fn delete_session(&self, session_id: &str) -> Result<(), MemoryError>;

    async fn update_message_status(
        &self,
        _session_id: &str,
        _message_id: &str,
        _status: EmbeddingStatus,
    ) -> Result<(), MemoryError> {
        Ok(())
    }

    async fn get_message_by_id(
        &self,
        _session_id: &str,
        _message_id: &str,
    ) -> Result<Option<Message>, MemoryError> {
        Ok(None)
    }

    async fn dump_all(&self) -> Result<Vec<(String, Vec<Message>)>, MemoryError> {
        Ok(vec![])
    }

    async fn restore_all(&self, _sessions: Vec<(String, Vec<Message>)>) -> Result<(), MemoryError> {
        Ok(())
    }

    // Drop specific messages by id once they've been rolled up into a summary.
    // Default no-op keeps non-primary stores compiling; real stores override this.
    async fn remove_messages(&self, _session_id: &str, _ids: &[String]) -> Result<(), MemoryError> {
        Ok(())
    }
}

pub trait TokenCounter: Send + Sync {
    fn count_tokens(&self, text: &str) -> usize;
}

#[async_trait]
pub trait CoreMemoryStore: Send + Sync {
    async fn add_fact(&self, session_id: &str, fact: &str) -> Result<(), MemoryError>;

    async fn get_facts(&self, session_id: &str) -> Result<Vec<String>, MemoryError>;

    async fn delete_session(&self, _session_id: &str) -> Result<(), MemoryError> {
        Ok(())
    }

    async fn dump_all(&self) -> Result<Vec<(String, Vec<String>)>, MemoryError> {
        Ok(vec![])
    }

    async fn restore_all(&self, _sessions: Vec<(String, Vec<String>)>) -> Result<(), MemoryError> {
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct RandomEmbeddingProvider;

#[async_trait]
impl EmbeddingProvider for RandomEmbeddingProvider {
    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        let mut rng = rand::rng();
        let embeddings = texts
            .iter()
            .map(|_| {
                (0..EMBEDDING_DIMENSION)
                    .map(|_| rng.random::<f32>())
                    .collect::<Vec<f32>>()
            })
            .collect();

        Ok(embeddings)
    }
}

#[derive(Debug, Clone)]
struct StoredVector {
    message_id: String,
    text: String,
    embedding: Vec<f32>,
}

#[derive(Debug, Default)]
pub struct InMemoryVectorStore {
    memories: Mutex<HashMap<String, Vec<StoredVector>>>,
}

#[async_trait]
impl VectorStore for InMemoryVectorStore {
    async fn insert(
        &self,
        session_id: &str,
        text: &str,
        embedding: Vec<f32>,
        message_id: &str,
    ) -> Result<(), StoreError> {
        let mut memories = self
            .memories
            .lock()
            .map_err(|error| StoreError::Message(error.to_string()))?;

        let session_memories = memories.entry(session_id.to_string()).or_default();

        if let Some(existing) = session_memories
            .iter_mut()
            .find(|entry| entry.message_id == message_id)
        {
            existing.text = text.to_string();
            existing.embedding = embedding;
            return Ok(());
        }

        session_memories.push(StoredVector {
            message_id: message_id.to_string(),
            text: text.to_string(),
            embedding,
        });

        Ok(())
    }

    async fn search(
        &self,
        session_id: &str,
        query_embedding: &[f32],
        top_k: usize,
    ) -> Result<Vec<SearchResult>, StoreError> {
        let memories = self
            .memories
            .lock()
            .map_err(|error| StoreError::Message(error.to_string()))?;

        let mut results = memories
            .get(session_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|entry| SearchResult {
                text: entry.text,
                score: cosine_similarity(query_embedding, &entry.embedding),
            })
            .collect::<Vec<_>>();

        results.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(Ordering::Equal)
        });
        results.truncate(top_k);

        Ok(results)
    }

    async fn delete_session(&self, session_id: &str) -> Result<(), StoreError> {
        let mut memories = self
            .memories
            .lock()
            .map_err(|error| StoreError::Message(error.to_string()))?;

        memories.remove(session_id);
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct InMemoryStore {
    messages: Mutex<HashMap<String, Vec<Message>>>,
}

impl InMemoryStore {
    fn clone_messages(&self, session_id: &str) -> Result<Vec<Message>, MemoryError> {
        let messages = self
            .messages
            .lock()
            .map_err(|error| MemoryError::Message(error.to_string()))?;

        Ok(messages.get(session_id).cloned().unwrap_or_default())
    }
}

#[async_trait]
impl ShortTermMemory for InMemoryStore {
    async fn add_message(&self, session_id: &str, msg: Message) -> Result<(), MemoryError> {
        let mut messages = self
            .messages
            .lock()
            .map_err(|error| MemoryError::Message(error.to_string()))?;

        messages
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
        let messages = self.clone_messages(session_id)?;
        let start = messages.len().saturating_sub(count);

        Ok(messages[start..].to_vec())
    }

    async fn trim(&self, session_id: &str, max_count: usize) -> Result<(), MemoryError> {
        let mut messages = self
            .messages
            .lock()
            .map_err(|error| MemoryError::Message(error.to_string()))?;

        if let Some(session_messages) = messages.get_mut(session_id) {
            if session_messages.len() > max_count {
                let remove_count = session_messages.len() - max_count;
                session_messages.drain(0..remove_count);
            }
        }

        Ok(())
    }

    async fn trim_to_token_budget(
        &self,
        session_id: &str,
        max_tokens: usize,
        token_counter: &dyn TokenCounter,
    ) -> Result<Vec<Message>, MemoryError> {
        let messages = self.clone_messages(session_id)?;
        Ok(trim_messages_to_token_budget(
            messages,
            max_tokens,
            token_counter,
        ))
    }

    async fn delete_session(&self, session_id: &str) -> Result<(), MemoryError> {
        let mut messages = self
            .messages
            .lock()
            .map_err(|error| MemoryError::Message(error.to_string()))?;

        messages.remove(session_id);
        Ok(())
    }

    async fn remove_messages(&self, session_id: &str, ids: &[String]) -> Result<(), MemoryError> {
        let mut messages = self
            .messages
            .lock()
            .map_err(|error| MemoryError::Message(error.to_string()))?;
        if let Some(session_messages) = messages.get_mut(session_id) {
            session_messages.retain(|m| match m.id.as_deref() {
                Some(id) => !ids.contains(&id.to_string()),
                None => true,
            });
        }
        Ok(())
    }

    async fn update_message_status(
        &self,
        session_id: &str,
        message_id: &str,
        status: EmbeddingStatus,
    ) -> Result<(), MemoryError> {
        let mut messages = self
            .messages
            .lock()
            .map_err(|error| MemoryError::Message(error.to_string()))?;

        if let Some(session_messages) = messages.get_mut(session_id) {
            if let Some(message) = session_messages
                .iter_mut()
                .find(|message| message.id.as_deref() == Some(message_id))
            {
                message.embedding_status = Some(status);
            }
        }

        Ok(())
    }

    async fn get_message_by_id(
        &self,
        session_id: &str,
        message_id: &str,
    ) -> Result<Option<Message>, MemoryError> {
        let messages = self.clone_messages(session_id)?;

        Ok(messages
            .into_iter()
            .find(|message| message.id.as_deref() == Some(message_id)))
    }

    async fn dump_all(&self) -> Result<Vec<(String, Vec<Message>)>, MemoryError> {
        let messages = self.messages.lock().map_err(|e| MemoryError::Message(e.to_string()))?;
        Ok(messages.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }

    async fn restore_all(&self, sessions: Vec<(String, Vec<Message>)>) -> Result<(), MemoryError> {
        let mut messages = self.messages.lock().map_err(|e| MemoryError::Message(e.to_string()))?;
        messages.clear();
        for (session_id, msgs) in sessions {
            messages.insert(session_id, msgs);
        }
        Ok(())
    }
}

pub struct OpenAITokenCounter {
    encoding: CoreBPE,
}

impl OpenAITokenCounter {
    pub fn new() -> Result<Self, BoxError> {
        Ok(Self {
            encoding: cl100k_base()?,
        })
    }
}

impl TokenCounter for OpenAITokenCounter {
    fn count_tokens(&self, text: &str) -> usize {
        self.encoding.encode_with_special_tokens(text).len()
    }
}

#[derive(Debug, Default)]
pub struct InMemoryCoreMemoryStore {
    facts: Mutex<HashMap<String, Vec<String>>>,
}

#[async_trait]
impl CoreMemoryStore for InMemoryCoreMemoryStore {
    async fn add_fact(&self, session_id: &str, fact: &str) -> Result<(), MemoryError> {
        let mut facts = self
            .facts
            .lock()
            .map_err(|error| MemoryError::Message(error.to_string()))?;

        facts
            .entry(session_id.to_string())
            .or_default()
            .push(fact.to_string());
        Ok(())
    }

    async fn get_facts(&self, session_id: &str) -> Result<Vec<String>, MemoryError> {
        let facts = self
            .facts
            .lock()
            .map_err(|error| MemoryError::Message(error.to_string()))?;

        Ok(facts.get(session_id).cloned().unwrap_or_default())
    }

    async fn delete_session(&self, session_id: &str) -> Result<(), MemoryError> {
        let mut facts = self
            .facts
            .lock()
            .map_err(|error| MemoryError::Message(error.to_string()))?;

        facts.remove(session_id);
        Ok(())
    }

    async fn dump_all(&self) -> Result<Vec<(String, Vec<String>)>, MemoryError> {
        let facts = self.facts.lock().map_err(|e| MemoryError::Message(e.to_string()))?;
        Ok(facts.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }

    async fn restore_all(&self, sessions: Vec<(String, Vec<String>)>) -> Result<(), MemoryError> {
        let mut facts = self.facts.lock().map_err(|e| MemoryError::Message(e.to_string()))?;
        facts.clear();
        for (session_id, list) in sessions {
            facts.insert(session_id, list);
        }
        Ok(())
    }
}

fn conversation_text(messages: &[Message]) -> String {
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

fn total_tokens(messages: &[Message], token_counter: &dyn TokenCounter) -> usize {
    token_counter.count_tokens(&conversation_text(messages))
}

pub(crate) fn trim_messages_to_token_budget(
    mut messages: Vec<Message>,
    max_tokens: usize,
    token_counter: &dyn TokenCounter,
) -> Vec<Message> {
    while total_tokens(&messages, token_counter) > max_tokens && !messages.is_empty() {
        let remove_count = removable_prefix_len(&messages);
        messages.drain(0..remove_count);
    }

    messages
}

fn removable_prefix_len(messages: &[Message]) -> usize {
    if messages.len() >= 2 && messages[0].role == "user" && messages[1].role == "assistant" {
        2
    } else {
        1
    }
}

fn cosine_similarity(left: &[f32], right: &[f32]) -> f32 {
    if left.len() != right.len() || left.is_empty() {
        return 0.0;
    }

    let dot_product = left
        .iter()
        .zip(right.iter())
        .map(|(left, right)| left * right)
        .sum::<f32>();
    let left_norm = left.iter().map(|value| value * value).sum::<f32>().sqrt();
    let right_norm = right.iter().map(|value| value * value).sum::<f32>().sqrt();

    if left_norm == 0.0 || right_norm == 0.0 {
        if left
            .iter()
            .zip(right.iter())
            .all(|(left, right)| left == right)
        {
            1.0
        } else {
            0.0
        }
    } else {
        dot_product / (left_norm * right_norm)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[tokio::test]
    async fn random_embedding_provider_returns_expected_shape() {
        let provider = RandomEmbeddingProvider::default();
        let inputs = vec!["hello".to_string(), "world".to_string()];

        let embeddings = provider.embed(&inputs).await.unwrap();

        assert_eq!(embeddings.len(), 2);
        assert!(embeddings.iter().all(|embedding| embedding.len() == 1536));
        assert!(embeddings.iter().all(|embedding| {
            embedding
                .iter()
                .all(|value| value.is_finite() && *value >= 0.0 && *value < 1.0)
        }));
    }

    #[tokio::test]
    async fn in_memory_vector_store_supports_insert_search_and_delete() {
        let store = InMemoryVectorStore::default();

        store
            .insert("session-1", "hello", vec![0.0; 1536], "message-1")
            .await
            .unwrap();

        let results = store.search("session-1", &[0.0; 1536], 5).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].text, "hello");

        store.delete_session("session-1").await.unwrap();

        let results = store.search("session-1", &[0.0; 1536], 5).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn in_memory_store_adds_messages_and_returns_recent_slice() {
        let store = InMemoryStore::default();

        store
            .add_message("session-1", message("user", "first"))
            .await
            .unwrap();
        store
            .add_message("session-1", message("assistant", "second"))
            .await
            .unwrap();
        store
            .add_message("session-1", message("user", "third"))
            .await
            .unwrap();

        let recent = store.get_recent("session-1", 2).await.unwrap();

        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].content, "second");
        assert_eq!(recent[1].content, "third");
    }

    #[tokio::test]
    async fn in_memory_store_trim_keeps_latest_messages() {
        let store = InMemoryStore::default();

        store
            .add_message("session-1", message("user", "first"))
            .await
            .unwrap();
        store
            .add_message("session-1", message("assistant", "second"))
            .await
            .unwrap();
        store
            .add_message("session-1", message("user", "third"))
            .await
            .unwrap();

        store.trim("session-1", 2).await.unwrap();

        let recent = store.get_recent("session-1", 10).await.unwrap();
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].content, "second");
        assert_eq!(recent[1].content, "third");
    }

    #[tokio::test]
    async fn in_memory_store_trim_to_token_budget_preserves_pairs() {
        let store = InMemoryStore::default();
        let token_counter = OpenAITokenCounter::new().unwrap();

        store
            .add_message("session-1", message("user", "aaa"))
            .await
            .unwrap();
        store
            .add_message("session-1", message("assistant", "bbb"))
            .await
            .unwrap();
        store
            .add_message("session-1", message("user", "cc"))
            .await
            .unwrap();
        store
            .add_message("session-1", message("assistant", "dd"))
            .await
            .unwrap();
        store
            .add_message("session-1", message("user", "eee"))
            .await
            .unwrap();

        let max_tokens = token_counter.count_tokens(&conversation_text(&[
            message("user", "cc"),
            message("assistant", "dd"),
            message("user", "eee"),
        ]));

        let trimmed = store
            .trim_to_token_budget("session-1", max_tokens, &token_counter)
            .await
            .unwrap();

        assert_eq!(trimmed.len(), 3);
        assert_eq!(trimmed[0].role, "user");
        assert_eq!(trimmed[0].content, "cc");
        assert_eq!(trimmed[1].role, "assistant");
        assert_eq!(trimmed[1].content, "dd");
        assert_eq!(trimmed[2].role, "user");
        assert_eq!(trimmed[2].content, "eee");
    }

    #[tokio::test]
    async fn in_memory_store_never_orphans_assistant_message_when_budget_is_tight() {
        let store = InMemoryStore::default();
        let token_counter = OpenAITokenCounter::new().unwrap();

        store
            .add_message("session-1", message("user", "aa"))
            .await
            .unwrap();
        store
            .add_message("session-1", message("assistant", "bb"))
            .await
            .unwrap();
        store
            .add_message("session-1", message("user", "cccccc"))
            .await
            .unwrap();
        store
            .add_message("session-1", message("assistant", "d"))
            .await
            .unwrap();

        let trimmed = store
            .trim_to_token_budget("session-1", 1, &token_counter)
            .await
            .unwrap();

        assert!(trimmed.is_empty());
    }

    #[test]
    fn openai_token_counter_matches_known_counts() {
        let counter = OpenAITokenCounter::new().unwrap();

        assert_eq!(counter.count_tokens(""), 0);
        assert_eq!(counter.count_tokens("Hello world"), 2);
        assert_eq!(counter.count_tokens("Hello, world!"), 4);
    }

    #[tokio::test]
    async fn in_memory_core_memory_store_adds_and_isolates_facts_by_session() {
        let store = InMemoryCoreMemoryStore::default();

        store
            .add_fact("session-1", "User name is Alex")
            .await
            .unwrap();
        store
            .add_fact("session-2", "User prefers light mode")
            .await
            .unwrap();

        let facts_session_1 = store.get_facts("session-1").await.unwrap();
        let facts_session_2 = store.get_facts("session-2").await.unwrap();

        assert_eq!(facts_session_1, vec!["User name is Alex".to_string()]);
        assert_eq!(facts_session_2, vec!["User prefers light mode".to_string()]);
    }

    #[tokio::test]
    async fn in_memory_core_memory_store_delete_session_removes_facts() {
        let store = InMemoryCoreMemoryStore::default();

        store
            .add_fact("session-1", "User prefers dark mode")
            .await
            .unwrap();

        store.delete_session("session-1").await.unwrap();

        let facts = store.get_facts("session-1").await.unwrap();
        assert!(facts.is_empty());
    }

    #[tokio::test]
    async fn in_memory_store_dump_and_restore_all_sessions() {
        let store = InMemoryStore::default();
        store.add_message("s1", message("user", "hi")).await.unwrap();
        store.add_message("s2", message("user", "yo")).await.unwrap();

        let dump = store.dump_all().await.unwrap();
        assert_eq!(dump.len(), 2);

        let fresh = InMemoryStore::default();
        // Pre-existing data in `fresh` must be wiped by restore_all.
        fresh.add_message("stale", message("user", "old")).await.unwrap();
        fresh.restore_all(dump).await.unwrap();

        assert!(fresh.get_recent("stale", 10).await.unwrap().is_empty());
        assert_eq!(fresh.get_recent("s1", 10).await.unwrap()[0].content, "hi");
        assert_eq!(fresh.get_recent("s2", 10).await.unwrap()[0].content, "yo");
    }

    #[tokio::test]
    async fn restore_all_empty_clears_everything() {
        let store = InMemoryStore::default();
        store.add_message("s1", message("user", "hi")).await.unwrap();
        store.restore_all(vec![]).await.unwrap();
        assert!(store.get_recent("s1", 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn in_memory_core_memory_dump_and_restore() {
        let store = InMemoryCoreMemoryStore::default();
        store.add_fact("s1", "a").await.unwrap();
        store.add_fact("s1", "b").await.unwrap();
        let dump = store.dump_all().await.unwrap();

        let fresh = InMemoryCoreMemoryStore::default();
        fresh.restore_all(dump).await.unwrap();
        assert_eq!(fresh.get_facts("s1").await.unwrap(), vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn memory_server_error_wraps_each_trait_error_type() {
        let embed_error = MemoryServerError::from(EmbedError::Message("embed".to_string()));
        let store_error = MemoryServerError::from(StoreError::Message("store".to_string()));
        let memory_error = MemoryServerError::from(MemoryError::Message("memory".to_string()));

        assert_eq!(embed_error.to_string(), "embed");
        assert_eq!(store_error.to_string(), "store");
        assert_eq!(memory_error.to_string(), "memory");
    }

    #[tokio::test]
    async fn in_memory_store_remove_messages_by_id() {
        let store = InMemoryStore::default();
        let mut a = message("user", "first");  a.id = Some("m1".into());
        let mut b = message("assistant", "second"); b.id = Some("m2".into());
        let mut c = message("user", "third");   c.id = Some("m3".into());
        store.add_message("s1", a).await.unwrap();
        store.add_message("s1", b).await.unwrap();
        store.add_message("s1", c).await.unwrap();

        store.remove_messages("s1", &["m1".into(), "m2".into()]).await.unwrap();

        let recent = store.get_recent("s1", 10).await.unwrap();
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].content, "third");
    }
}
