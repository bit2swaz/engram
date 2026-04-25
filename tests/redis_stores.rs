use chrono::Utc;
use engram::core::{CoreMemoryStore, ShortTermMemory, TokenCounter};
use engram::models::{EmbeddingStatus, Message};
use engram::stores::{RedisCoreMemoryStore, RedisShortTermMemory};
use redis::Client;
use testcontainers::{
    GenericImage,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

const REDIS_PORT: u16 = 6379;

struct OneTokenPerMessage;

impl TokenCounter for OneTokenPerMessage {
    fn count_tokens(&self, text: &str) -> usize {
        text.split_whitespace().count().max(1)
    }
}

fn message(role: &str, content: &str) -> Message {
    Message {
        id: None,
        role: role.to_string(),
        content: content.to_string(),
        timestamp: Some(Utc::now()),
        embedding_status: Some(EmbeddingStatus::Pending),
    }
}

async fn start_redis() -> (testcontainers::ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("redis", "7.2.4")
        .with_exposed_port(REDIS_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
        .start()
        .await
        .unwrap();

    let host = container.get_host().await.unwrap();
    let port = container
        .get_host_port_ipv4(REDIS_PORT.tcp())
        .await
        .unwrap();

    (container, format!("redis://{host}:{port}/"))
}

#[tokio::test]
async fn redis_short_term_memory_adds_and_returns_recent_messages() {
    let (_container, redis_url) = start_redis().await;
    let store = RedisShortTermMemory::connect(&redis_url).await.unwrap();

    store
        .add_message("session-1", message("user", "hello"))
        .await
        .unwrap();
    store
        .add_message("session-1", message("assistant", "world"))
        .await
        .unwrap();
    store
        .add_message("session-1", message("user", "again"))
        .await
        .unwrap();

    let recent = store.get_recent("session-1", 2).await.unwrap();

    assert_eq!(recent.len(), 2);
    assert_eq!(recent[0].role, "assistant");
    assert_eq!(recent[0].content, "world");
    assert_eq!(recent[1].role, "user");
    assert_eq!(recent[1].content, "again");
}

#[tokio::test]
async fn redis_short_term_memory_trim_keeps_latest_messages() {
    let (_container, redis_url) = start_redis().await;
    let store = RedisShortTermMemory::connect(&redis_url).await.unwrap();

    for index in 0..10 {
        store
            .add_message("session-2", message("user", &format!("message-{index}")))
            .await
            .unwrap();
    }

    store.trim("session-2", 5).await.unwrap();

    let recent = store.get_recent("session-2", 10).await.unwrap();
    assert_eq!(recent.len(), 5);
    assert_eq!(recent[0].content, "message-5");
    assert_eq!(recent[4].content, "message-9");
}

#[tokio::test]
async fn redis_short_term_memory_trim_to_token_budget_preserves_pairs_and_updates_store() {
    let (_container, redis_url) = start_redis().await;
    let client = Client::open(redis_url.as_str()).unwrap();
    let connection = client.get_multiplexed_async_connection().await.unwrap();
    let store = RedisShortTermMemory::new(connection);
    let token_counter = OneTokenPerMessage;

    for (role, content) in [
        ("user", "u1"),
        ("assistant", "a1"),
        ("user", "u2"),
        ("assistant", "a2"),
        ("user", "u3"),
        ("assistant", "a3"),
    ] {
        store
            .add_message("session-3", message(role, content))
            .await
            .unwrap();
    }

    let trimmed = store
        .trim_to_token_budget("session-3", 4, &token_counter)
        .await
        .unwrap();

    let trimmed_contents = trimmed
        .iter()
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>();
    assert_eq!(trimmed_contents, vec!["u2", "a2", "u3", "a3"]);
    assert!(
        trimmed.chunks(2).all(|chunk| chunk.len() == 2
            && chunk[0].role == "user"
            && chunk[1].role == "assistant")
    );

    let persisted = store.get_recent("session-3", 10).await.unwrap();
    let persisted_contents = persisted
        .iter()
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>();
    assert_eq!(persisted_contents, vec!["u2", "a2", "u3", "a3"]);
}

#[tokio::test]
async fn redis_short_term_memory_delete_session_removes_messages() {
    let (_container, redis_url) = start_redis().await;
    let store = RedisShortTermMemory::connect(&redis_url).await.unwrap();

    store
        .add_message("session-4", message("user", "temporary"))
        .await
        .unwrap();

    store.delete_session("session-4").await.unwrap();

    let recent = store.get_recent("session-4", 10).await.unwrap();
    assert!(recent.is_empty());
}

#[tokio::test]
async fn redis_core_memory_store_adds_reads_and_deletes_facts() {
    let (_container, redis_url) = start_redis().await;
    let client = Client::open(redis_url.as_str()).unwrap();
    let connection = client.get_multiplexed_async_connection().await.unwrap();
    let store = RedisCoreMemoryStore::new(connection);

    store
        .add_fact("session-5", "User prefers dark mode")
        .await
        .unwrap();
    store
        .add_fact("session-5", "User likes Rust")
        .await
        .unwrap();

    let mut facts = store.get_facts("session-5").await.unwrap();
    facts.sort();
    assert_eq!(
        facts,
        vec![
            "User likes Rust".to_string(),
            "User prefers dark mode".to_string(),
        ]
    );

    store.delete_session("session-5").await.unwrap();

    let facts = store.get_facts("session-5").await.unwrap();
    assert!(facts.is_empty());
}
