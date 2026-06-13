use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::mpsc;

use engram::app::build_raft_node;
use engram::config::Config;
use engram::core::{InMemoryCoreMemoryStore, InMemoryStore, InMemoryVectorStore, ShortTermMemory};
use engram::raft::types::{MemoryCommand, MessagePayload};
use tempfile;

#[tokio::test]
async fn single_node_raft_write_commits_to_state_machine() {
    let short_term = Arc::new(InMemoryStore::default());
    let (tx, _rx) = mpsc::channel(10);
    let raft_dir = tempfile::tempdir().unwrap();
    let config = Config {
        node_id: Some(1),
        raft_db_path: raft_dir.path().join("engram.redb"),
        ..Config::default()
    };
    let knowledge_graph = Arc::new(tokio::sync::RwLock::new(engram::knowledge::graph::KnowledgeGraph::new()));
    let (knowledge_tx, _knowledge_rx) = mpsc::channel(500);
    let raft = build_raft_node(
        &config,
        short_term.clone(),
        Arc::new(InMemoryCoreMemoryStore::default()),
        Arc::new(InMemoryVectorStore::default()),
        tx,
        knowledge_graph,
        knowledge_tx,
    )
    .await
    .unwrap();

    let mut members = BTreeMap::new();
    members.insert(1u64, openraft::BasicNode::new("127.0.0.1:0"));
    raft.initialize(members).await.unwrap();

    // Wait for leader election.
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(raft.current_leader().await, Some(1));

    raft.client_write(MemoryCommand::AddMessage {
        session_id: "s1".to_string(),
        message: MessagePayload {
            id: "m1".to_string(),
            role: "user".to_string(),
            content: "confirmed via raft".to_string(),
            timestamp: Utc::now(),
        },
    })
    .await
    .unwrap();

    let msgs = short_term.get_recent("s1", 10).await.unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].content, "confirmed via raft");
}
