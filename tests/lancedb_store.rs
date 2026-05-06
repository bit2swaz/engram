use engram::core::VectorStore;
use engram::stores::LanceDBStore;
use lancedb::connect;
use tempfile::TempDir;

const EMBEDDING_DIMENSION: usize = 1536;

fn embedding(value: f32) -> Vec<f32> {
    vec![value; EMBEDDING_DIMENSION]
}

#[tokio::test]
async fn lancedb_store_insert_and_search_returns_matching_text() {
    let temp_dir = TempDir::new().unwrap();
    let store = LanceDBStore::connect(temp_dir.path(), EMBEDDING_DIMENSION)
        .await
        .unwrap();

    store
        .insert("session-a", "hello memory", embedding(1.0), "message-1")
        .await
        .unwrap();

    let results = store.search("session-a", &embedding(1.0), 5).await.unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].text, "hello memory");
    assert!(results[0].score > 0.99);
}

#[tokio::test]
async fn lancedb_store_insert_is_idempotent_by_message_id() {
    let temp_dir = TempDir::new().unwrap();
    let store = LanceDBStore::connect(temp_dir.path(), EMBEDDING_DIMENSION)
        .await
        .unwrap();

    store
        .insert("session-a", "original", embedding(1.0), "abc123")
        .await
        .unwrap();
    store
        .insert("session-a", "replacement", embedding(2.0), "abc123")
        .await
        .unwrap();

    let results = store
        .search("session-a", &embedding(1.0), 10)
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].text, "original");
}

#[tokio::test]
async fn lancedb_store_search_is_isolated_by_session() {
    let temp_dir = TempDir::new().unwrap();
    let store = LanceDBStore::connect(temp_dir.path(), EMBEDDING_DIMENSION)
        .await
        .unwrap();

    store
        .insert("session-a", "session-a-text", embedding(1.0), "message-1")
        .await
        .unwrap();
    store
        .insert("session-b", "session-b-text", embedding(1.0), "message-2")
        .await
        .unwrap();

    let results = store
        .search("session-a", &embedding(1.0), 10)
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].text, "session-a-text");
}

#[tokio::test]
async fn lancedb_store_delete_session_removes_rows() {
    let temp_dir = TempDir::new().unwrap();
    let store = LanceDBStore::connect(temp_dir.path(), EMBEDDING_DIMENSION)
        .await
        .unwrap();

    store
        .insert("session-a", "to-delete", embedding(1.0), "message-1")
        .await
        .unwrap();

    store.delete_session("session-a").await.unwrap();

    let results = store
        .search("session-a", &embedding(1.0), 10)
        .await
        .unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn lancedb_store_constructor_creates_memories_table() {
    let temp_dir = TempDir::new().unwrap();
    let _store = LanceDBStore::connect(temp_dir.path(), EMBEDDING_DIMENSION)
        .await
        .unwrap();

    let db = connect(temp_dir.path().to_str().unwrap())
        .execute()
        .await
        .unwrap();
    let table_names = db.table_names().execute().await.unwrap();

    assert!(table_names.iter().any(|name| name == "memories"));
}

#[tokio::test]
async fn lancedb_store_rejects_query_embedding_with_wrong_dimension() {
    let temp_dir = TempDir::new().unwrap();
    let store = LanceDBStore::connect(temp_dir.path(), 384).await.unwrap();

    let error = store.search("session-a", &embedding(1.0), 5).await.unwrap_err();

    assert!(error.to_string().contains("query embedding must have dimension 384"));
}
