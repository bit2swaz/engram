use std::sync::Arc;

use crate::core::{CoreMemoryStore, ShortTermMemory};
use crate::knowledge::graph::KnowledgeGraph;
use crate::raft::snapshot::EngramSnapshot;
use crate::raft::state_machine::EngStateMachineStore;

/// Startup recovery: clear this node's memory stores, then restore the latest
/// persisted snapshot (if any) into the stores, knowledge graph, and the state
/// machine's applied bookkeeping. openraft replays committed log entries after
/// the restored index once `Raft::new` runs.
pub async fn recover_state_machine(
    sm: &EngStateMachineStore,
    short_term: Arc<dyn ShortTermMemory>,
    core_memory: Arc<dyn CoreMemoryStore>,
) -> anyhow::Result<()> {
    // 1. Always flush first so stale state from a prior run cannot bleed through.
    short_term.restore_all(vec![]).await?;
    core_memory.restore_all(vec![]).await?;

    // 2. Load the persisted snapshot, if present.
    let Some((meta, bytes)) = sm.load_snapshot_for_recovery()? else {
        tracing::info!("recovery: no snapshot found; openraft will replay full committed log");
        return Ok(());
    };
    let payload = EngramSnapshot::from_bytes(&bytes)?;

    // 3. Restore stores + graph + applied bookkeeping.
    let st_sessions = payload.short_term.into_iter().map(|s| (s.session_id, s.messages)).collect();
    short_term.restore_all(st_sessions).await?;
    let cm_sessions = payload.core_memory.into_iter().map(|s| (s.session_id, s.facts)).collect();
    core_memory.restore_all(cm_sessions).await?;
    let graph = KnowledgeGraph::from_snapshot(payload.knowledge_graph);
    sm.restore_applied_for_recovery(meta, graph).await;

    tracing::info!("recovery: restored snapshot; openraft will replay log tail");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tokio::sync::{mpsc, RwLock};
    use redb::Database;

    use crate::core::{CoreMemoryStore, InMemoryCoreMemoryStore, InMemoryStore, InMemoryVectorStore, ShortTermMemory};
    use crate::knowledge::graph::KnowledgeGraph;
    use crate::raft::recovery::recover_state_machine;
    use crate::raft::state_machine::EngStateMachineStore;
    use crate::raft::types::MemoryCommand;

    fn build(db: Arc<Database>) -> (EngStateMachineStore, Arc<InMemoryStore>, Arc<InMemoryCoreMemoryStore>, Arc<RwLock<KnowledgeGraph>>) {
        let st = Arc::new(InMemoryStore::default());
        let cm = Arc::new(InMemoryCoreMemoryStore::default());
        let vs = Arc::new(InMemoryVectorStore::default());
        let (etx, _erx) = mpsc::channel(10);
        let (ktx, _krx) = mpsc::channel(10);
        let kg = Arc::new(RwLock::new(KnowledgeGraph::new()));
        let sm = EngStateMachineStore::new(st.clone(), cm.clone(), vs, etx, kg.clone(), ktx, db);
        (sm, st, cm, kg)
    }

    fn cm_as_dyn(cm: &Arc<InMemoryCoreMemoryStore>) -> Arc<dyn CoreMemoryStore> {
        cm.clone()
    }

    #[tokio::test]
    async fn recovery_with_no_snapshot_flushes_stores() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(Database::create(dir.path().join("r.redb")).unwrap());
        let (sm, st, cm, _kg) = build(db.clone());
        st.add_message("stale", crate::models::Message {
            id: Some("x".into()), role: "user".into(), content: "old".into(),
            timestamp: None, embedding_status: None,
        }).await.unwrap();

        recover_state_machine(&sm, st.clone() as Arc<dyn ShortTermMemory>, cm_as_dyn(&cm)).await.unwrap();
        assert!(st.get_recent("stale", 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn recovery_restores_persisted_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(Database::create(dir.path().join("r.redb")).unwrap());

        // Apply a fact and build a snapshot so it's persisted into db.
        let (mut src, _st, _src_cm, _kg) = build(db.clone());
        src.apply_for_test(0, MemoryCommand::AddFact { session_id: "s1".into(), fact: "remember me".into() }).await;
        let mut b = openraft::storage::RaftStateMachine::get_snapshot_builder(&mut src).await;
        openraft::RaftSnapshotBuilder::build_snapshot(&mut b).await.unwrap();

        // Fresh state machine over the same db; recovery should restore the fact.
        let (sm, st, cm, _kg2) = build(db.clone());
        recover_state_machine(&sm, st as Arc<dyn ShortTermMemory>, cm.clone() as Arc<dyn CoreMemoryStore>).await.unwrap();
        assert_eq!(cm.get_facts("s1").await.unwrap(), vec!["remember me".to_string()]);
    }
}
