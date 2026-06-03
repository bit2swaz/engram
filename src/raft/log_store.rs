use std::collections::BTreeMap;
use std::ops::RangeBounds;
use std::sync::Arc;
use tokio::sync::Mutex;
use openraft::{
    LogId, LogState, RaftLogReader, Vote, Entry,
    storage::{LogFlushed, RaftLogStorage},
    StorageError,
};

use crate::raft::types::TypeConfig;

#[derive(Debug, Default, Clone)]
pub struct EngRaftLogStore {
    inner: Arc<Mutex<LogStoreInner>>,
}

#[derive(Debug, Default)]
struct LogStoreInner {
    last_purged_log_id: Option<LogId<u64>>,
    log: BTreeMap<u64, Entry<TypeConfig>>,
    committed: Option<LogId<u64>>,
    vote: Option<Vote<u64>>,
}

impl RaftLogReader<TypeConfig> for EngRaftLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        let inner = self.inner.lock().await;
        Ok(inner.log.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogStorage<TypeConfig> for EngRaftLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let inner = self.inner.lock().await;
        let last = inner
            .log
            .values()
            .next_back()
            .map(|e| e.log_id.clone())
            .or_else(|| inner.last_purged_log_id.clone());
        Ok(LogState {
            last_purged_log_id: inner.last_purged_log_id.clone(),
            last_log_id: last,
        })
    }

    async fn save_committed(&mut self, committed: Option<LogId<u64>>) -> Result<(), StorageError<u64>> {
        self.inner.lock().await.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.inner.lock().await.committed.clone())
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.inner.lock().await.vote = Some(vote.clone());
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.inner.lock().await.vote.clone())
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TypeConfig>) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        {
            let mut inner = self.inner.lock().await;
            for entry in entries {
                inner.log.insert(entry.log_id.index, entry);
            }
        }
        // In-memory: flush is instantaneous. Signal completion immediately.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().await;
        // Remove all entries with index >= log_id.index (inclusive).
        let keys: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().await;
        inner.last_purged_log_id = Some(log_id.clone());
        let keys: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        Ok(())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

#[cfg(test)]
impl EngRaftLogStore {
    /// Test helper: insert entries directly into the log without going through
    /// the LogFlushed callback (which is pub(crate) in openraft and cannot be
    /// constructed in external test code).
    async fn insert_for_test(&self, entries: Vec<Entry<TypeConfig>>) {
        let mut inner = self.inner.lock().await;
        for entry in entries {
            inner.log.insert(entry.log_id.index, entry);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::{CommittedLeaderId, EntryPayload, LogState};

    fn log_id(term: u64, index: u64) -> LogId<u64> {
        LogId::new(CommittedLeaderId::new(term, 1), index)
    }

    fn blank(term: u64, index: u64) -> Entry<TypeConfig> {
        Entry { log_id: log_id(term, index), payload: EntryPayload::Blank }
    }

    #[tokio::test]
    async fn initial_log_state_is_empty() {
        let mut store = EngRaftLogStore::default();
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state, LogState { last_purged_log_id: None, last_log_id: None });
    }

    #[tokio::test]
    async fn save_and_read_vote() {
        let mut store = EngRaftLogStore::default();
        assert!(store.read_vote().await.unwrap().is_none());
        let vote = Vote::new(1, 1);
        store.save_vote(&vote).await.unwrap();
        assert_eq!(store.read_vote().await.unwrap(), Some(vote));
    }

    #[tokio::test]
    async fn save_and_read_committed() {
        let mut store = EngRaftLogStore::default();
        assert!(store.read_committed().await.unwrap().is_none());
        let lid = log_id(1, 5);
        store.save_committed(Some(lid.clone())).await.unwrap();
        assert_eq!(store.read_committed().await.unwrap(), Some(lid));
    }

    #[tokio::test]
    async fn append_and_read_back() {
        let mut store = EngRaftLogStore::default();
        store.insert_for_test(vec![blank(1, 0), blank(1, 1), blank(1, 2)]).await;
        let got = store.try_get_log_entries(0..3).await.unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].log_id.index, 0);
        assert_eq!(got[2].log_id.index, 2);
    }

    #[tokio::test]
    async fn truncate_removes_from_index_inclusive() {
        let mut store = EngRaftLogStore::default();
        store.insert_for_test(vec![blank(1, 0), blank(1, 1), blank(1, 2), blank(1, 3)]).await;

        store.truncate(log_id(1, 2)).await.unwrap();

        let got = store.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(got.len(), 2, "entries 0 and 1 should remain");
        assert_eq!(got[1].log_id.index, 1);
    }

    #[tokio::test]
    async fn purge_removes_up_to_inclusive_and_updates_last_purged() {
        let mut store = EngRaftLogStore::default();
        store.insert_for_test(vec![blank(1, 0), blank(1, 1), blank(1, 2)]).await;

        store.purge(log_id(1, 1)).await.unwrap();

        let got = store.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(got.len(), 1, "only entry at index 2 should remain");
        assert_eq!(got[0].log_id.index, 2);

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(log_id(1, 1)));
    }

    #[tokio::test]
    async fn log_state_last_log_id_falls_back_to_last_purged_when_log_is_empty() {
        let mut store = EngRaftLogStore::default();
        store.insert_for_test(vec![blank(1, 0)]).await;
        store.purge(log_id(1, 0)).await.unwrap();

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(log_id(1, 0)));
        assert_eq!(state.last_log_id, Some(log_id(1, 0)));
    }

    #[tokio::test]
    async fn get_log_reader_is_a_clone_that_reads_same_data() {
        let mut store = EngRaftLogStore::default();
        store.insert_for_test(vec![blank(1, 0)]).await;
        let mut reader = store.get_log_reader().await;
        let got = reader.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(got.len(), 1);
    }
}
