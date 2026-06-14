use std::io;
use std::ops::RangeBounds;
use std::sync::Arc;
use openraft::{
    LogId, LogState, RaftLogReader, Vote, Entry,
    ErrorSubject, ErrorVerb,
    storage::{LogFlushed, RaftLogStorage},
    StorageError,
};
use redb::{Database, ReadableTable, TableDefinition};

use crate::raft::types::TypeConfig;

const LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");
const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");

const META_VOTE: &str = "vote";
const META_COMMITTED: &str = "committed";
const META_LAST_PURGED: &str = "last_purged";

#[derive(Clone)]
pub struct EngRaftLogStore {
    db: Arc<Database>,
}

impl EngRaftLogStore {
    pub fn new(db: Arc<Database>) -> Self {
        // Ensure both tables exist so read txns never fail on a fresh db.
        let txn = db.begin_write().expect("redb begin_write on init");
        {
            let _ = txn.open_table(LOG_TABLE).expect("open LOG_TABLE");
            let _ = txn.open_table(META_TABLE).expect("open META_TABLE");
        }
        txn.commit().expect("redb commit on init");
        Self { db }
    }

    fn read_meta(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError<u64>> {
        let txn = self.db.begin_read().map_err(read_err)?;
        let table = txn.open_table(META_TABLE).map_err(read_err)?;
        Ok(table.get(key).map_err(read_err)?.map(|v| v.value().to_vec()))
    }

    fn write_meta(&self, key: &str, bytes: &[u8]) -> Result<(), StorageError<u64>> {
        let txn = self.db.begin_write().map_err(write_err)?;
        {
            let mut table = txn.open_table(META_TABLE).map_err(write_err)?;
            table.insert(key, bytes).map_err(write_err)?;
        }
        txn.commit().map_err(write_err)?;
        Ok(())
    }
}

fn read_err<E: std::error::Error + Send + Sync + 'static>(e: E) -> StorageError<u64> {
    StorageError::from_io_error(
        ErrorSubject::Store,
        ErrorVerb::Read,
        io::Error::new(io::ErrorKind::Other, e.to_string()),
    )
}

fn write_err<E: std::error::Error + Send + Sync + 'static>(e: E) -> StorageError<u64> {
    StorageError::from_io_error(
        ErrorSubject::Store,
        ErrorVerb::Write,
        io::Error::new(io::ErrorKind::Other, e.to_string()),
    )
}

fn decode_err(e: serde_json::Error) -> StorageError<u64> {
    StorageError::from_io_error(
        ErrorSubject::Logs,
        ErrorVerb::Read,
        io::Error::new(io::ErrorKind::InvalidData, e.to_string()),
    )
}

impl RaftLogReader<TypeConfig> for EngRaftLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        let txn = self.db.begin_read().map_err(read_err)?;
        let table = txn.open_table(LOG_TABLE).map_err(read_err)?;
        let mut out = Vec::new();
        for item in table.range(range).map_err(read_err)? {
            let (_, value) = item.map_err(read_err)?;
            let entry: Entry<TypeConfig> =
                serde_json::from_slice(value.value()).map_err(decode_err)?;
            out.push(entry);
        }
        Ok(out)
    }
}

impl RaftLogStorage<TypeConfig> for EngRaftLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let last_purged: Option<LogId<u64>> = match self.read_meta(META_LAST_PURGED)? {
            Some(b) => Some(serde_json::from_slice(&b).map_err(decode_err)?),
            None => None,
        };
        let txn = self.db.begin_read().map_err(read_err)?;
        let table = txn.open_table(LOG_TABLE).map_err(read_err)?;
        let last = match table.last().map_err(read_err)? {
            Some((_, value)) => {
                let entry: Entry<TypeConfig> =
                    serde_json::from_slice(value.value()).map_err(decode_err)?;
                Some(entry.log_id)
            }
            None => last_purged.clone(),
        };
        Ok(LogState { last_purged_log_id: last_purged, last_log_id: last })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        let bytes = serde_json::to_vec(&committed).map_err(write_err)?;
        self.write_meta(META_COMMITTED, &bytes)
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        match self.read_meta(META_COMMITTED)? {
            Some(b) => Ok(serde_json::from_slice(&b).map_err(decode_err)?),
            None => Ok(None),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let bytes = serde_json::to_vec(vote).map_err(write_err)?;
        self.write_meta(META_VOTE, &bytes)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        match self.read_meta(META_VOTE)? {
            Some(b) => Ok(Some(serde_json::from_slice(&b).map_err(decode_err)?)),
            None => Ok(None),
        }
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let txn = self.db.begin_write().map_err(write_err)?;
        {
            let mut table = txn.open_table(LOG_TABLE).map_err(write_err)?;
            for entry in entries {
                let bytes = serde_json::to_vec(&entry).map_err(write_err)?;
                table.insert(entry.log_id.index, bytes.as_slice()).map_err(write_err)?;
            }
        }
        // DURABILITY HINGE: commit (fsync) BEFORE signaling completion to openraft.
        txn.commit().map_err(write_err)?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let txn = self.db.begin_write().map_err(write_err)?;
        {
            let mut table = txn.open_table(LOG_TABLE).map_err(write_err)?;
            table.retain(|k, _| k < log_id.index).map_err(write_err)?;
        }
        txn.commit().map_err(write_err)?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let bytes = serde_json::to_vec(&log_id).map_err(write_err)?;
        self.write_meta(META_LAST_PURGED, &bytes)?;
        let txn = self.db.begin_write().map_err(write_err)?;
        {
            let mut table = txn.open_table(LOG_TABLE).map_err(write_err)?;
            table.retain(|k, _| k > log_id.index).map_err(write_err)?;
        }
        txn.commit().map_err(write_err)?;
        Ok(())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

#[cfg(test)]
impl EngRaftLogStore {
    /// Test helper: insert entries directly via a redb write txn, bypassing the
    /// LogFlushed callback (which is pub(crate) in openraft and not constructible here).
    async fn insert_for_test(&self, entries: Vec<Entry<TypeConfig>>) {
        let txn = self.db.begin_write().unwrap();
        {
            let mut table = txn.open_table(LOG_TABLE).unwrap();
            for entry in entries {
                let bytes = serde_json::to_vec(&entry).unwrap();
                table.insert(entry.log_id.index, bytes.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::{CommittedLeaderId, EntryPayload, LogState};

    fn temp_store() -> (EngRaftLogStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join("test.redb")).unwrap();
        (EngRaftLogStore::new(std::sync::Arc::new(db)), dir)
    }

    fn log_id(term: u64, index: u64) -> LogId<u64> {
        LogId::new(CommittedLeaderId::new(term, 1), index)
    }

    fn blank(term: u64, index: u64) -> Entry<TypeConfig> {
        Entry { log_id: log_id(term, index), payload: EntryPayload::Blank }
    }

    #[tokio::test]
    async fn initial_log_state_is_empty() {
        let (mut store, _d) = temp_store();
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state, LogState { last_purged_log_id: None, last_log_id: None });
    }

    #[tokio::test]
    async fn save_and_read_vote() {
        let (mut store, _d) = temp_store();
        assert!(store.read_vote().await.unwrap().is_none());
        let vote = Vote::new(1, 1);
        store.save_vote(&vote).await.unwrap();
        assert_eq!(store.read_vote().await.unwrap(), Some(vote));
    }

    #[tokio::test]
    async fn save_and_read_committed() {
        let (mut store, _d) = temp_store();
        assert!(store.read_committed().await.unwrap().is_none());
        let lid = log_id(1, 5);
        store.save_committed(Some(lid.clone())).await.unwrap();
        assert_eq!(store.read_committed().await.unwrap(), Some(lid));
    }

    #[tokio::test]
    async fn append_and_read_back() {
        let (mut store, _d) = temp_store();
        store.insert_for_test(vec![blank(1, 0), blank(1, 1), blank(1, 2)]).await;
        let got = store.try_get_log_entries(0..3).await.unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].log_id.index, 0);
        assert_eq!(got[2].log_id.index, 2);
    }

    #[tokio::test]
    async fn truncate_removes_from_index_inclusive() {
        let (mut store, _d) = temp_store();
        store.insert_for_test(vec![blank(1, 0), blank(1, 1), blank(1, 2), blank(1, 3)]).await;
        store.truncate(log_id(1, 2)).await.unwrap();
        let got = store.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(got.len(), 2, "entries 0 and 1 should remain");
        assert_eq!(got[1].log_id.index, 1);
    }

    #[tokio::test]
    async fn purge_removes_up_to_inclusive_and_updates_last_purged() {
        let (mut store, _d) = temp_store();
        store.insert_for_test(vec![blank(1, 0), blank(1, 1), blank(1, 2)]).await;
        store.purge(log_id(1, 1)).await.unwrap();
        let got = store.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(got.len(), 1, "only entry at index 2 should remain");
        assert_eq!(got[0].log_id.index, 2);
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(log_id(1, 1)));
    }

    #[tokio::test]
    async fn data_survives_database_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("persist.redb");
        {
            let db = std::sync::Arc::new(Database::create(&path).unwrap());
            let mut store = EngRaftLogStore::new(db);
            store.insert_for_test(vec![blank(2, 0), blank(2, 1)]).await;
            store.save_vote(&Vote::new(2, 1)).await.unwrap();
            store.purge(log_id(2, 0)).await.unwrap();
        }
        // Reopen the same file with a brand-new Database handle.
        let db = std::sync::Arc::new(Database::open(&path).unwrap());
        let mut store = EngRaftLogStore::new(db);
        let got = store.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].log_id.index, 1);
        assert_eq!(store.read_vote().await.unwrap(), Some(Vote::new(2, 1)));
        assert_eq!(
            store.get_log_state().await.unwrap().last_purged_log_id,
            Some(log_id(2, 0))
        );
    }
}
