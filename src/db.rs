use crate::session::Sid;
use crate::session_manager::SessionManager;
use crate::timestamp::Timestamp;
use crate::vfs::VirtualDir;
use bytes::{Bytes, BytesMut};
use dashmap::{DashMap, Entry};
use std::cmp::Ordering;
use std::sync::Arc;
use tokio::sync::Mutex;

pub type Pid = Arc<str>;

pub struct Db<D: VirtualDir> {
    entries: DashMap<Bytes, DbEntry>,
    sessions: Mutex<SessionManager<D>>,
    pid: Pid,
    current_session: Sid,
}

impl<D: VirtualDir> Db<D> {
    pub async fn open_write<S: Into<Pid>>(pid: S, root: D) -> crate::Result<Self> {
        let pid = pid.into();
        let sid = Sid::new(pid.clone(), Timestamp::now());
        let db = Db {
            pid,
            entries: DashMap::new(),
            sessions: SessionManager::new(root).into(),
            current_session: sid,
        };
        db.restore().await?;
        {
            // start a new session
            let mut sessions = db.sessions.lock().await;
            sessions.open(db.current_session.clone(), false).await?;
        }
        Ok(db)
    }

    pub async fn insert<K, V>(&self, key: K, value: V) -> crate::Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let value = value.as_ref();
        let entry = {
            let mut lock = self.sessions.lock().await;
            let session = lock.open(self.current_session.clone(), true).await?;
            session.append_entry(key, value).await?
        };
        Self::merge(&self.entries, Bytes::copy_from_slice(key), entry);
        Ok(())
    }

    pub async fn get<K>(&self, key: K) -> crate::Result<Option<Bytes>>
    where
        K: AsRef<[u8]>,
    {
        let key = key.as_ref();
        match self.entries.get(key) {
            Some(entry) => {
                let entry = entry.value();

                let mut lock = self.sessions.lock().await;
                let session = lock.open(entry.sid().clone(), false).await?;
                let mut key = BytesMut::new();
                let mut value = BytesMut::new();
                session.read_entry(entry, &mut key, &mut value).await?;
                if value.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(value.freeze()))
                }
            }
            None => Ok(None),
        }
    }

    pub async fn remove<K>(&self, key: K) -> crate::Result<()>
    where
        K: AsRef<[u8]>,
    {
        // remove the entry is the same as inserting an empty value
        self.insert(key, []).await
    }

    fn merge(x: &DashMap<Bytes, DbEntry>, key: Bytes, entry: DbEntry) -> Option<Sid> {
        match x.entry(key) {
            Entry::Occupied(mut e) => {
                let existing = e.get();
                if entry > *existing {
                    // conflict resolution - keep the entry with the latest timestamp
                    let old = e.insert(entry);
                    Some(old.sid) // return the ID of the outdated entry
                } else {
                    Some(entry.sid) // return the ID of the new entry which is outdated
                }
            }
            Entry::Vacant(e) => {
                e.insert(entry);
                None
            }
        }
    }

    async fn restore(&self) -> crate::Result<()> {
        let mut lock = self.sessions.lock().await;
        lock.replay_all(|key, entry| Self::merge(&self.entries, key, entry))
            .await?;
        Ok(())
    }
}

/// Marker informing how to locate a given key-value entry in the database session file.
/// On-disk layout of the entry is as follows:
/// - 4 bytes: total length of the entry in bytes
/// - 8 bytes: timestamp of the entry
/// - 4 bytes: length of the key
/// - N bytes: key
/// - M bytes: value
/// - 4 bytes: checksum that confirms the integrity of the entry
#[derive(Debug, Clone)]
pub struct DbEntry {
    /// Identifier of session holding this entry.
    sid: Sid,
    /// Timestamp of when this entry was created.
    timestamp: Timestamp,
    /// Offset where the entry starts in the session file.
    entry_offset: u64,
    /// Length of the key in bytes.
    key_len: u32,
    /// Total length of an entry in bytes, including metadata.
    total_len: u32,
}

impl DbEntry {
    pub fn new(
        sid: Sid,
        timestamp: Timestamp,
        entry_offset: u64,
        key_len: u32,
        total_len: u32,
    ) -> Self {
        Self {
            sid,
            timestamp,
            entry_offset,
            key_len,
            total_len,
        }
    }

    /// Identifier of a session holding this entry.
    pub fn sid(&self) -> &Sid {
        &self.sid
    }

    /// Entry's own unique identifier.
    pub fn id(&self) -> Sid {
        Sid {
            timestamp: self.timestamp,
            pid: self.sid.pid.clone(),
        }
    }

    /// Timestamp of when this entry was created. Used for conflict resolution.
    pub fn timestamp(&self) -> &Timestamp {
        &self.timestamp
    }

    /// Total length of an entry in bytes, including metadata.
    pub fn len(&self) -> u32 {
        self.total_len
    }

    /// Length of the key in bytes.
    pub fn key_len(&self) -> u32 {
        self.key_len
    }

    /// Length of the value in bytes.
    pub fn value_len(&self) -> u32 {
        self.total_len - self.key_len - 20 // 4B for total_len, 8B for timestamp, 4B for key_len, 4B for checksum
    }

    /// We treat entries with empty values as deletions.
    pub fn is_deleted(&self) -> bool {
        self.value_len() == 0
    }

    /// Offset where the entry starts in the session file.
    pub fn entry_offset(&self) -> u64 {
        self.entry_offset
    }

    /// Offset where the entry's key starts in the session file.
    pub fn key_offset(&self) -> u64 {
        self.entry_offset + 16 // 4 bytes for total_len, 8 bytes for timestamp, 4 bytes for key_len
    }

    /// Offset where the entry's value starts in the session file.
    pub fn value_offset(&self) -> u64 {
        self.key_offset() + self.key_len as u64
    }
}

impl Eq for DbEntry {}
impl PartialEq<Self> for DbEntry {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp && self.sid == other.sid
    }
}

impl PartialOrd for DbEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DbEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // entries can be compared by timestamp and PIDs of sessions that created them
        match self.timestamp.cmp(&other.timestamp) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => self.sid.pid.cmp(&other.sid.pid),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::db::Db;
    use crate::vfs::fs::Dir;

    #[tokio::test]
    async fn insert_get() {
        let _ = env_logger::builder().is_test(true).try_init();
        let temp_dir = tempfile::tempdir().unwrap();
        let root = Dir::new(temp_dir.path());
        let db = Db::open_write("test", root).await.unwrap();

        // init database state
        for i in 0..10 {
            let key = format!("key-{}", i);
            let value = format!("value-{}", i);
            db.insert(key.as_bytes(), value.as_bytes()).await.unwrap();
        }

        // check if all entries are present
        for i in 0..10 {
            let key = format!("key-{}", i);
            let value = format!("value-{}", i);
            let result = db.get(key.as_bytes()).await.unwrap().unwrap();
            assert_eq!(value.as_bytes(), &result[..]);
        }
    }
}
