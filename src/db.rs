use crate::session::{SessionFile, Sid};
use crate::timestamp::Timestamp;
use crate::vfs::VirtualDir;
use bytes::{Bytes, BytesMut};
use dashmap::{DashMap, Entry};
use futures_lite::StreamExt;
use std::cmp::Ordering;
use std::str::FromStr;
use std::sync::Arc;
use tokio::pin;

pub type Pid = Arc<str>;

pub struct Db<D: VirtualDir> {
    entries: Arc<DashMap<Bytes, DbEntry>>,
    root: D,
    pid: Pid,
}

impl<D: VirtualDir> Db<D> {
    pub async fn open_write<S: Into<Pid>>(pid: S, root: D) -> crate::Result<Self> {
        let pid = pid.into();
        let db = Db {
            entries: DashMap::new().into(),
            root,
            pid,
        };
        db.restore().await?;
        Ok(db)
    }

    pub async fn insert<K, V>(&self, key: K, value: V) -> crate::Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        todo!()
    }

    pub async fn get<K>(&self, key: K) -> crate::Result<Option<Bytes>>
    where
        K: AsRef<[u8]>,
    {
        todo!()
    }

    pub async fn remove<K>(&self, key: K) -> crate::Result<()>
    where
        K: AsRef<[u8]>,
    {
        todo!()
    }

    async fn restore(&self) -> crate::Result<()> {
        let mut key = BytesMut::new();
        let stream = self.root.list_files();
        pin!(stream);
        while let Some(res) = stream.next().await {
            let pid = Pid::from(res?);
            let subdir = D::open_dir(&pid).await?;
            let files = subdir.list_files();
            pin!(files);
            while let Some(res) = files.next().await {
                let file_name = res?;
                let timestamp = Timestamp::from_str(&file_name)?;
                let file = subdir.read_file(&file_name).await?;
                let mut session = SessionFile::new(pid.clone(), timestamp, file).await?;
                while let Ok(entry) = session.next_entry(&mut key, None).await {
                    match self.entries.entry(key.clone().freeze()) {
                        Entry::Occupied(mut e) => {
                            let existing = e.get();
                            if entry > *existing {
                                // conflict resolution - keep the entry with the latest timestamp
                                e.insert(entry);
                            }
                        }
                        Entry::Vacant(e) => {
                            e.insert(entry);
                        }
                    }
                    key.clear(); // clear the key for the next iteration
                }
            }
        }
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
