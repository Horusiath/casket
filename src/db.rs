use crate::session::{SessionFile, Sid};
use crate::timestamp::Timestamp;
use crate::vfs::{VirtualDir, VirtualFile};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use dashmap::mapref::one::RefMut;
use dashmap::{DashMap, Entry, OccupiedEntry};
use futures_lite::StreamExt;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Error, ErrorKind, SeekFrom};
use std::ops::{Deref, DerefMut};
use std::str::FromStr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::pin;
use tokio::sync::Mutex;

pub type Pid = Arc<str>;

pub struct Db<D: VirtualDir> {
    /// In BitCask architecture entries are stored in memory, while their values are persisted on disk.
    entries: DashMap<Bytes, DbEntry>,
    /// Structure which keeps file handles to opened active sessions. We do that to avoid expensive
    /// file opening every time we need to get data from session files.
    sessions: DashMap<Sid, SessionHandle<D::File>>,
    /// Unique identifier of a current process.
    pid: Pid,
    /// Unique identifier of a currently opened session belonging to this process.
    current_session: Sid,
    /// Root entry to a file system, where current Database data is being stored.
    root: D,
}

impl<D: VirtualDir> Db<D> {
    pub async fn open_write<S: Into<Pid>>(pid: S, root: D) -> crate::Result<Self> {
        let pid = pid.into();
        let sid = Sid::new(pid.clone(), Timestamp::now());
        let mut db = Db {
            pid,
            root,
            current_session: sid.clone(),
            entries: DashMap::new(),
            sessions: DashMap::new(),
        };
        db.restore().await?;
        db.reset_session().await?;
        Ok(db)
    }

    /// Insert a new key-value pair.
    /// This operation doesn't flush the contents to the disk immediately. Use [flush] method to do so.
    pub async fn insert<K, V>(&self, key: K, value: V) -> crate::Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let value = value.as_ref();
        let entry = {
            let mut h = self
                .sessions
                .session_entry(self.current_session.clone())
                .or_write(&self.root)
                .await?;
            let e = h.session.append_entry(key, value).await?;
            h.inc_ref(); // increment number of entries using this session
            e
        };
        if let Some(sid) = self.entries.merge(Bytes::copy_from_slice(key), entry) {
            if let Entry::Occupied(mut session) = self.sessions.entry(sid) {
                if session.get_mut().dec_ref() {
                    // remove session handle if there's no more entries pointing to it
                    session.remove();
                }
            }
        }
        Ok(())
    }

    /// Reads the latest value for a given key-value pair. Returns None if key was not found or
    /// entry has been deleted.
    pub async fn get<K>(&self, key: K) -> crate::Result<Option<Bytes>>
    where
        K: AsRef<[u8]>,
    {
        let key = key.as_ref();
        match self.entries.get(key) {
            Some(e) => {
                let mut h = self
                    .sessions
                    .session_entry(e.sid.clone())
                    .or_read(&self.root)
                    .await?;
                let mut key = BytesMut::new();
                let mut value = BytesMut::new();
                h.session.read_entry(&*e, &mut key, &mut value).await?;
                if value.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(value.freeze()))
                }
            }
            None => Ok(None),
        }
    }

    /// Remove a key-value entry for a given key.
    pub async fn remove<K>(&self, key: K) -> crate::Result<()>
    where
        K: AsRef<[u8]>,
    {
        // remove the entry is the same as inserting an empty value
        self.insert(key, []).await

        //TODO: we could theoretically drop a session ref counter, but that doesn't mean that we
        // can prune the session file
    }

    /// Flushes all pending writes to the disk.
    pub async fn flush(&self) -> crate::Result<()> {
        if let Some(mut h) = self.sessions.get_mut(&self.current_session) {
            h.session.flush().await?;
        }
        Ok(())
    }

    /// Iterate over all available sessions and restore their state into current database.
    async fn restore(&self) -> crate::Result<()> {
        let subdirs = self.root.list_files();
        pin!(subdirs);
        while let Some(subdir) = subdirs.next().await {
            let pid = Pid::from(subdir?);
            let subdir = self.root.open_subdir(&pid).await?;
            let sessions = subdir.list_files();
            pin!(sessions);
            while let Some(res) = sessions.next().await {
                let file_name = res?;
                if file_name == ".head" {
                    continue; //TODO: attach watcher over this pid
                }
                let timestamp = Timestamp::from_str(&file_name)?;
                let sid = Sid::new(pid.clone(), timestamp);
                self.restore_session(sid, 0).await?;
            }
        }
        Ok(())
    }

    async fn restore_session(&self, sid: Sid, from: u64) -> crate::Result<()> {
        tracing::trace!(
            "restoring session {} starting from file position {}",
            sid,
            from
        );
        let mut h = self
            .sessions
            .session_entry(sid.clone())
            .or_read(&self.root)
            .await?;
        h.session.seek(from).await?;
        let mut key_buf = BytesMut::new();
        loop {
            key_buf.clear();
            match h.session.next_entry(&mut key_buf, None).await {
                Ok(entry) => {
                    let key = key_buf.clone().freeze();
                    match self.entries.merge(key, entry) {
                        // current value is in use, increment the reference count for this session file
                        None => h.inc_ref(),
                        Some(sid) if &sid == h.session.sid() => {
                            // current value is outdated, decrement the reference count for this session file
                            h.dec_ref();
                        }
                        // after merge, this value has outdated another session, decrement that session's reference count
                        Some(sid) => {
                            if let Entry::Occupied(mut other) = self.sessions.entry(sid) {
                                if other.get_mut().dec_ref() {
                                    // if number of references reaches zero, remove the session file handle
                                    other.remove();
                                }
                            }
                        }
                    }
                }
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => return Err(err.into()),
            }
        }

        drop(h);
        if let Entry::Occupied(mut e) = self.sessions.entry(sid) {
            // remote session from cache if it has no references
            if e.get().ref_count <= 0 {
                e.remove();
            }
        }
        Ok(())
    }

    /// Gracefully closes current write session for this process and opens a new one.
    pub async fn reset_session(&mut self) -> crate::Result<()> {
        if let Entry::Occupied(mut e) = self.sessions.entry(self.current_session.clone()) {
            tracing::trace!("closing current session {}", e.key());
            // we're going to reset this session, so flush all the data
            let h = e.get_mut();
            h.session.flush().await?;
            if h.dec_ref() {
                // if there are no other references to this session, remove it from the cache
                e.remove();
            }
        }

        self.current_session = Sid::new(self.pid.clone(), Timestamp::now());
        let sid = self.current_session.clone();
        tracing::trace!("initializing new session {}", sid);
        if let Entry::Vacant(e) = self.sessions.entry(sid.clone()) {
            let subdir = self.root.open_subdir(&sid.pid).await?;
            let file = subdir.write_file(&sid.timestamp.to_string()).await?;

            // keep track of current active session
            let mut f = subdir.write_file(".head").await?;
            // reset the file pointer to the beginning and override any existing content
            f.seek(SeekFrom::Start(0)).await?;
            f.write_all(sid.timestamp.to_string().as_bytes()).await?;

            let session = SessionFile::new(sid, file).await?;
            let handle = SessionHandle::new(session, 1);
            e.insert(handle);
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

struct SessionHandle<F> {
    ref_count: isize,
    session: SessionFile<F>,
}

impl<F> SessionHandle<F> {
    fn new(session: SessionFile<F>, ref_count: isize) -> Self {
        Self { ref_count, session }
    }

    fn from(session: SessionFile<F>) -> Self {
        Self::new(session, 1)
    }

    /// Increment number of keys referencing this session file.
    fn inc_ref(&mut self) {
        self.ref_count += 1;
    }

    /// Decrement number of keys referencing this session file. Returns true if the reference count
    /// reaches zero.
    fn dec_ref(&mut self) -> bool {
        self.ref_count -= 1;
        self.ref_count <= 0
    }
}

struct SessionEntry<'a, F> {
    handle: Entry<'a, Sid, SessionHandle<F>>,
}

impl<'a, F> From<Entry<'a, Sid, SessionHandle<F>>> for SessionEntry<'a, F> {
    fn from(handle: Entry<'a, Sid, SessionHandle<F>>) -> Self {
        Self { handle }
    }
}

impl<'a, F> SessionEntry<'a, F>
where
    F: VirtualFile,
{
    async fn or_read<D>(self, root: &D) -> crate::Result<SessionMut<'a, F>>
    where
        D: VirtualDir<File = F>,
    {
        match self.handle {
            Entry::Occupied(e) => Ok(SessionMut {
                handle: e.into_ref(),
            }),
            Entry::Vacant(mut e) => {
                let sid = e.key();
                let subdir = root.open_subdir(&sid.pid).await?;
                let file = subdir.read_file(&sid.timestamp.to_string()).await?;
                let session = SessionFile::new(sid.clone(), file).await?;
                let handle = SessionHandle::new(session, 0);

                Ok(SessionMut {
                    handle: e.insert(handle),
                })
            }
        }
    }

    async fn or_write<D>(self, root: &D) -> crate::Result<SessionMut<'a, F>>
    where
        D: VirtualDir<File = F>,
    {
        match self.handle {
            Entry::Occupied(e) => Ok(SessionMut {
                handle: e.into_ref(),
            }),
            Entry::Vacant(e) => {
                let sid = e.key();
                let subdir = root.open_subdir(&sid.pid).await?;
                let file = subdir.write_file(&sid.timestamp.to_string()).await?;
                let session = SessionFile::new(sid.clone(), file).await?;
                let handle = SessionHandle::new(session, 0);

                Ok(SessionMut {
                    handle: e.insert(handle),
                })
            }
        }
    }
}

struct SessionMut<'a, F> {
    handle: RefMut<'a, Sid, SessionHandle<F>>,
}

impl<'a, F> Deref for SessionMut<'a, F> {
    type Target = SessionHandle<F>;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl<'a, F> DerefMut for SessionMut<'a, F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.handle
    }
}

trait SessionManager<F> {
    fn session_entry(&self, sid: Sid) -> SessionEntry<F>;
}

impl<F> SessionManager<F> for DashMap<Sid, SessionHandle<F>> {
    fn session_entry(&self, sid: Sid) -> SessionEntry<F> {
        SessionEntry::from(self.entry(sid))
    }
}

trait Merge {
    /// Merges given key-value pair into the current database.
    /// If no entry under given key existed, a `None` is returned.
    /// If there was an existing value, a last-write-wins strategy will be applied and a [Sid]
    /// of a loosing entry will be returned.
    fn merge(&self, key: Bytes, db_entry: DbEntry) -> Option<Sid>;
}

impl Merge for DashMap<Bytes, DbEntry> {
    fn merge(&self, key: Bytes, entry: DbEntry) -> Option<Sid> {
        match self.entry(key) {
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

        db.flush().await.unwrap();

        // check if all entries are present
        for i in 0..10 {
            let key = format!("key-{}", i);
            let value = format!("value-{}", i);
            let result = db.get(key.as_bytes()).await.unwrap().unwrap();
            assert_eq!(value.as_bytes(), &result[..]);
        }
    }

    #[tokio::test]
    async fn restore() {
        let _ = env_logger::builder().is_test(true).try_init();
        let temp_dir = tempfile::tempdir().unwrap();
        let root = Dir::new(temp_dir.path());
        // init and close db
        {
            let db = Db::open_write("test", root.clone()).await.unwrap();
            // init database state
            for i in 0..10 {
                let key = format!("key-{}", i);
                let value = format!("value-{}", i);
                db.insert(key.as_bytes(), value.as_bytes()).await.unwrap();
            }

            db.flush().await.unwrap()
        }

        // reopen the db
        let db = Db::open_write("test", root).await.unwrap();

        // check if all entries are present
        for i in 0..10 {
            let key = format!("key-{}", i);
            let value = format!("value-{}", i);
            let result = db.get(key.as_bytes()).await.unwrap().unwrap();
            assert_eq!(value.as_bytes(), &result[..]);
        }
    }
}
