use crate::db::{DbEntry, Pid};
use crate::session::{SessionFile, Sid};
use crate::timestamp::Timestamp;
use crate::vfs::VirtualDir;
use bytes::{Bytes, BytesMut};
use futures_lite::StreamExt;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::io::{ErrorKind, SeekFrom};
use std::str::FromStr;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;
use tokio::pin;

pub struct SessionManager<D: VirtualDir> {
    root: D,
    sessions: HashMap<Sid, SessionHandle<D::File>>,
}

impl<D: VirtualDir> SessionManager<D> {
    pub fn new(root: D) -> Self {
        Self {
            root,
            sessions: HashMap::new(),
        }
    }

    pub async fn open(
        &mut self,
        sid: Sid,
        inc_ref: bool,
    ) -> crate::Result<&mut SessionFile<D::File>> {
        let h = match self.sessions.entry(sid) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                let session = Self::start_session(&mut self.root, e.key()).await?;
                let h = SessionHandle::new(session, 0);
                e.insert(h)
            }
        };
        if inc_ref {
            h.inc_ref();
        }
        Ok(&mut h.session)
    }

    /// Iterate over all sessions stored in current root directory, replaying all entries using
    /// given function and cache the session files for entries that are still in use.
    pub async fn replay_all<F>(&mut self, f: F) -> crate::Result<()>
    where
        F: Fn(Bytes, DbEntry) -> Option<Sid>,
    {
        let subdirs = self.root.list_files();
        pin!(subdirs);
        let mut key_buf = BytesMut::new();
        while let Some(subdir) = subdirs.next().await {
            let pid = Pid::from(subdir?);
            let subdir = self.root.open_subdir(&pid).await?;
            let sessions = subdir.list_files();
            pin!(sessions);
            while let Some(session) = sessions.next().await {
                let session = session?;
                if session == ".head" {
                    continue;
                }
                let timestamp = Timestamp::from_str(&session)?;
                let session_file = subdir.read_file(&session).await?;
                let mut session = SessionFile::new(pid.clone(), timestamp, session_file).await?;
                let mut session_ref_count = 0i32;
                loop {
                    key_buf.clear();
                    match session.next_entry(&mut key_buf, None).await {
                        Ok(entry) => {
                            let key = key_buf.clone().freeze();
                            match f(key, entry) {
                                // current value is in use, increment the reference count for this session file
                                None => session_ref_count += 1,
                                // current value is outdated, decrement the reference count for this session file
                                Some(sid) if &sid == session.sid() => session_ref_count -= 1,
                                // after merge, this value has outdated another session, decrement that session's reference count
                                Some(sid) => {
                                    session_ref_count += 1;
                                    if let Entry::Occupied(mut e) = self.sessions.entry(sid) {
                                        // decrement the reference count for the outdated session
                                        if e.get_mut().dec_ref() {
                                            // if number of references reaches zero, remove the session file handle
                                            e.remove();
                                        }
                                    }
                                }
                            }
                        }
                        Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                        Err(err) => return Err(err.into()),
                    }
                }

                if session_ref_count > 0 {
                    let sid = session.sid().clone();
                    let h = SessionHandle::new(session, session_ref_count as isize);
                    self.sessions.insert(sid, h);
                }
            }
        }
        Ok(())
    }

    async fn start_session(root: &mut D, sid: &Sid) -> crate::Result<SessionFile<D::File>> {
        let timestamp = Timestamp::now();
        let subdir = root.open_subdir(&sid.pid).await?;
        let session_file = subdir.write_file(&timestamp.to_string()).await?;
        let session = SessionFile::new(sid.pid.clone(), timestamp, session_file).await?;
        Self::update_head(root, sid).await?;
        Ok(session)
    }

    /// Update the .head file for the current pid with the given session timestamp.
    async fn update_head(root: &mut D, sid: &Sid) -> crate::Result<()> {
        let subdir = root.open_subdir(&sid.pid).await?;
        let mut f = subdir.write_file(".head").await?;
        // reset the file pointer to the beginning and override any existing content
        f.seek(SeekFrom::Start(0)).await?;
        f.write_all(sid.timestamp.to_string().as_bytes()).await?;
        Ok(())
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
