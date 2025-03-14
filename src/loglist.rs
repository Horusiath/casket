use crate::db::Pid;
use crate::session::Sid;
use crate::timestamp::Timestamp;
use crate::vfs::{VirtualDir, VirtualFile};
use std::io::{ErrorKind, SeekFrom};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub struct LogListFile<F> {
    pid: Pid,
    entries_read: u64,
    file: F,
}

impl<F: VirtualFile> LogListFile<F> {
    pub async fn open_read<D>(pid: Pid, root: &D) -> crate::Result<Self>
    where
        D: VirtualDir<File = F>,
    {
        let file = root.read_file(&format!("{}/.loglist", pid)).await?;
        Ok(Self::new(pid, file))
    }

    pub async fn open_write<D>(pid: Pid, root: &D) -> crate::Result<Self>
    where
        D: VirtualDir<File = F>,
    {
        let file = root.open_subdir(&pid).await?.write_file(".loglist").await?;
        Ok(Self::new(pid, file))
    }

    pub fn new(pid: Pid, file: F) -> Self {
        Self {
            pid,
            file,
            entries_read: 0,
        }
    }

    async fn read_buf(&mut self) -> crate::Result<Option<[u8; 8]>> {
        let mut buf = [0u8; 8];
        match self.file.read_exact(&mut buf).await {
            Ok(_) => Ok(Some(buf)),
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// Returns the next entry from the `.loglist` file.
    pub async fn next_entry(&mut self) -> crate::Result<Option<LogEntry>> {
        self.file
            .seek(SeekFrom::Start(self.entries_read * 16)) // each entry is 16B
            .await?;
        let timestamp = match self.read_buf().await? {
            Some(buf) => Timestamp::from_le_bytes(buf),
            None => return Ok(None),
        };
        let size = match self.read_buf().await? {
            None => {
                // this log entry is still actively being written to
                None
            }
            Some(buf) => {
                self.entries_read += 1; // we've read an entire entry
                Some(u64::from_le_bytes(buf))
            }
        };
        let e = LogEntry::new(timestamp, size);
        Ok(Some(e))
    }

    /// Starts a new session. Returns
    pub async fn begin(&mut self) -> crate::Result<Sid> {
        let session_start = Timestamp::now();
        self.file.seek(SeekFrom::End(0)).await?;
        self.file
            .write_all(session_start.to_le_bytes().as_ref())
            .await?;
        self.file.flush().await?;
        Ok(Sid::new(self.pid.clone(), session_start))
    }

    /// Commits an existing session. Once called, no writes to that session file will be allowed.
    pub async fn commit(&mut self, size: u64) -> crate::Result<()> {
        self.file.seek(SeekFrom::End(0)).await?;
        self.file.write_all(size.to_le_bytes().as_ref()).await?;
        self.file.flush().await?;
        Ok(())
    }
}

/// Single entry inside a `.loglist`` file.
#[derive(Clone)]
pub struct LogEntry {
    /// Name of the corresponding session file.
    pub session_start: Timestamp,
    /// Expected length of a session file.
    /// If `None`, it means that this is an active session file that's being continuously
    /// written to by another process.
    pub size: Option<u64>,
}

impl LogEntry {
    fn new(session_start: Timestamp, size: Option<u64>) -> Self {
        LogEntry {
            session_start,
            size,
        }
    }
}

impl LogEntry {
    pub fn is_latest(&self) -> bool {
        self.size.is_none()
    }
}
