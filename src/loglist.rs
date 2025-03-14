use crate::db::Pid;
use crate::session::Sid;
use crate::timestamp::Timestamp;
use crate::vfs::VirtualFile;
use std::io::{ErrorKind, SeekFrom};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub struct LogListFile<F> {
    pid: Pid,
    file: F,
}

impl<F: VirtualFile> LogListFile<F> {
    pub fn new(pid: Pid, file: F) -> Self {
        Self { pid, file }
    }

    /// Returns the next entry from the `.loglist` file.
    pub async fn next_entry(&mut self) -> crate::Result<LogEntry> {
        let mut buf = [0u8; 8];
        self.file.read_exact(&mut buf).await?;
        let timestamp = Timestamp::from_le_bytes(buf);
        let size = match self.file.read_exact(&mut buf).await {
            Ok(_) => Some(u64::from_le_bytes(buf)),
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => None,
            Err(err) => return Err(err.into()),
        };
        Ok(LogEntry {
            session_start: timestamp,
            size,
        })
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
pub struct LogEntry {
    /// Name of the corresponding session file.
    pub session_start: Timestamp,
    /// Expected length of a session file.
    /// If `None`, it means that this is an active session file that's being continuously
    /// written to by another process.
    pub size: Option<u64>,
}

impl LogEntry {
    pub fn is_latest(&self) -> bool {
        self.size.is_none()
    }
}
