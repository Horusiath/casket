use crate::db::{DbEntry, Pid};
use crate::timestamp::Timestamp;
use crate::vfs::VirtualFile;
use bytes::BytesMut;
use std::fmt::Display;
use std::io::SeekFrom;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

/// Session ID.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Sid {
    pub timestamp: Timestamp,
    pub pid: Pid,
}

impl Sid {
    pub fn new(pid: Pid, timestamp: Timestamp) -> Self {
        Self { timestamp, pid }
    }
}

impl Display for Sid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.timestamp, self.pid)
    }
}

pub struct SessionFile<F> {
    /// Session ID.
    sid: Sid,
    /// File handle.
    file: F,
}

impl<F: VirtualFile> SessionFile<F> {
    pub async fn new(sid: Sid, mut file: F) -> crate::Result<Self> {
        file.seek(SeekFrom::Start(0)).await?;
        Ok(Self { sid, file })
    }

    pub fn sid(&self) -> &Sid {
        &self.sid
    }

    pub async fn append_entry(&mut self, key: &[u8], value: &[u8]) -> crate::Result<DbEntry> {
        let file_position = self.file.seek(SeekFrom::End(0)).await?;
        let timestamp = Timestamp::now();
        let entry_len = key.len() + value.len() + 20; // 4B total len + 8B timestamp + 4B key len + 4B checksum.
        let mut buf = Vec::with_capacity(entry_len);
        tracing::trace!(
            "{} preparing write data (len: {}) at position: {}",
            self.sid,
            entry_len,
            file_position
        );

        // write total length of the entry
        buf.extend_from_slice(&(entry_len as u32).to_le_bytes());
        // write timestamp
        buf.extend_from_slice(&timestamp.to_le_bytes());
        // write key length
        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        // write key
        buf.extend_from_slice(key);
        // write value
        buf.extend_from_slice(value);

        // write checksum
        let mut crc = crc32fast::Hasher::new();
        crc.update(&buf);
        let checksum = crc.finalize();
        let checksum_bytes = checksum.to_le_bytes();
        buf.extend_from_slice(&checksum_bytes);

        let entry = DbEntry::new(
            self.sid.clone(),
            timestamp,
            file_position,
            key.len() as u32,
            entry_len as u32,
        );

        tracing::trace!(
            "[{}] session {} appending entry at {} - len: {}, ts: {}, key len: {}, checksum: {}",
            self.sid.pid,
            self.sid,
            file_position,
            entry_len,
            entry.timestamp(),
            key.len(),
            checksum
        );
        // write serialized entry to the session file
        self.file.write_all(&buf).await?;

        Ok(entry)
    }

    pub async fn seek(&mut self, seek_from: SeekFrom) -> crate::Result<u64> {
        let offset = self.file.seek(seek_from).await?;
        Ok(offset)
    }

    pub async fn flush(&mut self) -> crate::Result<()> {
        self.file.flush().await
    }

    pub async fn next_entry(
        &mut self,
        key_buf: &mut BytesMut,
        value_buf: Option<&mut BytesMut>,
    ) -> crate::Result<DbEntry> {
        let start_len = self.file.stream_position().await?;
        let mut header = [0u8; 16]; // 4B total len + 8B timestamp + 4B key len
        self.file.read_exact(&mut header).await?;

        let mut crc = crc32fast::Hasher::new();
        crc.update(&header);

        // read total length
        let total_len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
        // read timestamp
        let timestamp = Timestamp::from_le_bytes(header[4..12].try_into().unwrap());
        // read key length
        let key_len = u32::from_le_bytes(header[12..16].try_into().unwrap());
        // read key
        key_buf.clear();
        key_buf.resize(key_len as usize, 0);
        self.file.read_exact(&mut key_buf[..]).await?;
        crc.update(&key_buf);

        // read value
        let value_len = total_len - key_len as usize - 20;
        let verify_crc = if let Some(value_buf) = value_buf {
            value_buf.clear();
            value_buf.resize(value_len, 0);
            self.file.read_exact(&mut value_buf[..]).await?;
            crc.update(&value_buf);
            true
        } else {
            self.file.seek(SeekFrom::Current(value_len as i64)).await?; // move value_len forward
            false
        };

        // read checksum
        let mut checksum = [0u8; 4];
        self.file.read_exact(&mut checksum).await?;
        let checksum = u32::from_le_bytes(checksum);

        // we don't need value for now, go straight to checksum
        if verify_crc && checksum != crc.finalize() {
            tracing::error!("{} cheksum for key {:?} don't match", self.sid, key_buf);
            Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "checksum mismatch").into())
        } else {
            Ok(DbEntry::new(
                self.sid.clone(),
                timestamp,
                start_len,
                key_len,
                total_len as u32,
            ))
        }
    }

    pub async fn read_entry(
        &mut self,
        entry: &DbEntry,
        key_buf: &mut BytesMut,
        value_buf: &mut BytesMut,
    ) -> crate::Result<()> {
        self.file
            .seek(SeekFrom::Start(entry.entry_offset()))
            .await?;
        self.next_entry(key_buf, Some(value_buf)).await?;
        self.file.seek(SeekFrom::End(0)).await?; // return to the end of the file
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::db::Pid;
    use crate::session::{SessionFile, Sid};
    use bytes::BytesMut;
    use std::io::SeekFrom;
    use std::path::Path;
    use tokio::fs::File;
    use tokio::io::AsyncSeekExt;

    #[tokio::test]
    async fn sequential_write_read() {
        let _ = env_logger::builder().is_test(true).try_init();
        let tmp_dir = tempfile::tempdir().unwrap();
        let mut session = test_session(tmp_dir.path(), "test").await;

        // fill session file with new entries
        for i in 0..10 {
            let key = format!("key-{}", i);
            let value = format!("value-{}", i);
            let _entry = session
                .append_entry(key.as_bytes(), value.as_bytes())
                .await
                .unwrap();
        }

        // reset session file position
        session.file.seek(SeekFrom::Start(0)).await.unwrap();

        // read entries back
        let mut key_buf = BytesMut::new();
        let mut value_buf = BytesMut::new();
        for i in 0..10 {
            let expected_key = format!("key-{}", i);
            let expected_value = format!("value-{}", i);
            let _entry = session
                .next_entry(&mut key_buf, Some(&mut value_buf))
                .await
                .unwrap();
            let actual_key = String::from_utf8(key_buf.to_vec()).unwrap();
            let actual_value = String::from_utf8(value_buf.to_vec()).unwrap();
            assert_eq!(expected_key, actual_key);
            assert_eq!(expected_value, actual_value);

            key_buf.clear();
            value_buf.clear();
        }
    }

    #[tokio::test]
    async fn random_reads() {
        let _ = env_logger::builder().is_test(true).try_init();
        let tmp_dir = tempfile::tempdir().unwrap();
        let mut session = test_session(tmp_dir.path(), "test").await;

        // fill session file with new entries
        let mut entries = Vec::new();
        for i in 0..10 {
            let key = format!("key-{}", i);
            let value = format!("value-{}", i);
            let entry = session
                .append_entry(key.as_bytes(), value.as_bytes())
                .await
                .unwrap();
            entries.push(entry);
        }

        // read every odd entry
        for i in (1..10).step_by(2) {
            let key = format!("key-{}", i);
            let value = format!("value-{}", i);
            let entry = &entries[i];
            let mut key_buf = BytesMut::new();
            let mut value_buf = BytesMut::new();
            session
                .read_entry(entry, &mut key_buf, &mut value_buf)
                .await
                .unwrap();
            let actual_key = String::from_utf8(key_buf.to_vec()).unwrap();
            let actual_value = String::from_utf8(value_buf.to_vec()).unwrap();
            assert_eq!(key, actual_key);
            assert_eq!(value, actual_value);
        }
    }

    async fn test_session<P1: AsRef<Path>, P2: Into<Pid>>(dir: P1, pid: P2) -> SessionFile<File> {
        let dir = dir.as_ref();
        let pid = pid.into();
        let pid_dir = dir.join(pid.as_ref());
        tokio::fs::create_dir(&pid_dir).await.unwrap();
        let start_time = crate::timestamp::Timestamp::now();
        let file = File::options()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(dir.join(pid.as_ref()).join(start_time.to_string()))
            .await
            .unwrap();
        SessionFile::new(Sid::new(pid, start_time), file)
            .await
            .unwrap()
    }
}
