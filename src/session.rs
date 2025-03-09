use crate::db::{DbEntry, Pid};
use crate::timestamp::Timestamp;
use crate::vfs::VirtualFile;
use bytes::BytesMut;
use std::fmt::Display;
use std::io::SeekFrom;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

/// Session ID.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Sid {
    pub timestamp: Timestamp,
    pub pid: Pid,
}

impl Display for Sid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.timestamp, self.pid)
    }
}

pub struct SessionFile<F> {
    /// Session ID.
    id: Sid,
    /// File handle.
    file: F,
}

impl<F: VirtualFile> SessionFile<F> {
    pub async fn new(pid: Pid, start_time: Timestamp, mut file: F) -> crate::Result<Self> {
        file.seek(SeekFrom::Start(0)).await?;
        Ok(Self {
            id: Sid {
                timestamp: start_time,
                pid,
            },
            file,
        })
    }

    pub fn sid(&self) -> &Sid {
        &self.id
    }

    pub async fn append_entry(&mut self, key: &[u8], value: &[u8]) -> crate::Result<DbEntry> {
        let file_position = self.file.stream_position().await?;
        let timestamp = Timestamp::now();
        let entry_len = key.len() + value.len() + 16; // 8B timestamp + 4B key len + 4B checksum. Exclude total length, it was already read
        let mut buf = Vec::with_capacity(entry_len - 4);

        // write total length of the entry
        let entry_len_bytes = (entry_len as u32).to_le_bytes();
        buf.extend_from_slice(&entry_len_bytes);

        // write timestamp
        let timestamp_bytes = timestamp.to_le_bytes();
        buf.extend_from_slice(&timestamp_bytes);

        // write key length
        let key_len_bytes = (key.len() as u32).to_le_bytes();
        buf.extend_from_slice(&key_len_bytes);

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
            self.id.clone(),
            timestamp,
            file_position,
            key.len() as u32,
            entry_len as u32,
        );

        tracing::trace!(
            "appending entry {} at position {} ({} bytes, checksum: {:x})",
            entry.id(),
            file_position,
            entry_len,
            checksum
        );
        // flush serialized entry to the file
        self.file.write_all(&buf).await?;
        self.file.flush().await?;

        Ok(entry)
    }

    pub async fn next_entry(
        &mut self,
        key_buf: &mut BytesMut,
        value_buf: Option<&mut BytesMut>,
    ) -> crate::Result<DbEntry> {
        let mut len_buf = [0u8; 4];
        let start_len = self.file.stream_position().await?;
        self.file.read_exact(&mut len_buf).await?;

        let mut crc = crc32fast::Hasher::new();
        crc.update(&len_buf);
        let total_len = u32::from_le_bytes(len_buf) as usize;
        let mut buf = vec![0u8; total_len];
        self.file.read_exact(&mut buf).await?;

        // compute checksum
        crc.update(&buf[..(total_len - 4)]); // exclude checksum itself
        let checksum = crc.finalize();

        // read timestamp
        let timestamp = Timestamp::from_le_bytes(buf[..8].try_into().unwrap());

        // read key length
        let key_len = u32::from_le_bytes(buf[8..12].try_into().unwrap());

        // read key
        let key = &buf[12..(12 + key_len as usize)];
        key_buf.extend_from_slice(key);

        // read value if requested
        if let Some(value_buf) = value_buf {
            let value = &buf[(12 + key_len as usize)..(total_len - 4)];
            value_buf.extend_from_slice(value);
        }

        tracing::trace!(
            "read entry {}-{} at position {} ({} bytes, checksum: {:x})",
            timestamp,
            self.id.pid,
            start_len,
            total_len,
            checksum
        );

        // we don't need value for now, go straight to checksum
        let checksum_bytes = u32::from_le_bytes(buf[(total_len - 4)..].try_into().unwrap());
        if checksum != checksum_bytes {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "checksum mismatch").into())
        } else {
            Ok(DbEntry::new(
                self.id.clone(),
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
    use crate::session::SessionFile;
    use bytes::BytesMut;
    use std::io::SeekFrom;
    use std::path::Path;
    use tokio::fs::File;
    use tokio::io::AsyncSeekExt;

    #[tokio::test]
    async fn write_read_entries() {
        let _ = env_logger::builder().is_test(true).try_init();
        let tmp_dir = tempfile::tempdir().unwrap();
        let mut session = test_session(tmp_dir.path(), "test").await;

        // fill session file with new entries
        for i in 0..10 {
            let key = format!("key-{}", i);
            let value = format!("value-{}", i);
            let entry = session
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
            let entry = session
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
        SessionFile::new(pid, start_time, file).await.unwrap()
    }
}
