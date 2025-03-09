use async_stream::try_stream;
use async_trait::async_trait;
use futures_lite::Stream;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::File;

pub struct Dir {
    path: PathBuf,
}

impl Dir {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }
}

#[async_trait]
impl super::VirtualDir for Dir {
    type File = File;
    type Files = DirFiles;

    async fn open_dir(dir_path: &str) -> std::io::Result<Self> {
        let dir_path = PathBuf::from(dir_path);
        tokio::fs::create_dir_all(&dir_path).await?;
        Ok(Dir { path: dir_path })
    }

    async fn open_subdir(&self, dir_name: &str) -> std::io::Result<Self> {
        let dir_path = PathBuf::from(self.path.join(dir_name));
        tokio::fs::create_dir_all(&dir_path).await?;
        Ok(Dir { path: dir_path })
    }

    async fn read_file(&self, file_name: &str) -> std::io::Result<Self::File> {
        let fpath = self.path.join(file_name);
        File::options().read(true).open(fpath).await
    }

    async fn write_file(&self, file_name: &str) -> std::io::Result<Self::File> {
        let fpath = self.path.join(file_name);
        File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(fpath)
            .await
    }

    async fn remove_file(&self, file_name: &str) -> std::io::Result<()> {
        tokio::fs::remove_file(file_name).await
    }

    fn list_files(&self) -> Self::Files {
        let fut = tokio::fs::read_dir(self.path.clone());
        let inner = try_stream! {
            let mut read_dir = fut.await?;
            while let Some(entry) = read_dir.next_entry().await? {
                let file_name = entry.file_name().into_string().map_err(|_| std::io::ErrorKind::InvalidData)?;
                yield file_name;
            }
        };
        DirFiles {
            inner: Box::pin(inner),
        }
    }
}

pub struct DirFiles {
    inner: Pin<Box<dyn Stream<Item = crate::Result<String>> + Send + Sync>>,
}

impl Stream for DirFiles {
    type Item = crate::Result<String>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}
