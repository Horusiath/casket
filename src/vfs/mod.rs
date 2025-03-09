use async_trait::async_trait;
use futures_lite::Stream;
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite};

pub mod fs;

pub trait VirtualFile: AsyncRead + AsyncWrite + AsyncSeek + Unpin {}
impl<F> VirtualFile for F where F: AsyncRead + AsyncWrite + AsyncSeek + Unpin {}

#[async_trait]
pub trait VirtualDir: Sized {
    type File: VirtualFile;
    type Files: Stream<Item = crate::Result<String>>;

    /// Open or create a directory at the given path, recursively creating parent directories if
    /// necessary.
    async fn open_dir(dir_path: &str) -> std::io::Result<Self>;

    /// Open or create a subdirectory at the given path, recursively creating parent directories if
    /// necessary.
    async fn open_subdir(&self, dir_name: &str) -> std::io::Result<Self>;

    /// Open a file with the given name at a current directory, for read-only mode. Returns an error
    /// if the file does not exist.
    async fn read_file(&self, file_name: &str) -> std::io::Result<Self::File>;

    /// Open a file with the given name at current directory, for read-write mode. Creates the file
    /// if it does not exist.
    async fn write_file(&self, file_name: &str) -> std::io::Result<Self::File>;

    /// Removes a file with the given name from the current directory.
    async fn remove_file(&self, file_name: &str) -> std::io::Result<()>;

    /// List all file names in the current directory.
    fn list_files(&self) -> Self::Files;
}
