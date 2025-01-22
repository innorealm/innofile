use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use tokio::io::{AsyncBufRead, AsyncSeek, AsyncWrite};

use crate::error::InnoFileResult;

pub mod local;
#[cfg(feature = "object_store")]
pub mod object_store;

pub type FileSystemRef = Arc<dyn FileSystem>;
pub type FileRef = Arc<dyn File>;

#[async_trait]
pub trait FileSystem: Debug + Unpin + Send + Sync {
    fn scheme(&self) -> &str;

    async fn exists(&self, path: &str) -> InnoFileResult<bool>;

    async fn open(&self, path: &str) -> InnoFileResult<Box<dyn File>>;

    async fn create(&self, path: &str) -> InnoFileResult<Box<dyn File>>;

    async fn create_new(&self, path: &str) -> InnoFileResult<Box<dyn File>>;

    async fn remove_dir(&self, path: &str) -> InnoFileResult<()>;

    async fn remove_file(&self, path: &str) -> InnoFileResult<()>;
}

#[async_trait]
pub trait File: Debug + Unpin + Send + Sync {
    fn path(&self) -> &str;

    async fn metadata(&self) -> InnoFileResult<Box<dyn Metadata>>;

    async fn reader(&self) -> InnoFileResult<Box<dyn FileRead>>;

    async fn writer(&self) -> InnoFileResult<Box<dyn FileWrite>>;
}

#[async_trait]
pub trait Metadata: Debug + Unpin + Send + Sync {
    fn len(&self) -> u64;
}

#[async_trait]
pub trait FileRead: Debug + AsyncBufRead + AsyncSeek + Unpin + Send {}

#[async_trait]
impl<R: Debug + AsyncBufRead + AsyncSeek + Unpin + Send> FileRead for R {}

#[async_trait]
pub trait FileWrite: Debug + AsyncWrite + Unpin + Send {}

#[async_trait]
impl<W: Debug + AsyncWrite + Unpin + Send> FileWrite for W {}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::fs::FileSystemBuilder;

    use super::*;

    #[tokio::test]
    async fn test_file_system() -> InnoFileResult<()> {
        let path = tempdir()?.path().join("does_not_exist.txt");
        let path = path.to_str().unwrap();
        let file_system = FileSystemBuilder::from_path(path)?.build_async().await?;
        assert!(!file_system.exists(path).await?);
        Ok(())
    }
}
