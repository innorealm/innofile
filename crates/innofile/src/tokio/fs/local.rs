use std::{
    fs::{self, Metadata as FsMetadata},
    path::Path,
};

use async_trait::async_trait;
use tokio::{
    fs::File as FsFile,
    io::{BufReader, BufWriter},
};

use crate::error::InnoFileResult;

use super::{File, FileRead, FileSystem, FileWrite, Metadata};

const SCHEME: &str = "file";

#[derive(Debug)]
pub struct LocalFS;

impl LocalFS {
    fn strip_scheme(path: &str) -> String {
        path.strip_prefix(&format!("{}:/", SCHEME))
            .map(|p| p.to_string())
            .unwrap_or_else(|| path.to_string())
    }

    fn ensure_parent_path(path: impl AsRef<Path>) -> InnoFileResult<()> {
        if let Some(parent) = path.as_ref().parent() {
            if !fs::exists(parent)? {
                fs::create_dir_all(parent)?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl FileSystem for LocalFS {
    fn scheme(&self) -> &str {
        SCHEME
    }

    async fn exists(&self, path: &str) -> InnoFileResult<bool> {
        Ok(fs::exists(Self::strip_scheme(path))?)
    }

    async fn open(&self, path: &str) -> InnoFileResult<Box<dyn File>> {
        let path = Self::strip_scheme(path);
        Ok(Box::new(LocalFile::new(&path, FsFile::open(&path).await?)))
    }

    async fn create(&self, path: &str) -> InnoFileResult<Box<dyn File>> {
        let path = Self::strip_scheme(path);
        Self::ensure_parent_path(&path)?;
        Ok(Box::new(LocalFile::new(
            &path,
            FsFile::create(&path).await?,
        )))
    }

    async fn create_new(&self, path: &str) -> InnoFileResult<Box<dyn File>> {
        let path = Self::strip_scheme(path);
        Self::ensure_parent_path(&path)?;
        Ok(Box::new(LocalFile::new(
            &path,
            FsFile::create_new(&path).await?,
        )))
    }

    async fn remove_dir(&self, path: &str) -> InnoFileResult<()> {
        Ok(fs::remove_dir(Self::strip_scheme(path))?)
    }

    async fn remove_file(&self, path: &str) -> InnoFileResult<()> {
        Ok(fs::remove_file(Self::strip_scheme(path))?)
    }
}

#[derive(Debug)]
pub struct LocalFile {
    path: String,
    inner: FsFile,
}

impl LocalFile {
    pub fn new(path: impl ToString, inner: FsFile) -> Self {
        Self {
            path: path.to_string(),
            inner,
        }
    }
}

#[async_trait]
impl File for LocalFile {
    fn path(&self) -> &str {
        &self.path
    }

    async fn metadata(&self) -> InnoFileResult<Box<dyn Metadata>> {
        Ok(Box::new(self.inner.metadata().await?))
    }

    async fn reader(&self) -> InnoFileResult<Box<dyn FileRead>> {
        Ok(Box::new(BufReader::new(self.inner.try_clone().await?)))
    }

    async fn writer(&self) -> InnoFileResult<Box<dyn FileWrite>> {
        Ok(Box::new(BufWriter::new(self.inner.try_clone().await?)))
    }
}

#[async_trait]
impl Metadata for FsMetadata {
    fn len(&self) -> u64 {
        self.len()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use crate::fs::FileSystemBuilder;

    use super::*;

    #[tokio::test]
    async fn test_local_fs() -> InnoFileResult<()> {
        let content = "Hello, LocalFS!\n";

        let path = tempdir()?.path().join("hello").join("local_fs.txt");
        let path = path.to_str().unwrap();
        let paths = [format!("file://{}", path), path.to_string()];

        for path in paths.as_ref() {
            let file_system = FileSystemBuilder::from_path(path)?.build_async().await?;

            assert!(!file_system.exists(path).await?);

            let output_file = file_system.create_new(path).await?;
            assert!(file_system.exists(output_file.path()).await?);

            let mut writer = output_file.writer().await?;
            writer.write_all(content.as_bytes()).await?;
            writer.shutdown().await?;

            let input_file = file_system.open(path).await?;
            assert_eq!(input_file.metadata().await?.len(), content.len() as u64);

            let mut buf = String::new();
            input_file.reader().await?.read_to_string(&mut buf).await?;
            assert_eq!(buf, content);

            file_system.remove_file(path).await?;
            assert!(!file_system.exists(path).await?);

            if let Some(parent) = Path::new(path).parent() {
                let parent = parent.to_str().unwrap();
                assert!(file_system.exists(parent).await?);
                file_system.remove_dir(parent).await?;
                assert!(!file_system.exists(parent).await?);
            }
        }

        Ok(())
    }
}
