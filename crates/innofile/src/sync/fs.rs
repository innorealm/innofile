use std::{
    fmt::Debug,
    io::{BufRead, Seek, Write},
    sync::Arc,
};

use crate::error::InnoFileResult;

pub mod local;
#[cfg(feature = "object_store")]
pub mod object_store;

pub type FileSystemRef = Arc<dyn FileSystem>;
pub type FileRef = Arc<dyn File>;

pub trait FileSystem: Debug + Send + Sync {
    fn scheme(&self) -> &str;

    fn exists(&self, path: &str) -> InnoFileResult<bool>;

    fn open(&self, path: &str) -> InnoFileResult<Box<dyn File>>;

    fn create(&self, path: &str) -> InnoFileResult<Box<dyn File>>;

    fn create_new(&self, path: &str) -> InnoFileResult<Box<dyn File>>;

    fn remove_dir(&self, path: &str) -> InnoFileResult<()>;

    fn remove_file(&self, path: &str) -> InnoFileResult<()>;
}

pub trait File: Debug + Send + Sync {
    fn path(&self) -> &str;

    fn metadata(&self) -> InnoFileResult<Box<dyn Metadata>>;

    fn reader(&self) -> InnoFileResult<Box<dyn FileRead>>;

    fn writer(&self) -> InnoFileResult<Box<dyn FileWrite>>;
}

pub trait Metadata: Debug + Send + Sync {
    fn len(&self) -> u64;
}

pub trait FileRead: Debug + BufRead + Seek + Send {}

impl<R: Debug + BufRead + Seek + Send> FileRead for R {}

pub trait FileWrite: Debug + Write + Send {}

impl<W: Debug + Write + Send> FileWrite for W {}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::fs::FileSystemBuilder;

    use super::*;

    #[test]
    fn test_file_system() -> InnoFileResult<()> {
        let path = tempdir()?.path().join("does_not_exist.txt");
        let path = path.to_str().unwrap();
        let file_system = FileSystemBuilder::from_path(path)?.build_sync()?;
        assert!(!file_system.exists(path)?);
        Ok(())
    }
}
