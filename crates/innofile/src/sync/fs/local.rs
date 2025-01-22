use std::{
    fs::{self, File as FsFile, Metadata as FsMetadata},
    io::{BufReader, BufWriter},
    path::Path,
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

impl FileSystem for LocalFS {
    fn scheme(&self) -> &str {
        SCHEME
    }

    fn exists(&self, path: &str) -> InnoFileResult<bool> {
        Ok(fs::exists(Self::strip_scheme(path))?)
    }

    fn open(&self, path: &str) -> InnoFileResult<Box<dyn File>> {
        let path = Self::strip_scheme(path);
        Ok(Box::new(LocalFile::new(&path, FsFile::open(&path)?)))
    }

    fn create(&self, path: &str) -> InnoFileResult<Box<dyn File>> {
        let path = Self::strip_scheme(path);
        Self::ensure_parent_path(&path)?;
        Ok(Box::new(LocalFile::new(&path, FsFile::create(&path)?)))
    }

    fn create_new(&self, path: &str) -> InnoFileResult<Box<dyn File>> {
        let path = Self::strip_scheme(path);
        Self::ensure_parent_path(&path)?;
        Ok(Box::new(LocalFile::new(&path, FsFile::create_new(&path)?)))
    }

    fn remove_dir(&self, path: &str) -> InnoFileResult<()> {
        Ok(fs::remove_dir(Self::strip_scheme(path))?)
    }

    fn remove_file(&self, path: &str) -> InnoFileResult<()> {
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

impl File for LocalFile {
    fn path(&self) -> &str {
        &self.path
    }

    fn metadata(&self) -> InnoFileResult<Box<dyn Metadata>> {
        Ok(Box::new(self.inner.metadata()?))
    }

    fn reader(&self) -> InnoFileResult<Box<dyn FileRead>> {
        Ok(Box::new(BufReader::new(self.inner.try_clone()?)))
    }

    fn writer(&self) -> InnoFileResult<Box<dyn FileWrite>> {
        Ok(Box::new(BufWriter::new(self.inner.try_clone()?)))
    }
}

impl Metadata for FsMetadata {
    fn len(&self) -> u64 {
        self.len()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::fs::FileSystemBuilder;

    use super::*;

    #[test]
    fn test_local_fs() -> InnoFileResult<()> {
        let content = "Hello, LocalFS!\n";

        let path = tempdir()?.path().join("hello").join("local_fs.txt");
        let path = path.to_str().unwrap();
        let paths = [format!("file://{}", path), path.to_string()];

        for path in paths.as_ref() {
            let file_system = FileSystemBuilder::from_path(path)?.build_sync()?;

            assert!(!file_system.exists(path)?);

            let output_file = file_system.create_new(path)?;
            assert!(file_system.exists(output_file.path())?);

            output_file.writer()?.write_all(content.as_bytes())?;

            let input_file = file_system.open(path)?;
            assert_eq!(input_file.metadata()?.len(), content.len() as u64);

            let mut buf = String::new();
            input_file.reader()?.read_to_string(&mut buf)?;
            assert_eq!(buf, content);

            file_system.remove_file(path)?;
            assert!(!file_system.exists(path)?);

            if let Some(parent) = Path::new(path).parent() {
                let parent = parent.to_str().unwrap();
                assert!(file_system.exists(parent)?);
                file_system.remove_dir(parent)?;
                assert!(!file_system.exists(parent)?);
            }
        }

        Ok(())
    }
}
