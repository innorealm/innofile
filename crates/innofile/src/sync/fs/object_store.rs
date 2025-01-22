use std::{
    collections::HashMap,
    io::{BufRead, Read, Seek, SeekFrom, Write},
    sync::Arc,
};

use futures::{StreamExt as _, TryStreamExt as _};
#[cfg(feature = "s3")]
use object_store::aws::AmazonS3Builder;
use object_store::{
    buffered::{BufReader, BufWriter},
    path::Path,
    Error, ObjectMeta, ObjectStore, PutMode, PutPayload, PutResult,
};
use tokio::{
    io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _},
    runtime::Runtime,
};

use crate::error::InnoFileResult;

use super::{File, FileRead, FileSystem, FileWrite, Metadata};

#[derive(Debug)]
pub struct ObjectFS {
    scheme: String,
    store: Arc<dyn ObjectStore>,
    rt: Arc<Runtime>,
}

impl ObjectFS {
    pub fn from_store(scheme: impl ToString, store: Arc<dyn ObjectStore>) -> InnoFileResult<Self> {
        Ok(Self {
            scheme: scheme.to_string(),
            store,
            rt: Arc::new(
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?,
            ),
        })
    }

    #[cfg(feature = "s3")]
    pub fn new_s3(
        scheme: impl ToString,
        bucket_name: Option<impl ToString>,
        properties: HashMap<impl ToString, impl ToString>,
    ) -> InnoFileResult<Self> {
        let mut builder = AmazonS3Builder::from_env();
        if let Some(bucket_name) = bucket_name {
            builder = builder.with_bucket_name(bucket_name.to_string());
        }
        for (key, value) in properties {
            builder = builder.with_config(key.to_string().parse()?, value.to_string());
        }
        let store = Arc::new(builder.build()?);
        Self::from_store(scheme, store)
    }

    fn head(&self, path: impl AsRef<str>) -> InnoFileResult<ObjectMeta, Error> {
        self.rt.block_on(self.store.head(&Path::parse(path)?))
    }

    fn put_empty_file(
        &self,
        path: impl AsRef<str>,
        put_mode: PutMode,
    ) -> InnoFileResult<PutResult, Error> {
        self.rt.block_on(self.store.put_opts(
            &Path::parse(path)?,
            PutPayload::new(),
            put_mode.into(),
        ))
    }

    fn make_file(&self, path: impl ToString) -> ObjectFile {
        ObjectFile::new(path, Arc::clone(&self.store), Arc::clone(&self.rt))
    }
}

impl FileSystem for ObjectFS {
    fn scheme(&self) -> &str {
        &self.scheme
    }

    fn exists(&self, path: &str) -> InnoFileResult<bool> {
        match self.head(path) {
            Ok(_) => Ok(true),
            Err(Error::NotFound { path: _, source: _ }) => Ok(false),
            Err(error) => Err(error)?,
        }
    }

    fn open(&self, path: &str) -> InnoFileResult<Box<dyn File>> {
        self.head(path)?;
        Ok(Box::new(self.make_file(path)))
    }

    fn create(&self, path: &str) -> InnoFileResult<Box<dyn File>> {
        self.put_empty_file(path, PutMode::Overwrite)?;
        Ok(Box::new(self.make_file(path)))
    }

    fn create_new(&self, path: &str) -> InnoFileResult<Box<dyn File>> {
        self.put_empty_file(path, PutMode::Create)?;
        Ok(Box::new(self.make_file(path)))
    }

    fn remove_dir(&self, path: &str) -> InnoFileResult<()> {
        let locations = self
            .store
            .list(Some(&Path::parse(path)?))
            .map_ok(|m| m.location)
            .boxed();
        self.rt
            .block_on(self.store.delete_stream(locations).try_collect::<Vec<_>>())?;
        Ok(())
    }

    fn remove_file(&self, path: &str) -> InnoFileResult<()> {
        Ok(self.rt.block_on(self.store.delete(&Path::parse(path)?))?)
    }
}

#[derive(Debug)]
pub struct ObjectFile {
    path: String,
    store: Arc<dyn ObjectStore>,
    rt: Arc<Runtime>,
}

impl ObjectFile {
    pub fn new(path: impl ToString, store: Arc<dyn ObjectStore>, rt: Arc<Runtime>) -> Self {
        Self {
            path: path.to_string(),
            store,
            rt,
        }
    }

    pub fn object_meta(&self) -> InnoFileResult<ObjectMeta> {
        Ok(self
            .rt
            .block_on(self.store.head(&Path::parse(&self.path)?))?)
    }
}

impl File for ObjectFile {
    fn path(&self) -> &str {
        &self.path
    }

    fn metadata(&self) -> InnoFileResult<Box<dyn Metadata>> {
        Ok(Box::new(self.object_meta()?))
    }

    fn reader(&self) -> InnoFileResult<Box<dyn FileRead>> {
        Ok(Box::new(ObjectReader {
            buf_reader: BufReader::new(Arc::clone(&self.store), &self.object_meta()?),
            rt: Arc::clone(&self.rt),
        }))
    }

    fn writer(&self) -> InnoFileResult<Box<dyn FileWrite>> {
        Ok(Box::new(ObjectWriter {
            buf_writer: BufWriter::new(Arc::clone(&self.store), Path::parse(&self.path)?),
            rt: Arc::clone(&self.rt),
        }))
    }
}

impl Metadata for ObjectMeta {
    fn len(&self) -> u64 {
        self.size as _
    }
}

#[derive(Debug)]
pub struct ObjectReader {
    buf_reader: BufReader,
    rt: Arc<Runtime>,
}

impl Read for ObjectReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.rt.block_on(self.buf_reader.read(buf))
    }
}

impl BufRead for ObjectReader {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        self.rt.block_on(self.buf_reader.fill_buf())
    }

    fn consume(&mut self, amt: usize) {
        self.buf_reader.consume(amt)
    }
}

impl Seek for ObjectReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.rt.block_on(self.buf_reader.seek(pos))
    }
}

#[derive(Debug)]
pub struct ObjectWriter {
    buf_writer: BufWriter,
    rt: Arc<Runtime>,
}

impl Drop for ObjectWriter {
    fn drop(&mut self) {
        self.rt.block_on(self.buf_writer.shutdown()).unwrap();
    }
}

impl Write for ObjectWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.rt.block_on(self.buf_writer.write(buf))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.rt.block_on(self.buf_writer.flush())
    }
}

#[cfg(test)]
mod tests {
    use object_store::local::LocalFileSystem;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_object_store() -> InnoFileResult<()> {
        let content = "Hello, ObjectFS!\n";

        let dir = tempdir()?;
        let file_name = "object_fs.txt";
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let file_system = ObjectFS::from_store("file", store)?;
        assert!(!file_system.exists(file_name)?);

        let output_file = file_system.create_new(file_name)?;
        assert!(file_system.exists(file_name)?);

        output_file.writer()?.write_all(content.as_bytes())?;

        let input_file = file_system.open(file_name)?;
        assert_eq!(input_file.metadata()?.len(), content.len() as u64);

        let mut buf = String::new();
        input_file.reader()?.read_to_string(&mut buf)?;
        assert_eq!(buf, content);

        file_system.remove_file(file_name)?;
        assert!(!file_system.exists(file_name)?);

        Ok(())
    }
}
