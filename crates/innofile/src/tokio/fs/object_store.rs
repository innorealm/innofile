use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use futures::{StreamExt as _, TryStreamExt as _};
#[cfg(feature = "s3")]
use object_store::aws::AmazonS3Builder;
use object_store::{
    buffered::{BufReader, BufWriter},
    path::Path,
    Error, ObjectMeta, ObjectStore, PutMode, PutPayload, PutResult,
};

use crate::error::InnoFileResult;

use super::{File, FileRead, FileSystem, FileWrite, Metadata};

#[derive(Debug)]
pub struct ObjectFS {
    scheme: String,
    store: Arc<dyn ObjectStore>,
}

impl ObjectFS {
    pub fn from_store(scheme: impl ToString, store: Arc<dyn ObjectStore>) -> InnoFileResult<Self> {
        Ok(Self {
            scheme: scheme.to_string(),
            store,
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

    async fn head(&self, path: impl AsRef<str>) -> InnoFileResult<ObjectMeta, Error> {
        self.store.head(&Path::parse(path)?).await
    }

    async fn put_empty_file(
        &self,
        path: impl AsRef<str>,
        put_mode: PutMode,
    ) -> InnoFileResult<PutResult, Error> {
        self.store
            .put_opts(&Path::parse(path)?, PutPayload::new(), put_mode.into())
            .await
    }

    fn make_file(&self, path: impl ToString) -> ObjectFile {
        ObjectFile::new(path, Arc::clone(&self.store))
    }
}

#[async_trait]
impl FileSystem for ObjectFS {
    fn scheme(&self) -> &str {
        &self.scheme
    }

    async fn exists(&self, path: &str) -> InnoFileResult<bool> {
        match self.head(path).await {
            Ok(_) => Ok(true),
            Err(Error::NotFound { path: _, source: _ }) => Ok(false),
            Err(error) => Err(error)?,
        }
    }

    async fn open(&self, path: &str) -> InnoFileResult<Box<dyn File>> {
        self.head(path).await?;
        Ok(Box::new(self.make_file(path)))
    }

    async fn create(&self, path: &str) -> InnoFileResult<Box<dyn File>> {
        self.put_empty_file(path, PutMode::Overwrite).await?;
        Ok(Box::new(self.make_file(path)))
    }

    async fn create_new(&self, path: &str) -> InnoFileResult<Box<dyn File>> {
        self.put_empty_file(path, PutMode::Create).await?;
        Ok(Box::new(self.make_file(path)))
    }

    async fn remove_dir(&self, path: &str) -> InnoFileResult<()> {
        let locations = self
            .store
            .list(Some(&Path::parse(path)?))
            .map_ok(|m| m.location)
            .boxed();
        self.store
            .delete_stream(locations)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(())
    }

    async fn remove_file(&self, path: &str) -> InnoFileResult<()> {
        Ok(self.store.delete(&Path::parse(path)?).await?)
    }
}

#[derive(Debug)]
pub struct ObjectFile {
    path: String,
    store: Arc<dyn ObjectStore>,
}

impl ObjectFile {
    pub fn new(path: impl ToString, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            path: path.to_string(),
            store,
        }
    }

    pub async fn object_meta(&self) -> InnoFileResult<ObjectMeta> {
        Ok(self.store.head(&Path::parse(&self.path)?).await?)
    }
}

#[async_trait]
impl File for ObjectFile {
    fn path(&self) -> &str {
        &self.path
    }

    async fn metadata(&self) -> InnoFileResult<Box<dyn Metadata>> {
        Ok(Box::new(self.object_meta().await?))
    }

    async fn reader(&self) -> InnoFileResult<Box<dyn FileRead>> {
        let buf_reader = BufReader::new(Arc::clone(&self.store), &self.object_meta().await?);
        Ok(Box::new(buf_reader))
    }

    async fn writer(&self) -> InnoFileResult<Box<dyn FileWrite>> {
        let buf_writer = BufWriter::new(Arc::clone(&self.store), Path::parse(&self.path)?);
        Ok(Box::new(buf_writer))
    }
}

#[async_trait]
impl Metadata for ObjectMeta {
    fn len(&self) -> u64 {
        self.size as _
    }
}

#[cfg(test)]
mod tests {
    use object_store::local::LocalFileSystem;
    use tempfile::tempdir;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;

    #[tokio::test]
    async fn test_object_store() -> InnoFileResult<()> {
        let content = "Hello, ObjectFS!\n";

        let dir = tempdir()?;
        let file_name = "object_fs.txt";
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let file_system = ObjectFS::from_store("file", store)?;
        assert!(!file_system.exists(file_name).await?);

        let output_file = file_system.create_new(file_name).await?;
        assert!(file_system.exists(file_name).await?);

        let mut writer = output_file.writer().await?;
        writer.write_all(content.as_bytes()).await?;
        writer.shutdown().await?;

        let input_file = file_system.open(file_name).await?;
        assert_eq!(input_file.metadata().await?.len(), content.len() as u64);

        let mut buf = String::new();
        input_file.reader().await?.read_to_string(&mut buf).await?;
        assert_eq!(buf, content);

        file_system.remove_file(file_name).await?;
        assert!(!file_system.exists(file_name).await?);

        Ok(())
    }
}
