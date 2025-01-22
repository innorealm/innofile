use std::collections::HashMap;

use fluent_uri::UriRef;

#[cfg(feature = "sync")]
use crate::sync::fs::{local::LocalFS as SyncLocalFS, FileSystem as SyncFileSystem};
#[cfg(feature = "tokio")]
use crate::tokio::fs::{local::LocalFS as AsyncLocalFS, FileSystem as AsyncFileSystem};
use crate::{
    error::{InnoFileError, InnoFileResult},
    with_field,
};

#[cfg(feature = "object_store")]
use self::object_store::*;

#[cfg(feature = "object_store")]
mod object_store {
    #[cfg(feature = "sync")]
    pub use crate::sync::fs::object_store::ObjectFS as SyncObjectFS;
    #[cfg(feature = "tokio")]
    pub use crate::tokio::fs::object_store::ObjectFS as AsyncObjectFS;
}

#[derive(Debug, Default)]
pub struct FileSystemBuilder {
    scheme: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    properties: HashMap<String, String>,
}

impl FileSystemBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn from_path(path: impl AsRef<str>) -> InnoFileResult<Self> {
        let uri = UriRef::parse(path.as_ref())?;

        let scheme = uri.scheme();

        let authority = uri.authority();
        let host = authority.map(|a| a.host());
        let port = authority
            .and_then(|a| a.port_to_u16().transpose())
            .transpose()?;

        Ok(Self::new()
            .with_scheme(scheme)
            .with_host(host)
            .with_port(port))
    }

    with_field!(with_scheme, scheme, String);

    with_field!(with_host, host, String);

    with_field!(with_port, port, u16);

    pub fn with_property(mut self, key: impl ToString, value: impl ToString) -> Self {
        self.properties.insert(key.to_string(), value.to_string());
        self
    }

    pub fn with_properties(
        mut self,
        properties: impl IntoIterator<Item = (impl ToString, impl ToString)>,
    ) -> Self {
        self.properties.extend(
            properties
                .into_iter()
                .map(|p| (p.0.to_string(), p.1.to_string())),
        );
        self
    }

    #[cfg(feature = "tokio")]
    pub async fn build_async(self) -> InnoFileResult<Box<dyn AsyncFileSystem>> {
        Ok(match self.scheme {
            None => Box::new(AsyncLocalFS),

            Some(scheme) => match scheme.to_lowercase().as_str() {
                "file" => Box::new(AsyncLocalFS),

                #[cfg(feature = "s3")]
                "s3" | "s3a" => {
                    Box::new(AsyncObjectFS::new_s3(scheme, self.host, self.properties)?)
                }

                _ => Err(InnoFileError::SchemeNotSupported(scheme))?,
            },
        })
    }

    #[cfg(feature = "sync")]
    pub fn build_sync(self) -> InnoFileResult<Box<dyn SyncFileSystem>> {
        Ok(match self.scheme {
            None => Box::new(SyncLocalFS),

            Some(scheme) => match scheme.to_lowercase().as_str() {
                "file" => Box::new(SyncLocalFS),

                #[cfg(feature = "s3")]
                "s3" | "s3a" => Box::new(SyncObjectFS::new_s3(scheme, self.host, self.properties)?),

                _ => Err(InnoFileError::SchemeNotSupported(scheme))?,
            },
        })
    }
}
