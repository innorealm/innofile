use thiserror::Error as ThisError;

pub type InnoFileResult<T, E = InnoFileError> = Result<T, E>;

#[derive(Debug, ThisError)]
pub enum InnoFileError {
    #[error("ArrowError: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("File format not found")]
    FileFormatNotFound,

    #[error("File format not supported: {0}")]
    FileFormatNotSupported(String),

    #[error("File system not supported: {0}")]
    FileSystemNotSupported(String),

    #[error("IoError: {0}")]
    Io(#[from] std::io::Error),

    #[cfg(feature = "object_store")]
    #[error("ObjectStoreError: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[cfg(feature = "orc")]
    #[error("OrcError: {0}")]
    Orc(#[from] orc_rust::error::OrcError),

    #[error(transparent)]
    Other(#[from] anyhow::Error),

    #[cfg(feature = "parquet")]
    #[error("ParquetError: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("ParseIntError: {0}")]
    ParseInt(#[from] core::num::ParseIntError),

    #[error("Scheme not supported: {0}")]
    SchemeNotSupported(String),

    #[error("UriBuildError: {0}")]
    UriBuild(#[from] fluent_uri::error::BuildError),

    #[error("UriParseError: {0}")]
    UriParse(#[from] fluent_uri::error::ParseError),

    #[error("UriResolveError: {0}")]
    UriResolve(#[from] fluent_uri::error::ResolveError),
}

#[cfg(feature = "object_store")]
impl From<object_store::path::Error> for InnoFileError {
    fn from(value: object_store::path::Error) -> Self {
        Self::ObjectStore(object_store::Error::InvalidPath { source: value })
    }
}

#[cfg(feature = "parquet")]
impl From<InnoFileError> for parquet::errors::ParquetError {
    fn from(value: InnoFileError) -> Self {
        parquet::errors::ParquetError::External(Box::new(value))
    }
}

impl From<InnoFileError> for std::io::Error {
    fn from(value: InnoFileError) -> Self {
        match value {
            InnoFileError::Io(error) => error,
            error => std::io::Error::other(error),
        }
    }
}
