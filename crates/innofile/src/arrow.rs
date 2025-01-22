use arrow::datatypes::SchemaRef;

#[cfg(feature = "sync")]
use crate::sync::{
    arrow::{ArrowReader as SyncArrowReader, ArrowWriter as SyncArrowWriter},
    fs::FileRef as SyncFileRef,
};
#[cfg(feature = "tokio")]
use crate::tokio::{
    arrow::{ArrowReader as AsyncArrowReader, ArrowWriter as AsyncArrowWriter},
    fs::FileRef as AsyncFileRef,
};
use crate::{
    error::{InnoFileError, InnoFileResult},
    utils::path_extension,
    with_field,
};

#[cfg(feature = "csv")]
use self::csv::*;
#[cfg(feature = "json")]
use self::json::*;
#[cfg(feature = "orc")]
use self::orc::*;
#[cfg(feature = "parquet")]
use self::parquet::*;

#[cfg(feature = "csv")]
mod csv {
    #[cfg(feature = "sync")]
    pub use crate::sync::arrow::csv::{
        ArrowCsvReader as SyncArrowCsvReader, ArrowCsvWriter as SyncArrowCsvWriter,
    };
    #[cfg(feature = "tokio")]
    pub use crate::tokio::arrow::csv::{
        ArrowCsvReader as AsyncArrowCsvReader, ArrowCsvWriter as AsyncArrowCsvWriter,
    };
}

#[cfg(feature = "json")]
mod json {
    #[cfg(feature = "sync")]
    pub use crate::sync::arrow::json::{
        ArrowJsonReader as SyncArrowJsonReader, ArrowJsonWriter as SyncArrowJsonWriter,
    };
    #[cfg(feature = "tokio")]
    pub use crate::tokio::arrow::json::{
        ArrowJsonReader as AsyncArrowJsonReader, ArrowJsonWriter as AsyncArrowJsonWriter,
    };
}

#[cfg(feature = "orc")]
mod orc {
    #[cfg(feature = "sync")]
    pub use crate::sync::arrow::orc::{
        ArrowOrcReader as SyncArrowOrcReader, ArrowOrcWriter as SyncArrowOrcWriter,
    };
    #[cfg(feature = "tokio")]
    pub use crate::tokio::arrow::orc::{
        ArrowOrcReader as AsyncArrowOrcReader, ArrowOrcWriter as AsyncArrowOrcWriter,
    };
}

#[cfg(feature = "parquet")]
mod parquet {
    #[cfg(feature = "sync")]
    pub use crate::sync::arrow::parquet::{
        ArrowParquetReader as SyncArrowParquetReader, ArrowParquetWriter as SyncArrowParquetWriter,
    };
    #[cfg(feature = "tokio")]
    pub use crate::tokio::arrow::parquet::{
        ArrowParquetReader as AsyncArrowParquetReader,
        ArrowParquetWriter as AsyncArrowParquetWriter,
    };
}

#[derive(Debug, Default)]
pub struct ArrowReaderBuilder {
    file_format: Option<String>,
    schema: Option<SchemaRef>,
}

impl ArrowReaderBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    with_field!(with_file_format, file_format, String);

    with_field!(with_schema, schema, SchemaRef);

    #[cfg(feature = "tokio")]
    pub async fn build_async(
        self,
        file: AsyncFileRef,
    ) -> InnoFileResult<Box<dyn AsyncArrowReader>> {
        let file_format = if self.file_format.is_some() {
            self.file_format
        } else {
            path_extension(file.path())?
        };
        Ok(match file_format {
            None => Err(InnoFileError::FileFormatNotFound)?,

            Some(file_format) => match file_format.to_lowercase().as_str() {
                #[cfg(feature = "csv")]
                "csv" | "dsv" | "psv" | "tsv" => {
                    Box::new(AsyncArrowCsvReader::new(file, file_format, self.schema).await?)
                }

                #[cfg(feature = "json")]
                "json" => Box::new(AsyncArrowJsonReader::new(file, self.schema).await?),

                #[cfg(feature = "orc")]
                "orc" => Box::new(AsyncArrowOrcReader::new(file).await?),

                #[cfg(feature = "parquet")]
                "parquet" => Box::new(AsyncArrowParquetReader::new(file).await?),

                _ => Err(InnoFileError::FileFormatNotSupported(file_format))?,
            },
        })
    }

    #[cfg(feature = "sync")]
    pub fn build_sync(self, file: SyncFileRef) -> InnoFileResult<Box<dyn SyncArrowReader>> {
        let file_format = if self.file_format.is_some() {
            self.file_format
        } else {
            path_extension(file.path())?
        };
        Ok(match file_format {
            None => Err(InnoFileError::FileFormatNotFound)?,

            Some(file_format) => match file_format.to_lowercase().as_str() {
                #[cfg(feature = "csv")]
                "csv" | "dsv" | "psv" | "tsv" => {
                    Box::new(SyncArrowCsvReader::new(file, file_format, self.schema)?)
                }

                #[cfg(feature = "json")]
                "json" => Box::new(SyncArrowJsonReader::new(file, self.schema)?),

                #[cfg(feature = "orc")]
                "orc" => Box::new(SyncArrowOrcReader::new(file)?),

                #[cfg(feature = "parquet")]
                "parquet" => Box::new(SyncArrowParquetReader::new(file)?),

                _ => Err(InnoFileError::FileFormatNotSupported(file_format))?,
            },
        })
    }
}

#[derive(Debug)]
pub struct ArrowWriterBuilder {
    file_format: Option<String>,
    schema: SchemaRef,
}

impl ArrowWriterBuilder {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            file_format: None,
            schema,
        }
    }

    with_field!(with_file_format, file_format, String);

    #[cfg(feature = "tokio")]
    pub async fn build_async(
        self,
        file: AsyncFileRef,
    ) -> InnoFileResult<Box<dyn AsyncArrowWriter>> {
        let file_format = if self.file_format.is_some() {
            self.file_format
        } else {
            path_extension(file.path())?
        };
        Ok(match file_format {
            None => Err(InnoFileError::FileFormatNotFound)?,

            Some(file_format) => match file_format.to_lowercase().as_str() {
                #[cfg(feature = "csv")]
                "csv" | "dsv" | "psv" | "tsv" => {
                    Box::new(AsyncArrowCsvWriter::new(file, file_format).await?)
                }

                #[cfg(feature = "json")]
                "json" => Box::new(AsyncArrowJsonWriter::new_line_delimited(file).await?),

                #[cfg(feature = "orc")]
                "orc" => Box::new(AsyncArrowOrcWriter::new(file, self.schema).await?),

                #[cfg(feature = "parquet")]
                "parquet" => Box::new(AsyncArrowParquetWriter::new(file, self.schema).await?),

                _ => Err(InnoFileError::FileFormatNotSupported(file_format))?,
            },
        })
    }

    #[cfg(feature = "sync")]
    pub fn build_sync(self, file: SyncFileRef) -> InnoFileResult<Box<dyn SyncArrowWriter>> {
        let file_format = if self.file_format.is_some() {
            self.file_format
        } else {
            path_extension(file.path())?
        };
        Ok(match file_format {
            None => Err(InnoFileError::FileFormatNotFound)?,

            Some(file_format) => match file_format.to_lowercase().as_str() {
                #[cfg(feature = "csv")]
                "csv" | "dsv" | "psv" | "tsv" => {
                    Box::new(SyncArrowCsvWriter::new(file, file_format)?)
                }

                #[cfg(feature = "json")]
                "json" => Box::new(SyncArrowJsonWriter::new_line_delimited(file)?),

                #[cfg(feature = "orc")]
                "orc" => Box::new(SyncArrowOrcWriter::new(file, self.schema)?),

                #[cfg(feature = "parquet")]
                "parquet" => Box::new(SyncArrowParquetWriter::new(file, self.schema)?),

                _ => Err(InnoFileError::FileFormatNotSupported(file_format))?,
            },
        })
    }
}
