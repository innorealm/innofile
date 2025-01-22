use std::{
    pin::Pin,
    task::{Context, Poll},
};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_trait::async_trait;
use futures::Stream;

use crate::{
    error::InnoFileResult,
    tokio::{fs::FileRef, io::Closeable},
};

use super::{ArrowReader, ArrowWriter};

pub struct ArrowCsvReader;

impl ArrowCsvReader {
    pub async fn new(
        file: FileRef,
        file_format: impl AsRef<str>,
        schema: Option<SchemaRef>,
    ) -> InnoFileResult<Self> {
        _ = file;
        _ = file_format;
        _ = schema;
        todo!("ArrowCsvReader::new")
    }
}

impl Stream for ArrowCsvReader {
    type Item = InnoFileResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        _ = cx;
        todo!("ArrowCsvReader::poll_next")
    }
}

#[async_trait]
impl ArrowReader for ArrowCsvReader {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        todo!("ArrowCsvReader::schema")
    }
}

pub struct ArrowCsvWriter;

impl ArrowCsvWriter {
    pub async fn new(file: FileRef, file_format: impl AsRef<str>) -> InnoFileResult<Self> {
        _ = file;
        _ = file_format;
        todo!("ArrowCsvWriter::new")
    }
}

#[async_trait]
impl Closeable for ArrowCsvWriter {
    async fn close(self) -> InnoFileResult<()> {
        todo!("ArrowCsvWriter::close")
    }
}

#[async_trait]
impl ArrowWriter for ArrowCsvWriter {
    async fn write(&mut self, batch: &RecordBatch) -> InnoFileResult<()> {
        _ = batch;
        todo!("ArrowCsvWriter::write")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_arrow_read_write() -> InnoFileResult<()> {
        Ok(())
    }
}
