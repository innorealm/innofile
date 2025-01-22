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

pub struct ArrowJsonReader;

impl ArrowJsonReader {
    pub async fn new(file: FileRef, schema: Option<SchemaRef>) -> InnoFileResult<Self> {
        _ = file;
        _ = schema;
        todo!("ArrowJsonReader::new")
    }
}

impl Stream for ArrowJsonReader {
    type Item = InnoFileResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        _ = cx;
        todo!("ArrowJsonReader::poll_next")
    }
}

#[async_trait]
impl ArrowReader for ArrowJsonReader {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        todo!("ArrowJsonReader::schema")
    }
}

pub struct ArrowJsonWriter;

impl ArrowJsonWriter {
    pub async fn new_json_array(file: FileRef) -> InnoFileResult<Self> {
        _ = file;
        todo!("ArrowJsonWriter::new_json_array")
    }

    pub async fn new_line_delimited(file: FileRef) -> InnoFileResult<Self> {
        _ = file;
        todo!("ArrowJsonWriter::new_line_delimited")
    }
}

#[async_trait]
impl Closeable for ArrowJsonWriter {
    async fn close(self) -> InnoFileResult<()> {
        todo!("ArrowJsonWriter::close")
    }
}

#[async_trait]
impl ArrowWriter for ArrowJsonWriter {
    async fn write(&mut self, batch: &RecordBatch) -> InnoFileResult<()> {
        _ = batch;
        todo!("ArrowJsonWriter::write")
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
