use std::{
    pin::Pin,
    task::{Context, Poll},
};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_trait::async_trait;
use futures::{Stream, StreamExt as _};
use orc_rust::{ArrowReaderBuilder as InnerReaderBuilder, ArrowStreamReader as InnerReader};

use crate::{
    error::InnoFileResult,
    tokio::{
        fs::{FileRead, FileRef},
        io::Closeable,
    },
};

use super::{ArrowReader, ArrowWriter};

pub struct ArrowOrcReader(InnerReader<Box<dyn FileRead>>);

impl ArrowOrcReader {
    pub async fn new(file: FileRef) -> InnoFileResult<Self> {
        let inner_reader = InnerReaderBuilder::try_new_async(file.reader().await?)
            .await?
            .build_async();
        Ok(Self(inner_reader))
    }
}

impl Stream for ArrowOrcReader {
    type Item = InnoFileResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx).map_err(|e| e.into())
    }
}

#[async_trait]
impl ArrowReader for ArrowOrcReader {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }
}

pub struct ArrowOrcWriter;

impl ArrowOrcWriter {
    pub async fn new(file: FileRef, schema: SchemaRef) -> InnoFileResult<Self> {
        _ = file;
        _ = schema;
        todo!("ArrowOrcWriter::new")
    }
}

#[async_trait]
impl Closeable for ArrowOrcWriter {
    async fn close(self) -> InnoFileResult<()> {
        todo!("ArrowOrcWriter::close")
    }
}

#[async_trait]
impl ArrowWriter for ArrowOrcWriter {
    async fn write(&mut self, batch: &RecordBatch) -> InnoFileResult<()> {
        _ = batch;
        todo!("ArrowOrcWriter::write")
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
