use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_trait::async_trait;
use futures::{Stream, StreamExt as _};
use parquet::arrow::{
    async_reader::ParquetRecordBatchStream, AsyncArrowWriter as InnerWriter,
    ParquetRecordBatchStreamBuilder,
};

use crate::{
    error::InnoFileResult,
    tokio::{
        fs::{FileRead, FileRef, FileWrite},
        io::Closeable,
    },
};

use super::{ArrowReader, ArrowWriter};

pub struct ArrowParquetReader(ParquetRecordBatchStream<Box<dyn FileRead>>);

impl ArrowParquetReader {
    pub async fn new(file: FileRef) -> InnoFileResult<Self> {
        let inner_reader = ParquetRecordBatchStreamBuilder::new(file.reader().await?)
            .await?
            .build()?;
        Ok(Self(inner_reader))
    }
}

impl Stream for ArrowParquetReader {
    type Item = InnoFileResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx).map_err(|e| e.into())
    }
}

#[async_trait]
impl ArrowReader for ArrowParquetReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(self.0.schema())
    }
}

pub struct ArrowParquetWriter(InnerWriter<Box<dyn FileWrite>>);

impl ArrowParquetWriter {
    pub async fn new(file: FileRef, schema: SchemaRef) -> InnoFileResult<Self> {
        let inner_writer = InnerWriter::try_new(file.writer().await?, schema, None)?;
        Ok(Self(inner_writer))
    }
}

#[async_trait]
impl Closeable for ArrowParquetWriter {
    async fn close(self) -> InnoFileResult<()> {
        self.0.close().await?;
        Ok(())
    }
}

#[async_trait]
impl ArrowWriter for ArrowParquetWriter {
    async fn write(&mut self, batch: &RecordBatch) -> InnoFileResult<()> {
        Ok(self.0.write(batch).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::{super::tests::write_then_read, *};

    #[tokio::test]
    async fn test_arrow_read_write() -> InnoFileResult<()> {
        write_then_read("parquet", false).await
    }
}
