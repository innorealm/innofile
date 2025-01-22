use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_trait::async_trait;
use futures::Stream;

use crate::{error::InnoFileResult, tokio::io::Closeable};

#[cfg(feature = "csv")]
pub mod csv;
#[cfg(feature = "json")]
pub mod json;
#[cfg(feature = "orc")]
pub mod orc;
#[cfg(feature = "parquet")]
pub mod parquet;

#[async_trait]
pub trait ArrowReader: Stream<Item = InnoFileResult<RecordBatch>> + Unpin {
    fn schema(&self) -> SchemaRef;
}

#[async_trait]
pub trait ArrowWriter: Closeable + Unpin {
    async fn write_batches(&mut self, batches: &[&RecordBatch]) -> InnoFileResult<()> {
        for batch in batches {
            self.write(batch).await?;
        }
        Ok(())
    }

    async fn write(&mut self, batch: &RecordBatch) -> InnoFileResult<()>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{PrimitiveArray, StringArray},
        datatypes::{DataType, Field, Int64Type, Schema},
    };
    use futures::StreamExt as _;
    use tempfile::tempdir;

    use crate::{
        arrow::{ArrowReaderBuilder, ArrowWriterBuilder},
        fs::FileSystemBuilder,
        tokio::fs::FileRef,
    };

    use super::*;

    pub async fn write_then_read(extension: &str, read_with_schema: bool) -> InnoFileResult<()> {
        let path = tempdir()?.path().join(format!("local_fs.{}", extension));
        let path = path.to_str().unwrap();
        let file_system = FileSystemBuilder::from_path(path)?.build_async().await?;

        assert!(!file_system.exists(path).await?);

        let file: FileRef = Arc::from(file_system.create_new(path).await?);
        assert!(file_system.exists(file.path()).await?);

        let nullable = read_with_schema;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, nullable),
            Field::new("name", DataType::Utf8, nullable),
        ]));
        let ids = PrimitiveArray::<Int64Type>::from(vec![1, 2]);
        let names = StringArray::from(vec!["Alex", "Bob"]);
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ids), Arc::new(names)])?;

        let mut arrow_writer = ArrowWriterBuilder::new(Arc::clone(&schema))
            .build_async(Arc::clone(&file))
            .await?;
        arrow_writer.write(&batch).await?;
        arrow_writer.close().await?;

        let file: FileRef = Arc::from(file_system.open(path).await?);

        let mut arrow_reader_builder = ArrowReaderBuilder::new();
        if read_with_schema {
            arrow_reader_builder = arrow_reader_builder.with_schema(Some(Arc::clone(&schema)));
        }
        let mut arrow_reader = arrow_reader_builder.build_async(Arc::clone(&file)).await?;
        if let Some(record_batch) = arrow_reader.next().await {
            let record_batch = record_batch?;
            assert_eq!(record_batch, batch);
        }

        Ok(())
    }
}
