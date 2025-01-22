use arrow::{array::RecordBatch, datatypes::SchemaRef};

use crate::{error::InnoFileResult, sync::io::Closeable};

#[cfg(feature = "csv")]
pub mod csv;
#[cfg(feature = "json")]
pub mod json;
#[cfg(feature = "orc")]
pub mod orc;
#[cfg(feature = "parquet")]
pub mod parquet;

pub trait ArrowReader: Iterator<Item = InnoFileResult<RecordBatch>> {
    fn schema(&self) -> SchemaRef;
}

pub trait ArrowWriter: Closeable {
    fn write_batches(&mut self, batches: &[&RecordBatch]) -> InnoFileResult<()> {
        for batch in batches {
            self.write(batch)?;
        }
        Ok(())
    }

    fn write(&mut self, batch: &RecordBatch) -> InnoFileResult<()>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{PrimitiveArray, StringArray},
        datatypes::{DataType, Field, Int64Type, Schema},
    };
    use tempfile::tempdir;

    use crate::{
        arrow::{ArrowReaderBuilder, ArrowWriterBuilder},
        fs::FileSystemBuilder,
        sync::fs::FileRef,
    };

    use super::*;

    pub fn write_then_read(extension: &str, read_with_schema: bool) -> InnoFileResult<()> {
        let path = tempdir()?.path().join(format!("local_fs.{}", extension));
        let path = path.to_str().unwrap();
        let file_system = FileSystemBuilder::from_path(path)?.build_sync()?;

        assert!(!file_system.exists(path)?);

        let file: FileRef = Arc::from(file_system.create_new(path)?);
        assert!(file_system.exists(file.path())?);

        let nullable = read_with_schema;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, nullable),
            Field::new("name", DataType::Utf8, nullable),
        ]));
        let ids = PrimitiveArray::<Int64Type>::from(vec![1, 2]);
        let names = StringArray::from(vec!["Alex", "Bob"]);
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ids), Arc::new(names)])?;

        let mut arrow_writer =
            ArrowWriterBuilder::new(Arc::clone(&schema)).build_sync(Arc::clone(&file))?;
        arrow_writer.write(&batch)?;
        arrow_writer.close()?;

        let file: FileRef = Arc::from(file_system.open(path)?);

        let mut arrow_reader_builder = ArrowReaderBuilder::new();
        if read_with_schema {
            arrow_reader_builder = arrow_reader_builder.with_schema(Some(Arc::clone(&schema)));
        }
        let mut arrow_reader = arrow_reader_builder.build_sync(Arc::clone(&file))?;
        if let Some(record_batch) = arrow_reader.next() {
            let record_batch = record_batch?;
            assert_eq!(record_batch, batch);
        }

        Ok(())
    }
}
