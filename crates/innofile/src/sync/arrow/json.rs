use std::sync::Arc;

use arrow::{
    array::{RecordBatch, RecordBatchReader as _},
    datatypes::SchemaRef,
};
use arrow_json::{
    reader::infer_json_schema_from_seekable, ArrayWriter, LineDelimitedWriter, Reader,
    ReaderBuilder, WriterBuilder,
};

use crate::{
    error::InnoFileResult,
    sync::{
        fs::{FileRead, FileRef, FileWrite},
        io::Closeable,
    },
};

use super::{ArrowReader, ArrowWriter};

pub struct ArrowJsonReader(Reader<Box<dyn FileRead>>);

impl ArrowJsonReader {
    pub fn new(file: FileRef, schema: Option<SchemaRef>) -> InnoFileResult<Self> {
        let schema = match schema {
            Some(schema) => schema,
            None => {
                let (schema, _) = infer_json_schema_from_seekable(file.reader()?, Some(100))?;
                Arc::new(schema)
            }
        };
        let inner_reader = ReaderBuilder::new(schema).build(file.reader()?)?;
        Ok(Self(inner_reader))
    }
}

impl Iterator for ArrowJsonReader {
    type Item = InnoFileResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.0.next()?.map_err(|e| e.into()))
    }
}

impl ArrowReader for ArrowJsonReader {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }
}

pub enum ArrowJsonWriter {
    JsonArray(ArrayWriter<Box<dyn FileWrite>>),
    LineDelimited(LineDelimitedWriter<Box<dyn FileWrite>>),
}

impl ArrowJsonWriter {
    pub fn new_json_array(file: FileRef) -> InnoFileResult<Self> {
        Ok(Self::JsonArray(
            WriterBuilder::new()
                .with_explicit_nulls(true)
                .build(file.writer()?),
        ))
    }

    pub fn new_line_delimited(file: FileRef) -> InnoFileResult<Self> {
        Ok(Self::LineDelimited(
            WriterBuilder::new()
                .with_explicit_nulls(true)
                .build(file.writer()?),
        ))
    }
}

impl Closeable for ArrowJsonWriter {
    fn close(self) -> InnoFileResult<()> {
        match self {
            Self::JsonArray(mut writer) => writer.finish()?,
            Self::LineDelimited(mut writer) => writer.finish()?,
        }
        Ok(())
    }
}

impl ArrowWriter for ArrowJsonWriter {
    fn write(&mut self, batch: &RecordBatch) -> InnoFileResult<()> {
        match self {
            Self::JsonArray(writer) => writer.write(batch)?,
            Self::LineDelimited(writer) => writer.write(batch)?,
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{super::tests::write_then_read, *};

    #[test]
    fn test_arrow_read_write() -> InnoFileResult<()> {
        write_then_read("json", true)
    }
}
