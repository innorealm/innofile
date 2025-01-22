use std::sync::Arc;

use arrow::{
    array::{RecordBatch, RecordBatchReader as _},
    datatypes::SchemaRef,
};
use arrow_csv::{
    reader::{BufReader, Format},
    ReaderBuilder, Writer, WriterBuilder,
};

use crate::{
    error::{InnoFileError, InnoFileResult},
    sync::{
        fs::{FileRead, FileRef, FileWrite},
        io::Closeable,
    },
};

use super::{ArrowReader, ArrowWriter};

pub struct ArrowCsvReader(BufReader<Box<dyn FileRead>>);

impl ArrowCsvReader {
    pub fn new(
        file: FileRef,
        file_format: impl AsRef<str>,
        schema: Option<SchemaRef>,
    ) -> InnoFileResult<Self> {
        let delimiter = delimiter_from_file_format(file_format)?;
        let schema = match schema {
            Some(schema) => schema,
            None => {
                let (schema, _) = Format::default()
                    .with_header(true)
                    .with_delimiter(delimiter)
                    .infer_schema(file.reader()?, Some(100))?;
                file.reader()?.rewind()?;
                Arc::new(schema)
            }
        };
        let inner_reader = ReaderBuilder::new(schema)
            .with_header(true)
            .with_delimiter(delimiter)
            .build_buffered(file.reader()?)?;
        Ok(Self(inner_reader))
    }
}

impl Iterator for ArrowCsvReader {
    type Item = InnoFileResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.0.next()?.map_err(|e| e.into()))
    }
}

impl ArrowReader for ArrowCsvReader {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }
}

pub struct ArrowCsvWriter(Writer<Box<dyn FileWrite>>);

impl ArrowCsvWriter {
    pub fn new(file: FileRef, file_format: impl AsRef<str>) -> InnoFileResult<Self> {
        let delimiter = delimiter_from_file_format(file_format)?;
        Ok(Self(
            WriterBuilder::new()
                .with_delimiter(delimiter)
                .build(file.writer()?),
        ))
    }
}

impl Closeable for ArrowCsvWriter {
    fn close(self) -> InnoFileResult<()> {
        Ok(())
    }
}

impl ArrowWriter for ArrowCsvWriter {
    fn write(&mut self, batch: &RecordBatch) -> InnoFileResult<()> {
        Ok(self.0.write(batch)?)
    }
}

fn delimiter_from_file_format(file_format: impl AsRef<str>) -> InnoFileResult<u8> {
    match file_format.as_ref().to_lowercase().as_str() {
        "csv" => Ok(b','),
        "dsv" => Ok(b':'),
        "psv" => Ok(b'|'),
        "tsv" => Ok(b'\t'),
        _ => Err(InnoFileError::FileFormatNotSupported(
            file_format.as_ref().to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{super::tests::write_then_read, *};

    #[test]
    fn test_arrow_read_write() -> InnoFileResult<()> {
        for extension in ["csv", "dsv", "psv", "tsv"] {
            write_then_read(extension, true)?;
        }
        Ok(())
    }
}
