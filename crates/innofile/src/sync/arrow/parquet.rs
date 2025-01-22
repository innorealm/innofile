use std::io::{Read, SeekFrom};

use arrow::{
    array::{RecordBatch, RecordBatchReader as _},
    datatypes::SchemaRef,
};
use bytes::Bytes;
use parquet::{
    arrow::{
        arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
        ArrowWriter as InnerWriter,
    },
    errors::ParquetError,
    file::reader::{ChunkReader, Length},
};

use crate::{
    error::InnoFileResult,
    sync::{
        fs::{FileRead, FileRef, FileWrite},
        io::Closeable,
    },
};

use super::{ArrowReader, ArrowWriter};

pub struct ArrowParquetReader(ParquetRecordBatchReader);

impl ArrowParquetReader {
    pub fn new(file: FileRef) -> InnoFileResult<Self> {
        let inner_reader = ParquetRecordBatchReaderBuilder::try_new(FileReader(file))?.build()?;
        Ok(Self(inner_reader))
    }
}

pub struct FileReader(FileRef);

impl Length for FileReader {
    fn len(&self) -> u64 {
        self.0.metadata().map(|m| m.len()).unwrap_or_default()
    }
}

impl ChunkReader for FileReader {
    type T = Box<dyn FileRead>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let mut reader = self.0.reader()?;
        reader.seek(SeekFrom::Start(start))?;
        Ok(reader)
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let reader = self.get_read(start)?;
        let mut buf = Vec::with_capacity(length);
        let read = reader.take(length as _).read_to_end(&mut buf)?;
        if read != length {
            Err(ParquetError::EOF(format!(
                "Expected to read {} bytes, read only {}",
                length, read
            )))?
        }
        Ok(buf.into())
    }
}

impl Iterator for ArrowParquetReader {
    type Item = InnoFileResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.0.next()?.map_err(|e| e.into()))
    }
}

impl ArrowReader for ArrowParquetReader {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }
}

pub struct ArrowParquetWriter(InnerWriter<Box<dyn FileWrite>>);

impl ArrowParquetWriter {
    pub fn new(file: FileRef, schema: SchemaRef) -> InnoFileResult<Self> {
        let inner_writer = InnerWriter::try_new(file.writer()?, schema, None)?;
        Ok(Self(inner_writer))
    }
}

impl Closeable for ArrowParquetWriter {
    fn close(self) -> InnoFileResult<()> {
        self.0.close()?;
        Ok(())
    }
}

impl ArrowWriter for ArrowParquetWriter {
    fn write(&mut self, batch: &RecordBatch) -> InnoFileResult<()> {
        Ok(self.0.write(batch)?)
    }
}

#[cfg(test)]
mod tests {
    use super::{super::tests::write_then_read, *};

    #[test]
    fn test_arrow_read_write() -> InnoFileResult<()> {
        write_then_read("parquet", false)
    }
}
