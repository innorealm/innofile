use std::io::SeekFrom;

use arrow::{
    array::{RecordBatch, RecordBatchReader as _},
    datatypes::SchemaRef,
};
use orc_rust::{
    reader::ChunkReader, ArrowReader as InnerReader, ArrowReaderBuilder as InnerReaderBuilder,
    ArrowWriter as InnerWriter, ArrowWriterBuilder as InnerWriterBuilder,
};

use crate::{
    error::InnoFileResult,
    sync::{
        fs::{FileRead, FileRef, FileWrite},
        io::Closeable,
    },
};

use super::{ArrowReader, ArrowWriter};

pub struct ArrowOrcReader(InnerReader<FileReader>);

impl ArrowOrcReader {
    pub fn new(file: FileRef) -> InnoFileResult<Self> {
        let inner_reader = InnerReaderBuilder::try_new(FileReader(file))?.build();
        Ok(Self(inner_reader))
    }
}

pub struct FileReader(FileRef);

impl ChunkReader for FileReader {
    type T = Box<dyn FileRead>;

    fn len(&self) -> u64 {
        self.0.metadata().map(|m| m.len()).unwrap_or_default()
    }

    fn get_read(&self, offset_from_start: u64) -> std::io::Result<Self::T> {
        let mut reader = self.0.reader()?;
        reader.seek(SeekFrom::Start(offset_from_start))?;
        Ok(reader)
    }
}

impl Iterator for ArrowOrcReader {
    type Item = InnoFileResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.0.next()?.map_err(|e| e.into()))
    }
}

impl ArrowReader for ArrowOrcReader {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }
}

pub struct ArrowOrcWriter(InnerWriter<Box<dyn FileWrite>>);

impl ArrowOrcWriter {
    pub fn new(file: FileRef, schema: SchemaRef) -> InnoFileResult<Self> {
        Ok(Self(
            InnerWriterBuilder::new(file.writer()?, schema).try_build()?,
        ))
    }
}

impl Closeable for ArrowOrcWriter {
    fn close(self) -> InnoFileResult<()> {
        Ok(self.0.close()?)
    }
}

impl ArrowWriter for ArrowOrcWriter {
    fn write(&mut self, batch: &RecordBatch) -> InnoFileResult<()> {
        Ok(self.0.write(batch)?)
    }
}

#[cfg(test)]
mod tests {
    use super::{super::tests::write_then_read, *};

    #[test]
    fn test_arrow_read_write() -> InnoFileResult<()> {
        write_then_read("orc", false)
    }
}
