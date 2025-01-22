use std::sync::Arc;

use arrow::array::RecordBatch;
#[cfg(all(feature = "sync", feature = "orc"))]
use innofile::{
    arrow::{ArrowReaderBuilder, ArrowWriterBuilder},
    error::InnoFileResult,
    fs::FileSystemBuilder,
    io::SyncCloseable as _,
};

#[cfg(all(feature = "sync", feature = "orc"))]
fn main() -> InnoFileResult<()> {
    // prepare Arrow batch to write
    let batch: RecordBatch = make_record_batch()?;
    // specify path to write with file extension
    let path = "/tmp/innofile_tmp/test_sync_api.orc";

    let file_system = FileSystemBuilder::from_path(path)?.build_sync()?;

    let file = Arc::from(file_system.create(path)?);
    let mut arrow_writer = ArrowWriterBuilder::new(batch.schema()).build_sync(file)?;
    arrow_writer.write(&batch)?;
    arrow_writer.close()?;

    let file = Arc::from(file_system.open(path)?);
    let mut arrow_reader = ArrowReaderBuilder::new().build_sync(file)?;
    while let Some(record_batch) = arrow_reader.next() {
        println!("record_batch num_rows = {}", record_batch?.num_rows());
    }

    Ok(())
}

#[cfg(all(feature = "sync", feature = "orc"))]
fn make_record_batch() -> InnoFileResult<RecordBatch> {
    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
    };

    let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)])?;
    Ok(batch)
}

#[cfg(any(not(feature = "sync"), not(feature = "orc")))]
fn main() {}
