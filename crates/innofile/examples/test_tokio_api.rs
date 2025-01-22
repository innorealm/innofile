use std::sync::Arc;

use arrow::array::RecordBatch;
use futures::StreamExt as _;
#[cfg(all(feature = "tokio", feature = "parquet"))]
use innofile::{
    arrow::{ArrowReaderBuilder, ArrowWriterBuilder},
    error::InnoFileResult,
    fs::FileSystemBuilder,
    io::AsyncCloseable as _,
};

#[cfg(all(feature = "tokio", feature = "parquet"))]
#[tokio::main]
async fn main() -> InnoFileResult<()> {
    // prepare Arrow batch to write
    let batch: RecordBatch = make_record_batch()?;
    // specify path to write with file extension
    let path = "/tmp/innofile_tmp/test_tokio_api.parquet";

    let file_system = FileSystemBuilder::from_path(path)?.build_async().await?;

    let file = Arc::from(file_system.create(path).await?);
    let mut arrow_writer = ArrowWriterBuilder::new(batch.schema())
        .build_async(file)
        .await?;
    arrow_writer.write(&batch).await?;
    arrow_writer.close().await?;

    let file = Arc::from(file_system.open(path).await?);
    let mut arrow_reader = ArrowReaderBuilder::new().build_async(file).await?;
    while let Some(record_batch) = arrow_reader.next().await {
        println!("record_batch num_rows = {}", record_batch?.num_rows());
    }

    Ok(())
}

#[cfg(all(feature = "tokio", feature = "parquet"))]
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

#[cfg(any(not(feature = "tokio"), not(feature = "parquet")))]
fn main() {}
