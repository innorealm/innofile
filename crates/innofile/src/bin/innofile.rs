use std::sync::Arc;

use anyhow::anyhow;
use arrow::util::data_gen::create_random_batch;
use clap::{Args, Parser, Subcommand};
#[cfg(feature = "sync")]
use innofile::{
    arrow::{ArrowReaderBuilder, ArrowWriterBuilder},
    error::InnoFileResult,
    fs::FileSystemBuilder,
    io::SyncCloseable as _,
};

#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Convert files between supported formats
    Convert(ConvertArgs),

    /// Generate files based on example file
    Generate(GenerateArgs),
}

#[derive(Debug, Args)]
struct ConvertArgs {
    /// File path to convert from
    #[arg(long = "from")]
    from_path: String,

    /// File path to convert to
    #[arg(long = "to")]
    to_path: String,
}

#[derive(Debug, Args)]
struct GenerateArgs {
    /// Example of records to generate
    #[arg(long = "example")]
    example_path: String,

    /// Size of records to generate
    #[arg(long, default_value_t = 1)]
    size: usize,

    /// Null density of nullable fields to generate
    #[arg(long, default_value_t = 0.0)]
    null_density: f32,

    /// True density of boolean fields to generate
    #[arg(long, default_value_t = 0.5)]
    true_density: f32,

    /// Path of output file
    #[arg(value_name = "OUTPUT_PATH")]
    output_paths: Vec<String>,
}

#[cfg(feature = "sync")]
fn main() -> InnoFileResult<()> {
    let cli = Cli::parse();
    match &cli.command {
        Commands::Convert(args) => do_convert(args)?,
        Commands::Generate(args) => do_generate(args)?,
    }
    Ok(())
}

#[cfg(feature = "sync")]
fn do_convert(args: &ConvertArgs) -> InnoFileResult<()> {
    // create reader
    let from_file_system = FileSystemBuilder::from_path(&args.from_path)?.build_sync()?;
    let from_file = Arc::from(from_file_system.open(&args.from_path)?);
    let arrow_reader = ArrowReaderBuilder::new().build_sync(from_file)?;

    // create writer
    let file_system = FileSystemBuilder::from_path(&args.to_path)?.build_sync()?;
    let file = Arc::from(file_system.create(&args.to_path)?);
    let mut arrow_writer = ArrowWriterBuilder::new(arrow_reader.schema()).build_sync(file)?;

    // iteratively read and write record batches
    for record_batch in arrow_reader {
        arrow_writer.write(&record_batch?)?;
    }
    arrow_writer.close()?;

    Ok(())
}

#[cfg(feature = "sync")]
fn do_generate(args: &GenerateArgs) -> InnoFileResult<()> {
    // get schema from example file
    let file_system = FileSystemBuilder::from_path(&args.example_path)?.build_sync()?;
    let file = Arc::from(file_system.open(&args.example_path)?);
    let mut arrow_reader = ArrowReaderBuilder::new().build_sync(file)?;
    let schema = arrow_reader
        .next()
        .ok_or_else(|| anyhow!("Example file contains no records"))??
        .schema();

    // generate random records with schema
    let record_batch =
        create_random_batch(schema, args.size, args.null_density, args.true_density)?;

    // write records to each output file
    for output_path in &args.output_paths {
        let file_system = FileSystemBuilder::from_path(output_path)?.build_sync()?;
        let file = Arc::from(file_system.create(output_path)?);
        let mut arrow_writer = ArrowWriterBuilder::new(record_batch.schema()).build_sync(file)?;
        arrow_writer.write(&record_batch)?;
        arrow_writer.close()?;
    }

    Ok(())
}

#[cfg(not(feature = "sync"))]
fn main() {}

#[cfg(feature = "sync")]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parsing_convert_command() -> InnoFileResult<()> {
        let (from_path, to_path) = ("from.orc", "to.parquet");
        let cmd = format!("innofile convert --from {from_path} --to {to_path}");
        if let Commands::Convert(args) = Cli::parse_from(cmd.split_whitespace()).command {
            assert_eq!(args.from_path, from_path);
            assert_eq!(args.to_path, to_path);
        } else {
            assert!(false, "Convert command not parsed correctly");
        }
        Ok(())
    }

    #[test]
    fn test_parsing_generate_command() -> InnoFileResult<()> {
        let (example_path, size, null_density, true_density) = ("example.csv", 10, 0.2, 0.6);
        let output_paths = [
            "output.csv",
            "output.dsv",
            "output.psv",
            "output.tsv",
            "output.json",
            "output.orc",
            "output.parquet",
        ];
        let cmd = format!(
            "innofile generate --example {example_path} --size {size} --null-density {null_density} --true-density {true_density} {}",
            output_paths.join(" ")
        );
        if let Commands::Generate(args) = Cli::parse_from(cmd.split_whitespace()).command {
            assert_eq!(args.example_path, example_path);
            assert_eq!(args.size, size);
            assert_eq!(args.null_density, null_density);
            assert_eq!(args.true_density, true_density);
            assert_eq!(args.output_paths, output_paths);
        } else {
            assert!(false, "Generate command not parsed correctly");
        }
        Ok(())
    }
}
