use std::error::Error;
use std::marker::PhantomData;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

use crate::{D1MigrationGenerator, SchemaAccess};

/// Schema-aware CLI for generating Cloudflare D1 migration files locally.
#[derive(Debug)]
pub struct VitrailCli<S> {
    _schema: PhantomData<S>,
}

impl<S> Default for VitrailCli<S> {
    fn default() -> Self {
        Self {
            _schema: PhantomData,
        }
    }
}

impl<S> VitrailCli<S>
where
    S: SchemaAccess,
{
    /// Parses command-line arguments and runs the D1 migration generator.
    pub async fn run(self) -> Result<(), Box<dyn Error>> {
        Cli::parse().run::<S>().await
    }
}

/// Runs the schema-aware D1 migration generation CLI.
///
/// This CLI only generates migration files. Wrangler remains responsible for
/// applying migrations and maintaining D1 migration history.
pub async fn run_cli<S>() -> Result<(), Box<dyn Error>>
where
    S: SchemaAccess,
{
    VitrailCli::<S>::default().run().await
}

#[derive(Debug, Parser)]
#[command(name = "vitrail")]
#[command(about = "Vitrail CLI for generating Cloudflare D1 migrations locally")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

impl Cli {
    async fn run<S>(self) -> Result<(), Box<dyn Error>>
    where
        S: SchemaAccess,
    {
        self.command.run::<S>().await
    }
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(subcommand)]
    Migrate(MigrateCommand),
}

impl Command {
    async fn run<S>(self) -> Result<(), Box<dyn Error>>
    where
        S: SchemaAccess,
    {
        match self {
            Self::Migrate(command) => command.run::<S>().await,
        }
    }
}

#[derive(Debug, Subcommand)]
enum MigrateCommand {
    #[command(about = "Create a D1 migration from the current schema")]
    Dev(MigrateDevArgs),
}

impl MigrateCommand {
    async fn run<S>(self) -> Result<(), Box<dyn Error>>
    where
        S: SchemaAccess,
    {
        match self {
            Self::Dev(args) => args.run::<S>().await,
        }
    }
}

#[derive(Debug, Args)]
struct MigrateDevArgs {
    #[arg(long, short, help = "Human-readable name for the new migration")]
    name: String,
    #[arg(
        long,
        default_value = "migrations",
        help = "Path to the nested migrations directory"
    )]
    migrations_path: PathBuf,
}

impl MigrateDevArgs {
    async fn run<S>(self) -> Result<(), Box<dyn Error>>
    where
        S: SchemaAccess,
    {
        let generator = D1MigrationGenerator::new(self.migrations_path);

        match generator.generate_migration::<S>(&self.name).await? {
            Some(generated) => {
                println!(
                    "Created migration `{}` at `{}`",
                    generated.migration().name(),
                    generated
                        .migration()
                        .sql_path()
                        .expect("generated migration should always have a filesystem path")
                        .display(),
                );
                print!("{}", generated.sql());
            }
            None => {
                println!("Schema is already up to date. No migration was created.");
            }
        }

        Ok(())
    }
}
