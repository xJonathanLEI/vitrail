use std::error::Error;
use std::marker::PhantomData;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use vitrail_pg_core::{PostgresMigrator, SchemaAccess};

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
    pub async fn run(self) -> Result<(), Box<dyn Error>> {
        Cli::parse().run::<S>().await
    }
}

pub async fn run_cli<S>() -> Result<(), Box<dyn Error>>
where
    S: SchemaAccess,
{
    VitrailCli::<S>::default().run().await
}

#[derive(Debug, Parser)]
#[command(name = "vitrail")]
#[command(about = "Vitrail CLI for schema-aware PostgreSQL migrations")]
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
    #[command(about = "Create a new migration from the current schema")]
    Dev(MigrateDevArgs),
    #[command(about = "Apply all migrations from disk to the database")]
    Deploy(MigrationConnectionArgs),
    #[command(about = "Show migration status")]
    Status(MigrationConnectionArgs),
}

impl MigrateCommand {
    async fn run<S>(self) -> Result<(), Box<dyn Error>>
    where
        S: SchemaAccess,
    {
        match self {
            Self::Dev(args) => args.run::<S>().await,
            Self::Deploy(args) => args.run_deploy().await,
            Self::Status(args) => args.run_status().await,
        }
    }
}

#[derive(Debug, Args)]
struct MigrateDevArgs {
    #[command(flatten)]
    connection: MigrationConnectionArgs,
    #[arg(long, short, help = "Human-readable name for the new migration")]
    name: String,
}

impl MigrateDevArgs {
    async fn run<S>(self) -> Result<(), Box<dyn Error>>
    where
        S: SchemaAccess,
    {
        let migrator = self.connection.migrator();

        match migrator.generate_migration::<S>(&self.name).await? {
            Some(generated) => {
                println!(
                    "Created migration `{}` at `{}`",
                    generated.migration().name(),
                    generated.migration().sql_path().display(),
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

#[derive(Debug, Args)]
struct MigrationConnectionArgs {
    #[arg(long, env = "VITRAIL_DATABASE_URL", help = "PostgreSQL database URL")]
    database_url: String,
    #[arg(
        long,
        default_value = "migrations",
        help = "Path to the migrations directory"
    )]
    migrations_path: PathBuf,
}

impl MigrationConnectionArgs {
    fn migrator(&self) -> PostgresMigrator {
        PostgresMigrator::new(self.database_url.clone(), self.migrations_path.clone())
    }

    async fn run_deploy(self) -> Result<(), Box<dyn Error>> {
        let migrator = self.migrator();
        let report = migrator.apply_all().await?;

        if report.applied().is_empty() {
            println!("No pending migrations.");
        } else {
            println!("Applied {} migration(s):", report.applied().len());
            for migration in report.applied() {
                println!("- {}", migration.name());
            }
        }

        if !report.skipped().is_empty() {
            println!(
                "Skipped {} already applied migration(s).",
                report.skipped().len()
            );
        }

        Ok(())
    }

    async fn run_status(self) -> Result<(), Box<dyn Error>> {
        let migrator = self.migrator();
        let applied = migrator.applied_migrations().await?;
        let disk = migrator.migration_directory().read_all()?;

        println!(
            "Migration directory: {}",
            migrator.migration_directory().path().display()
        );
        println!("Migrations on disk: {}", disk.len());
        println!("Migrations applied: {}", applied.len());

        if !disk.is_empty() {
            println!("On disk:");
            for migration in &disk {
                println!("- {}", migration.name());
            }
        }

        if !applied.is_empty() {
            println!("Applied:");
            for migration in &applied {
                println!("- {}", migration.name());
            }
        }

        Ok(())
    }
}
