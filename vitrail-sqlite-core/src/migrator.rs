use std::collections::BTreeSet;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection};
use sqlx::{Connection as _, Row as _};
use vitrail_core::migrations::{
    AppliedMigration as SharedAppliedMigration,
    ApplyMigrationsReport as SharedApplyMigrationsReport,
    EmbeddedMigrations as SharedEmbeddedMigrations, GeneratedMigration as SharedGeneratedMigration,
    MIGRATION_SQL_FILE_NAME, Migration as SharedMigration,
    MigrationDirectory as SharedMigrationDirectory, MigrationSource as SharedMigrationSource,
    MigrationSourceError, new_applied_migration, new_apply_migrations_report,
    new_generated_migration,
};

use crate::schema::SqliteDialect;
use crate::{SchemaAccess, SqliteMigration, SqliteSchema};

pub type Migration = SharedMigration<SqliteDialect>;
pub type EmbeddedMigrations = SharedEmbeddedMigrations<SqliteDialect>;
pub type AppliedMigration = SharedAppliedMigration<SqliteDialect>;
pub type ApplyMigrationsReport = SharedApplyMigrationsReport<SqliteDialect>;
pub type GeneratedMigration = SharedGeneratedMigration<SqliteDialect>;

pub const SQLITE_MIGRATION_HISTORY_TABLE_NAME: &str = "_vitrail_migrations";

#[derive(Debug)]
pub enum MigratorError {
    Io(io::Error),
    Sqlx(sqlx::Error),
    InvalidDatabaseUrl(String),
    InvalidMigrationName(String),
    MissingMigrationScript { directory: PathBuf },
    MigrationGenerationRequiresDirectory,
}

impl fmt::Display for MigratorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "{error}"),
            Self::Sqlx(error) => write!(formatter, "{error}"),
            Self::InvalidDatabaseUrl(url) => {
                write!(formatter, "invalid sqlite database url `{url}`")
            }
            Self::InvalidMigrationName(name) => write!(
                formatter,
                "migration name `{name}` does not contain any valid characters"
            ),
            Self::MissingMigrationScript { directory } => write!(
                formatter,
                "migration directory `{}` does not contain `{MIGRATION_SQL_FILE_NAME}`",
                directory.display()
            ),
            Self::MigrationGenerationRequiresDirectory => write!(
                formatter,
                "generating migrations requires a filesystem-backed migration directory"
            ),
        }
    }
}

impl std::error::Error for MigratorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Sqlx(error) => Some(error),
            Self::InvalidDatabaseUrl(_)
            | Self::InvalidMigrationName(_)
            | Self::MissingMigrationScript { .. }
            | Self::MigrationGenerationRequiresDirectory => None,
        }
    }
}

impl From<io::Error> for MigratorError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<MigrationSourceError> for MigratorError {
    fn from(value: MigrationSourceError) -> Self {
        match value {
            MigrationSourceError::Io(error) => Self::Io(error),
            MigrationSourceError::InvalidMigrationName(name) => Self::InvalidMigrationName(name),
            MigrationSourceError::MissingMigrationScript { directory } => {
                Self::MissingMigrationScript { directory }
            }
        }
    }
}

impl From<sqlx::Error> for MigratorError {
    fn from(value: sqlx::Error) -> Self {
        Self::Sqlx(value)
    }
}

pub trait MigrationSource: fmt::Debug + Send + Sync {
    fn read_all(&self) -> Result<Vec<Migration>, MigratorError>;
}

impl MigrationSource for EmbeddedMigrations {
    fn read_all(&self) -> Result<Vec<Migration>, MigratorError> {
        <Self as SharedMigrationSource<SqliteDialect>>::read_all(self).map_err(Into::into)
    }
}

impl MigrationSource for Vec<Migration> {
    fn read_all(&self) -> Result<Vec<Migration>, MigratorError> {
        <Self as SharedMigrationSource<SqliteDialect>>::read_all(self).map_err(Into::into)
    }
}

#[derive(Clone)]
pub struct MigrationDirectory {
    inner: SharedMigrationDirectory<SqliteDialect>,
}

impl fmt::Debug for MigrationDirectory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(formatter)
    }
}

impl MigrationDirectory {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            inner: SharedMigrationDirectory::new(path),
        }
    }

    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    pub fn ensure_exists(&self) -> Result<(), MigratorError> {
        self.inner.ensure_exists().map_err(Into::into)
    }

    pub fn read_all(&self) -> Result<Vec<Migration>, MigratorError> {
        self.inner.read_all().map_err(Into::into)
    }

    pub fn create_migration(
        &self,
        migration_name: &str,
        sql: impl Into<String>,
    ) -> Result<Migration, MigratorError> {
        self.inner
            .create_migration(migration_name, sql)
            .map_err(Into::into)
    }
}

impl MigrationSource for MigrationDirectory {
    fn read_all(&self) -> Result<Vec<Migration>, MigratorError> {
        MigrationDirectory::read_all(self)
    }
}

#[derive(Clone)]
pub struct SqliteMigrator {
    database_url: String,
    migrations: Arc<dyn MigrationSource>,
    migration_directory: Option<MigrationDirectory>,
}

impl fmt::Debug for SqliteMigrator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteMigrator")
            .field("database_url", &self.database_url)
            .field("migrations", &self.migrations)
            .field("migration_directory", &self.migration_directory)
            .finish()
    }
}

impl SqliteMigrator {
    pub fn new(database_url: impl Into<String>, migrations_path: impl Into<PathBuf>) -> Self {
        let migration_directory = MigrationDirectory::new(migrations_path);

        Self {
            database_url: database_url.into(),
            migrations: Arc::new(migration_directory.clone()),
            migration_directory: Some(migration_directory),
        }
    }

    pub fn from_source(
        database_url: impl Into<String>,
        migrations: impl MigrationSource + 'static,
    ) -> Self {
        Self {
            database_url: database_url.into(),
            migrations: Arc::new(migrations),
            migration_directory: None,
        }
    }

    pub fn embedded<I, N, S>(database_url: impl Into<String>, migrations: I) -> Self
    where
        I: IntoIterator<Item = (N, S)>,
        N: Into<String>,
        S: Into<String>,
    {
        Self::from_source(database_url, EmbeddedMigrations::new(migrations))
    }

    pub fn database_url(&self) -> &str {
        &self.database_url
    }

    pub fn migration_source(&self) -> &dyn MigrationSource {
        self.migrations.as_ref()
    }

    pub fn migration_directory(&self) -> Option<&MigrationDirectory> {
        self.migration_directory.as_ref()
    }

    pub async fn applied_migrations(&self) -> Result<Vec<AppliedMigration>, MigratorError> {
        let mut connection = connect_database(&self.database_url).await?;

        ensure_migration_table(&mut connection).await?;
        let applied = fetch_applied_migration_names(&mut connection).await?;

        Ok(applied
            .into_iter()
            .map(new_applied_migration::<SqliteDialect>)
            .collect())
    }

    pub async fn apply_all(&self) -> Result<ApplyMigrationsReport, MigratorError> {
        let migrations = self.migrations.read_all()?;
        let mut connection = connect_database(&self.database_url).await?;

        ensure_migration_table(&mut connection).await?;
        let applied_names = fetch_applied_migration_names(&mut connection)
            .await?
            .into_iter()
            .collect::<BTreeSet<_>>();

        let mut applied = Vec::new();
        let mut skipped = Vec::new();

        for migration in migrations {
            if applied_names.contains(migration.name()) {
                skipped.push(migration);
                continue;
            }

            execute_sql_script(&mut connection, migration.sql()).await?;
            record_applied_migration(&mut connection, migration.name()).await?;
            applied.push(migration);
        }

        Ok(new_apply_migrations_report(applied, skipped))
    }

    pub async fn generate_migration<S>(
        &self,
        migration_name: &str,
    ) -> Result<Option<GeneratedMigration>, MigratorError>
    where
        S: SchemaAccess,
    {
        let migration_directory = self
            .migration_directory
            .as_ref()
            .ok_or(MigratorError::MigrationGenerationRequiresDirectory)?;

        migration_directory.ensure_exists()?;

        let migration = self.plan_migration::<S>().await?;

        if migration.is_empty() {
            return Ok(None);
        }

        let sql = migration.to_sql();
        let disk_migration = migration_directory.create_migration(migration_name, sql.clone())?;

        Ok(Some(new_generated_migration(disk_migration, sql)))
    }

    pub async fn plan_migration<S>(&self) -> Result<SqliteMigration, MigratorError>
    where
        S: SchemaAccess,
    {
        let target_schema = SqliteSchema::from_schema_access::<S>();
        let migrations = self.migrations.read_all()?;
        let mut shadow_connection = connect_shadow_database().await?;

        for migration in migrations {
            execute_sql_script(&mut shadow_connection, migration.sql()).await?;
        }

        let current_schema = SqliteSchema::introspect_from_connection(
            &mut shadow_connection,
            S::schema().external_tables(),
        )
        .await?;

        Ok(target_schema.migrate_from(&current_schema))
    }
}

async fn connect_database(database_url: &str) -> Result<SqliteConnection, MigratorError> {
    let options = SqliteConnectOptions::from_str(database_url)
        .map_err(|_| MigratorError::InvalidDatabaseUrl(database_url.to_owned()))?
        .foreign_keys(true)
        .create_if_missing(true);

    SqliteConnection::connect_with(&options)
        .await
        .map_err(Into::into)
}

async fn connect_shadow_database() -> Result<SqliteConnection, MigratorError> {
    let options = SqliteConnectOptions::from_str("sqlite::memory:")
        .expect("the static SQLite in-memory URL should always be valid")
        .foreign_keys(true);

    SqliteConnection::connect_with(&options)
        .await
        .map_err(Into::into)
}

async fn ensure_migration_table(connection: &mut SqliteConnection) -> Result<(), MigratorError> {
    sqlx::query(&format!(
        r#"
CREATE TABLE IF NOT EXISTS "{SQLITE_MIGRATION_HISTORY_TABLE_NAME}" (
    "name" TEXT PRIMARY KEY,
    "applied_at" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"#
    ))
    .execute(&mut *connection)
    .await?;

    Ok(())
}

async fn fetch_applied_migration_names(
    connection: &mut SqliteConnection,
) -> Result<Vec<String>, MigratorError> {
    let rows = sqlx::query(&format!(
        r#"
SELECT "name"
FROM "{SQLITE_MIGRATION_HISTORY_TABLE_NAME}"
ORDER BY "name"
"#
    ))
    .fetch_all(&mut *connection)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect())
}

async fn record_applied_migration(
    connection: &mut SqliteConnection,
    migration_name: &str,
) -> Result<(), MigratorError> {
    sqlx::query(&format!(
        r#"
INSERT INTO "{SQLITE_MIGRATION_HISTORY_TABLE_NAME}" ("name")
VALUES (?1)
"#
    ))
    .bind(migration_name)
    .execute(&mut *connection)
    .await?;

    Ok(())
}

async fn execute_sql_script(
    connection: &mut SqliteConnection,
    sql: &str,
) -> Result<(), MigratorError> {
    if sql.trim().is_empty() {
        return Ok(());
    }

    sqlx::raw_sql(sql).execute(&mut *connection).await?;
    Ok(())
}
