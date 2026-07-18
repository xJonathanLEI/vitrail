use std::collections::BTreeSet;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use sqlx::Connection as _;
use sqlx::Row as _;
use sqlx::postgres::{PgConnection, PgPool, PgPoolOptions};
use vitrail_core::migrations::{
    AppliedMigration as SharedAppliedMigration,
    ApplyMigrationsReport as SharedApplyMigrationsReport,
    EmbeddedMigrations as SharedEmbeddedMigrations, GeneratedMigration as SharedGeneratedMigration,
    MIGRATION_SQL_FILE_NAME, Migration as SharedMigration,
    MigrationDirectory as SharedMigrationDirectory, MigrationSource as SharedMigrationSource,
    MigrationSourceError, new_applied_migration, new_apply_migrations_report,
    new_generated_migration,
};

use crate::schema::PostgresDialect;
use crate::{PostgresMigration, PostgresSchema, SchemaAccess};

pub type Migration = SharedMigration<PostgresDialect>;
pub type EmbeddedMigrations = SharedEmbeddedMigrations<PostgresDialect>;
pub type AppliedMigration = SharedAppliedMigration<PostgresDialect>;
pub type ApplyMigrationsReport = SharedApplyMigrationsReport<PostgresDialect>;
pub type GeneratedMigration = SharedGeneratedMigration<PostgresDialect>;
pub const POSTGRES_MIGRATION_HISTORY_TABLE_NAME: &str = "_vitrail_migrations";

#[derive(Debug)]
pub enum MigratorError {
    Io(io::Error),
    Sqlx(sqlx::Error),
    InvalidDatabaseUrl(String),
    InvalidMigrationName(String),
    MissingMigrationScript {
        directory: PathBuf,
    },
    MigrationGenerationRequiresDirectory,
    CleanupFailed {
        database_name: String,
        source: sqlx::Error,
    },
}

impl fmt::Display for MigratorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "{error}"),
            Self::Sqlx(error) => write!(f, "{error}"),
            Self::InvalidDatabaseUrl(url) => {
                write!(f, "invalid postgres database url `{url}`")
            }
            Self::InvalidMigrationName(name) => {
                write!(
                    f,
                    "migration name `{name}` does not contain any valid characters"
                )
            }
            Self::MissingMigrationScript { directory } => write!(
                f,
                "migration directory `{}` does not contain `{MIGRATION_SQL_FILE_NAME}`",
                directory.display()
            ),
            Self::MigrationGenerationRequiresDirectory => write!(
                f,
                "generating migrations requires a filesystem-backed migration directory"
            ),
            Self::CleanupFailed {
                database_name,
                source,
            } => write!(
                f,
                "failed to clean up temporary postgres database `{database_name}`: {source}"
            ),
        }
    }
}

impl std::error::Error for MigratorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Sqlx(error) => Some(error),
            Self::CleanupFailed { source, .. } => Some(source),
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
        <Self as SharedMigrationSource<PostgresDialect>>::read_all(self).map_err(Into::into)
    }
}

impl MigrationSource for Vec<Migration> {
    fn read_all(&self) -> Result<Vec<Migration>, MigratorError> {
        <Self as SharedMigrationSource<PostgresDialect>>::read_all(self).map_err(Into::into)
    }
}

#[derive(Clone)]
pub struct MigrationDirectory {
    inner: SharedMigrationDirectory<PostgresDialect>,
}

impl fmt::Debug for MigrationDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
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
pub struct PostgresMigrator {
    database_url: String,
    migrations: Arc<dyn MigrationSource>,
    migration_directory: Option<MigrationDirectory>,
}

impl fmt::Debug for PostgresMigrator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresMigrator")
            .field("database_url", &self.database_url)
            .field("migrations", &self.migrations)
            .field("migration_directory", &self.migration_directory)
            .finish()
    }
}

impl PostgresMigrator {
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
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.database_url)
            .await?;

        ensure_migration_table(&pool).await?;
        let applied = fetch_applied_migration_names(&pool).await?;
        pool.close().await;

        Ok(applied
            .into_iter()
            .map(new_applied_migration::<PostgresDialect>)
            .collect())
    }

    pub async fn apply_all(&self) -> Result<ApplyMigrationsReport, MigratorError> {
        let migrations = self.migrations.read_all()?;
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.database_url)
            .await?;

        ensure_migration_table(&pool).await?;
        let applied_names = fetch_applied_migration_names(&pool)
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

            execute_sql_script(&pool, migration.sql()).await?;
            record_applied_migration(&pool, migration.name()).await?;
            applied.push(migration);
        }

        pool.close().await;

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

        let target_schema = PostgresSchema::from_schema_access::<S>();
        let shadow_database = ShadowDatabase::create(&self.database_url).await?;

        let result = async {
            for migration in self.migrations.read_all()? {
                execute_sql_script_url(shadow_database.url(), migration.sql()).await?;
            }

            let current_schema =
                PostgresSchema::introspect_ignoring_external_tables::<S>(shadow_database.url())
                    .await?;
            let migration = target_schema.migrate_from(&current_schema);

            if migration.is_empty() {
                return Ok(None);
            }

            let sql = migration.to_sql();
            let disk_migration =
                migration_directory.create_migration(migration_name, sql.clone())?;

            Ok(Some(new_generated_migration(disk_migration, sql)))
        }
        .await;

        let cleanup_result = shadow_database.cleanup().await;

        match (result, cleanup_result) {
            (Ok(value), Ok(())) => Ok(value),
            (Ok(_), Err(error)) | (Err(error), _) => Err(error),
        }
    }

    pub async fn plan_migration<S>(&self) -> Result<PostgresMigration, MigratorError>
    where
        S: SchemaAccess,
    {
        let target_schema = PostgresSchema::from_schema_access::<S>();
        let shadow_database = ShadowDatabase::create(&self.database_url).await?;

        let result = async {
            for migration in self.migrations.read_all()? {
                execute_sql_script_url(shadow_database.url(), migration.sql()).await?;
            }

            let current_schema =
                PostgresSchema::introspect_ignoring_external_tables::<S>(shadow_database.url())
                    .await?;
            Ok(target_schema.migrate_from(&current_schema))
        }
        .await;

        let cleanup_result = shadow_database.cleanup().await;

        match (result, cleanup_result) {
            (Ok(value), Ok(())) => Ok(value),
            (Ok(_), Err(error)) | (Err(error), _) => Err(error),
        }
    }
}

async fn ensure_migration_table(pool: &PgPool) -> Result<(), MigratorError> {
    sqlx::query(&format!(
        r#"
CREATE TABLE IF NOT EXISTS "{POSTGRES_MIGRATION_HISTORY_TABLE_NAME}" (
    "name" TEXT PRIMARY KEY,
    "applied_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"#
    ))
    .execute(pool)
    .await?;

    Ok(())
}

async fn fetch_applied_migration_names(pool: &PgPool) -> Result<Vec<String>, MigratorError> {
    let rows = sqlx::query(&format!(
        r#"
SELECT "name"
FROM "{POSTGRES_MIGRATION_HISTORY_TABLE_NAME}"
ORDER BY "name"
"#
    ))
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect())
}

async fn record_applied_migration(
    pool: &PgPool,
    migration_name: &str,
) -> Result<(), MigratorError> {
    sqlx::query(&format!(
        r#"
INSERT INTO "{POSTGRES_MIGRATION_HISTORY_TABLE_NAME}" ("name")
VALUES ($1)
"#
    ))
    .bind(migration_name)
    .execute(pool)
    .await?;

    Ok(())
}

async fn execute_sql_script_url(database_url: &str, sql: &str) -> Result<(), MigratorError> {
    if sql.trim().is_empty() {
        return Ok(());
    }

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;

    execute_sql_script(&pool, sql).await?;
    pool.close().await;

    Ok(())
}

async fn execute_sql_script(pool: &PgPool, sql: &str) -> Result<(), MigratorError> {
    sqlx::raw_sql(sql).execute(pool).await?;
    Ok(())
}

#[derive(Debug)]
struct ShadowDatabase {
    admin_database_url: String,
    database_name: String,
    database_url: String,
}

impl ShadowDatabase {
    async fn create(database_url: &str) -> Result<Self, MigratorError> {
        let database_name = format!("vitrail_shadow_{}", unique_suffix());
        let admin_database_url = replace_database_name(database_url, "postgres")?;
        let database_url = replace_database_name(database_url, &database_name)?;

        let mut admin = PgConnection::connect(&admin_database_url).await?;
        sqlx::query(format!(r#"CREATE DATABASE "{}""#, database_name).as_str())
            .execute(&mut admin)
            .await?;

        Ok(Self {
            admin_database_url,
            database_name,
            database_url,
        })
    }

    fn url(&self) -> &str {
        &self.database_url
    }

    async fn cleanup(&self) -> Result<(), MigratorError> {
        let mut admin = PgConnection::connect(&self.admin_database_url).await?;

        sqlx::query(
            format!(
                r#"
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = '{database_name}'
  AND pid <> pg_backend_pid()
"#,
                database_name = self.database_name,
            )
            .as_str(),
        )
        .execute(&mut admin)
        .await
        .map_err(|source| MigratorError::CleanupFailed {
            database_name: self.database_name.clone(),
            source,
        })?;

        sqlx::query(format!(r#"DROP DATABASE "{}""#, self.database_name).as_str())
            .execute(&mut admin)
            .await
            .map_err(|source| MigratorError::CleanupFailed {
                database_name: self.database_name.clone(),
                source,
            })?;

        Ok(())
    }
}

fn replace_database_name(database_url: &str, database_name: &str) -> Result<String, MigratorError> {
    let mut url = url::Url::parse(database_url)
        .map_err(|_| MigratorError::InvalidDatabaseUrl(database_url.to_owned()))?;
    let has_database_name = url
        .path_segments()
        .ok_or_else(|| MigratorError::InvalidDatabaseUrl(database_url.to_owned()))?
        .any(|segment| !segment.is_empty());

    if !has_database_name {
        return Err(MigratorError::InvalidDatabaseUrl(database_url.to_owned()));
    }

    let mut segments = url
        .path_segments_mut()
        .map_err(|_| MigratorError::InvalidDatabaseUrl(database_url.to_owned()))?;

    segments.pop_if_empty();
    segments.pop().push(database_name);
    drop(segments);

    Ok(url.into())
}

fn unique_suffix() -> String {
    let unix_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    format!("{}_{}", std::process::id(), unix_nanos)
}
