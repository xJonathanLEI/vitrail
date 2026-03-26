use std::collections::BTreeSet;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::Utc;
use sqlx::Connection as _;
use sqlx::Row as _;
use sqlx::postgres::{PgConnection, PgPool, PgPoolOptions};

use crate::{PostgresMigration, PostgresSchema, SchemaAccess};

const MIGRATION_SQL_FILE_NAME: &str = "migration.sql";
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
            | Self::MissingMigrationScript { .. } => None,
        }
    }
}

impl From<io::Error> for MigratorError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<sqlx::Error> for MigratorError {
    fn from(value: sqlx::Error) -> Self {
        Self::Sqlx(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiskMigration {
    name: String,
    directory: PathBuf,
    sql_path: PathBuf,
    sql: String,
}

impl DiskMigration {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    pub fn sql_path(&self) -> &Path {
        &self.sql_path
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppliedMigration {
    name: String,
}

impl AppliedMigration {
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplyMigrationsReport {
    applied: Vec<DiskMigration>,
    skipped: Vec<DiskMigration>,
}

impl ApplyMigrationsReport {
    pub fn applied(&self) -> &[DiskMigration] {
        &self.applied
    }

    pub fn skipped(&self) -> &[DiskMigration] {
        &self.skipped
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GeneratedMigration {
    migration: DiskMigration,
    sql: String,
}

impl GeneratedMigration {
    pub fn migration(&self) -> &DiskMigration {
        &self.migration
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }
}

#[derive(Clone, Debug)]
pub struct MigrationDirectory {
    root: PathBuf,
}

impl MigrationDirectory {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { root: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.root
    }

    pub fn ensure_exists(&self) -> Result<(), MigratorError> {
        fs::create_dir_all(&self.root)?;
        Ok(())
    }

    pub fn read_all(&self) -> Result<Vec<DiskMigration>, MigratorError> {
        self.ensure_exists()?;

        let mut entries = fs::read_dir(&self.root)?
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|entry| entry.file_type().is_ok_and(|file_type| file_type.is_dir()))
            .collect::<Vec<_>>();

        entries.sort_by_key(|entry| entry.file_name());

        let mut migrations = Vec::with_capacity(entries.len());

        for entry in entries {
            let directory = entry.path();
            let sql_path = directory.join(MIGRATION_SQL_FILE_NAME);

            if !sql_path.is_file() {
                return Err(MigratorError::MissingMigrationScript { directory });
            }

            let sql = fs::read_to_string(&sql_path)?;
            let name = entry.file_name().to_string_lossy().into_owned();

            migrations.push(DiskMigration {
                name,
                directory: entry.path(),
                sql_path,
                sql,
            });
        }

        Ok(migrations)
    }

    pub fn create_migration(
        &self,
        migration_name: &str,
        sql: impl Into<String>,
    ) -> Result<DiskMigration, MigratorError> {
        self.ensure_exists()?;

        let slug = slugify_migration_name(migration_name)?;
        let timestamp = Utc::now().format("%Y%m%d%H%M%S").to_string();

        let directory = self.root.join(format!("{timestamp}_{slug}"));
        fs::create_dir_all(&directory)?;

        let sql_path = directory.join(MIGRATION_SQL_FILE_NAME);
        let sql = sql.into();
        fs::write(&sql_path, &sql)?;

        Ok(DiskMigration {
            name: directory
                .file_name()
                .expect("generated migration directory should always have a file name")
                .to_string_lossy()
                .into_owned(),
            directory,
            sql_path,
            sql,
        })
    }
}

#[derive(Clone, Debug)]
pub struct PostgresMigrator {
    database_url: String,
    migrations: MigrationDirectory,
}

impl PostgresMigrator {
    pub fn new(database_url: impl Into<String>, migrations_path: impl Into<PathBuf>) -> Self {
        Self {
            database_url: database_url.into(),
            migrations: MigrationDirectory::new(migrations_path),
        }
    }

    pub fn database_url(&self) -> &str {
        &self.database_url
    }

    pub fn migration_directory(&self) -> &MigrationDirectory {
        &self.migrations
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
            .map(|name| AppliedMigration { name })
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

        Ok(ApplyMigrationsReport { applied, skipped })
    }

    pub async fn generate_migration<S>(
        &self,
        migration_name: &str,
    ) -> Result<Option<GeneratedMigration>, MigratorError>
    where
        S: SchemaAccess,
    {
        self.migrations.ensure_exists()?;

        let target_schema = PostgresSchema::from_schema_access::<S>();
        let shadow_database = ShadowDatabase::create(&self.database_url).await?;

        let result = async {
            for migration in self.migrations.read_all()? {
                execute_sql_script_url(shadow_database.url(), migration.sql()).await?;
            }

            let current_schema = PostgresSchema::introspect(shadow_database.url()).await?;
            let migration = target_schema.migrate_from(&current_schema);

            if migration.is_empty() {
                return Ok(None);
            }

            let sql = migration.to_sql();
            let disk_migration = self
                .migrations
                .create_migration(migration_name, sql.clone())?;

            Ok(Some(GeneratedMigration {
                migration: disk_migration,
                sql,
            }))
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

            let current_schema = PostgresSchema::introspect(shadow_database.url()).await?;
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

fn slugify_migration_name(name: &str) -> Result<String, MigratorError> {
    let mut slug = String::new();
    let mut previous_was_separator = false;

    for character in name.chars() {
        if character.is_ascii_alphanumeric() {
            slug.push(character.to_ascii_lowercase());
            previous_was_separator = false;
        } else if !previous_was_separator && !slug.is_empty() {
            slug.push('_');
            previous_was_separator = true;
        }
    }

    while slug.ends_with('_') {
        slug.pop();
    }

    if slug.is_empty() {
        return Err(MigratorError::InvalidMigrationName(name.to_owned()));
    }

    Ok(slug)
}
