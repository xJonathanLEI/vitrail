use std::fmt;
use std::fs;
use std::io;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use chrono::Utc;

use crate::schema::Dialect;

pub const MIGRATION_SQL_FILE_NAME: &str = "migration.sql";

#[derive(Debug)]
pub enum MigrationSourceError {
    Io(io::Error),
    InvalidMigrationName(String),
    MissingMigrationScript { directory: PathBuf },
}

impl fmt::Display for MigrationSourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "{error}"),
            Self::InvalidMigrationName(name) => write!(
                f,
                "migration name `{name}` does not contain any valid characters"
            ),
            Self::MissingMigrationScript { directory } => write!(
                f,
                "migration directory `{}` does not contain `{MIGRATION_SQL_FILE_NAME}`",
                directory.display()
            ),
        }
    }
}

impl std::error::Error for MigrationSourceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::InvalidMigrationName(_) | Self::MissingMigrationScript { .. } => None,
        }
    }
}

impl From<io::Error> for MigrationSourceError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct Migration<D: Dialect> {
    name: String,
    directory: Option<PathBuf>,
    sql_path: Option<PathBuf>,
    sql: String,
    dialect: PhantomData<fn() -> D>,
}

impl<D: Dialect> fmt::Debug for Migration<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Migration")
            .field("name", &self.name)
            .field("directory", &self.directory)
            .field("sql_path", &self.sql_path)
            .field("sql", &self.sql)
            .finish()
    }
}

impl<D: Dialect> Migration<D> {
    pub fn new(name: impl Into<String>, sql: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            directory: None,
            sql_path: None,
            sql: sql.into(),
            dialect: PhantomData,
        }
    }

    fn from_directory(
        name: impl Into<String>,
        directory: impl Into<PathBuf>,
        sql_path: impl Into<PathBuf>,
        sql: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            directory: Some(directory.into()),
            sql_path: Some(sql_path.into()),
            sql: sql.into(),
            dialect: PhantomData,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn directory(&self) -> Option<&Path> {
        self.directory.as_deref()
    }

    pub fn sql_path(&self) -> Option<&Path> {
        self.sql_path.as_deref()
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }
}

pub trait MigrationSource<D: Dialect>: fmt::Debug + Send + Sync {
    fn read_all(&self) -> Result<Vec<Migration<D>>, MigrationSourceError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EmbeddedMigrations<D: Dialect> {
    migrations: Vec<Migration<D>>,
}

impl<D: Dialect> EmbeddedMigrations<D> {
    pub fn new<I, N, S>(migrations: I) -> Self
    where
        I: IntoIterator<Item = (N, S)>,
        N: Into<String>,
        S: Into<String>,
    {
        let mut migrations = migrations
            .into_iter()
            .map(|(name, sql)| Migration::new(name, sql))
            .collect::<Vec<_>>();

        migrations.sort_by(|left, right| left.name().cmp(right.name()));

        Self { migrations }
    }
}

impl<D: Dialect> MigrationSource<D> for EmbeddedMigrations<D> {
    fn read_all(&self) -> Result<Vec<Migration<D>>, MigrationSourceError> {
        Ok(self.migrations.clone())
    }
}

impl<D: Dialect> MigrationSource<D> for Vec<Migration<D>> {
    fn read_all(&self) -> Result<Vec<Migration<D>>, MigrationSourceError> {
        let mut migrations = self.clone();
        migrations.sort_by(|left, right| left.name().cmp(right.name()));
        Ok(migrations)
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct AppliedMigration<D: Dialect> {
    name: String,
    dialect: PhantomData<fn() -> D>,
}

impl<D: Dialect> fmt::Debug for AppliedMigration<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppliedMigration")
            .field("name", &self.name)
            .finish()
    }
}

// These are free functions instead of inherent constructors so dialect facades can
// expose type aliases without adding constructors to their public API.
#[doc(hidden)]
pub fn new_applied_migration<D: Dialect>(name: impl Into<String>) -> AppliedMigration<D> {
    AppliedMigration {
        name: name.into(),
        dialect: PhantomData,
    }
}

impl<D: Dialect> AppliedMigration<D> {
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplyMigrationsReport<D: Dialect> {
    applied: Vec<Migration<D>>,
    skipped: Vec<Migration<D>>,
}

#[doc(hidden)]
pub fn new_apply_migrations_report<D: Dialect>(
    applied: Vec<Migration<D>>,
    skipped: Vec<Migration<D>>,
) -> ApplyMigrationsReport<D> {
    ApplyMigrationsReport { applied, skipped }
}

impl<D: Dialect> ApplyMigrationsReport<D> {
    pub fn applied(&self) -> &[Migration<D>] {
        &self.applied
    }

    pub fn skipped(&self) -> &[Migration<D>] {
        &self.skipped
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GeneratedMigration<D: Dialect> {
    migration: Migration<D>,
    sql: String,
}

#[doc(hidden)]
pub fn new_generated_migration<D: Dialect>(
    migration: Migration<D>,
    sql: impl Into<String>,
) -> GeneratedMigration<D> {
    GeneratedMigration {
        migration,
        sql: sql.into(),
    }
}

impl<D: Dialect> GeneratedMigration<D> {
    pub fn migration(&self) -> &Migration<D> {
        &self.migration
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct MigrationDirectory<D: Dialect> {
    root: PathBuf,
    dialect: PhantomData<fn() -> D>,
}

impl<D: Dialect> fmt::Debug for MigrationDirectory<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MigrationDirectory")
            .field("root", &self.root)
            .finish()
    }
}

impl<D: Dialect> MigrationDirectory<D> {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            root: path.into(),
            dialect: PhantomData,
        }
    }

    pub fn path(&self) -> &Path {
        &self.root
    }

    pub fn ensure_exists(&self) -> Result<(), MigrationSourceError> {
        fs::create_dir_all(&self.root)?;
        Ok(())
    }

    pub fn read_all(&self) -> Result<Vec<Migration<D>>, MigrationSourceError> {
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
                return Err(MigrationSourceError::MissingMigrationScript { directory });
            }

            let sql = fs::read_to_string(&sql_path)?;
            let name = entry.file_name().to_string_lossy().into_owned();

            migrations.push(Migration::from_directory(name, entry.path(), sql_path, sql));
        }

        Ok(migrations)
    }

    pub fn create_migration(
        &self,
        migration_name: &str,
        sql: impl Into<String>,
    ) -> Result<Migration<D>, MigrationSourceError> {
        self.ensure_exists()?;

        let slug = slugify_migration_name(migration_name)?;
        let timestamp = Utc::now().format("%Y%m%d%H%M%S").to_string();

        let directory = self.root.join(format!("{timestamp}_{slug}"));
        fs::create_dir_all(&directory)?;

        let sql_path = directory.join(MIGRATION_SQL_FILE_NAME);
        let sql = sql.into();
        fs::write(&sql_path, &sql)?;

        Ok(Migration::from_directory(
            directory
                .file_name()
                .expect("generated migration directory should always have a file name")
                .to_string_lossy()
                .into_owned(),
            directory,
            sql_path,
            sql,
        ))
    }
}

impl<D: Dialect> MigrationSource<D> for MigrationDirectory<D> {
    fn read_all(&self) -> Result<Vec<Migration<D>>, MigrationSourceError> {
        MigrationDirectory::read_all(self)
    }
}

fn slugify_migration_name(name: &str) -> Result<String, MigrationSourceError> {
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
        return Err(MigrationSourceError::InvalidMigrationName(name.to_owned()));
    }

    Ok(slug)
}
