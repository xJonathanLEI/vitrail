use std::fmt;
use std::io;
use std::path::PathBuf;

use vitrail_core::migrations::{
    GeneratedMigration as SharedGeneratedMigration, MIGRATION_SQL_FILE_NAME,
    Migration as SharedMigration, MigrationDirectory as SharedMigrationDirectory,
    MigrationSourceError, new_generated_migration,
};
use vitrail_sqlite_dialect::{
    SchemaAccess, SqliteDialect, SqliteMigration, SqliteSchema, ValidationErrors,
    validate_d1_schema,
};

/// A migration stored in Vitrail's nested migration-directory format.
pub type Migration = SharedMigration<SqliteDialect>;

/// A newly generated D1 migration and its rendered SQL.
pub type GeneratedMigration = SharedGeneratedMigration<SqliteDialect>;

type MigrationDirectory = SharedMigrationDirectory<SqliteDialect>;

/// A planned Cloudflare D1 schema migration.
///
/// The plan shares SQLite-family diff semantics with native SQLite but renders
/// SQL using D1-compatible migration pragmas.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct D1Migration {
    inner: SqliteMigration,
}

impl D1Migration {
    fn new(inner: SqliteMigration) -> Self {
        Self { inner }
    }

    /// Returns `true` when the target schema already matches the migration
    /// history represented by the generator's migration directory.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Renders this plan as a D1-compatible migration script.
    pub fn to_sql(&self) -> String {
        self.inner.to_d1_sql()
    }
}

/// Error produced while planning or generating a D1 migration.
#[derive(Debug)]
#[non_exhaustive]
pub enum D1MigrationError {
    /// A filesystem operation failed.
    Io(io::Error),
    /// The requested migration name contains no usable characters.
    InvalidMigrationName(String),
    /// A nested migration directory does not contain `migration.sql`.
    MissingMigrationScript {
        /// The invalid migration directory.
        directory: PathBuf,
    },
    /// The target schema exceeds a Cloudflare D1 platform limit.
    PlatformLimit(ValidationErrors),
    /// Existing migration scripts could not be applied to the local shadow
    /// SQLite database.
    Shadow(vitrail_sqlite_core::sqlx::Error),
}

impl fmt::Display for D1MigrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "{error}"),
            Self::InvalidMigrationName(name) => write!(
                formatter,
                "migration name `{name}` does not contain any valid characters"
            ),
            Self::MissingMigrationScript { directory } => write!(
                formatter,
                "migration directory `{}` does not contain `{MIGRATION_SQL_FILE_NAME}`",
                directory.display()
            ),
            Self::PlatformLimit(error) => {
                write!(formatter, "schema exceeds Cloudflare D1 limits: {error}")
            }
            Self::Shadow(error) => {
                write!(
                    formatter,
                    "failed to build the D1 migration shadow database: {error}"
                )
            }
        }
    }
}

impl std::error::Error for D1MigrationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::PlatformLimit(error) => Some(error),
            Self::Shadow(error) => Some(error),
            Self::InvalidMigrationName(_) | Self::MissingMigrationScript { .. } => None,
        }
    }
}

impl From<MigrationSourceError> for D1MigrationError {
    fn from(error: MigrationSourceError) -> Self {
        match error {
            MigrationSourceError::Io(error) => Self::Io(error),
            MigrationSourceError::InvalidMigrationName(name) => Self::InvalidMigrationName(name),
            MigrationSourceError::MissingMigrationScript { directory } => {
                Self::MissingMigrationScript { directory }
            }
        }
    }
}

impl From<ValidationErrors> for D1MigrationError {
    fn from(error: ValidationErrors) -> Self {
        Self::PlatformLimit(error)
    }
}

impl From<vitrail_sqlite_core::sqlx::Error> for D1MigrationError {
    fn from(error: vitrail_sqlite_core::sqlx::Error) -> Self {
        Self::Shadow(error)
    }
}

/// Generates Cloudflare D1 migration files using a local in-memory shadow
/// SQLite database.
///
/// This generator never connects to Cloudflare. Existing migration files are
/// applied locally, one atomic transaction per file, before the resulting
/// schema is diffed against the generated Vitrail schema. Migration application
/// and migration history remain the responsibility of Wrangler.
#[derive(Clone, Debug)]
pub struct D1MigrationGenerator {
    migration_directory: MigrationDirectory,
}

impl D1MigrationGenerator {
    /// Creates a generator backed by a nested migration directory.
    pub fn new(migrations_path: impl Into<PathBuf>) -> Self {
        Self {
            migration_directory: MigrationDirectory::new(migrations_path),
        }
    }

    /// Plans the migration required to bring the existing migration history to
    /// schema `S`.
    pub async fn plan_migration<S>(&self) -> Result<D1Migration, D1MigrationError>
    where
        S: SchemaAccess,
    {
        validate_d1_schema(S::schema())?;
        self.migration_directory.ensure_exists()?;

        let migrations = self.migration_directory.read_all()?;
        let current_schema = vitrail_sqlite_core::introspect_atomic_shadow_schema(
            migrations.iter().map(|migration| migration.sql()),
            S::schema().external_tables(),
        )
        .await?;
        let target_schema = SqliteSchema::from_schema(S::schema());

        Ok(D1Migration::new(
            target_schema.migrate_from(&current_schema),
        ))
    }

    /// Generates a nested D1 migration directory when schema `S` differs from
    /// the existing migration history.
    ///
    /// Returns `None` when no migration is required.
    pub async fn generate_migration<S>(
        &self,
        migration_name: &str,
    ) -> Result<Option<GeneratedMigration>, D1MigrationError>
    where
        S: SchemaAccess,
    {
        let migration = self.plan_migration::<S>().await?;

        if migration.is_empty() {
            return Ok(None);
        }

        let sql = migration.to_sql();
        let disk_migration = self
            .migration_directory
            .create_migration(migration_name, sql.clone())?;

        Ok(Some(new_generated_migration(disk_migration, sql)))
    }
}
