use std::ffi::OsString;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use sqlx::Connection as _;
use sqlx::sqlite::SqliteConnection;
use vitrail_sqlite::SqliteSchema;

pub(super) struct TestDatabase {
    database_path: PathBuf,
    database_url: String,
}

impl TestDatabase {
    pub(super) fn new() -> Self {
        let database_path = temporary_database_path();

        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&database_path)
            .unwrap_or_else(|error| {
                panic!(
                    "should create temporary SQLite database at {}: {error}",
                    database_path.display()
                )
            });

        let database_url = sqlite_url(&database_path);

        Self {
            database_path,
            database_url,
        }
    }

    pub(super) fn url(&self) -> &str {
        &self.database_url
    }

    pub(super) fn cleanup(&self) {
        remove_database_files(&self.database_path, true);
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        remove_database_files(&self.database_path, false);
    }
}

pub(super) async fn apply_sql_script(database_url: &str, sql: &str) {
    if sql.trim().is_empty() {
        return;
    }

    let mut connection = SqliteConnection::connect(database_url)
        .await
        .expect("should connect to SQLite");

    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&mut connection)
        .await
        .expect("should enable SQLite foreign keys");

    sqlx::raw_sql(sql)
        .execute(&mut connection)
        .await
        .unwrap_or_else(|error| panic!("should execute SQLite migration script: {error}\n{sql}"));
}

pub(super) async fn apply_schema(database_url: &str, schema: &SqliteSchema) {
    let sql = schema.migrate_from(&SqliteSchema::empty()).to_sql();
    apply_sql_script(database_url, &sql).await;
}

fn temporary_database_path() -> PathBuf {
    static UNIQUE_COUNTER: AtomicU64 = AtomicU64::new(0);

    let unix_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after the Unix epoch")
        .as_nanos();
    let counter = UNIQUE_COUNTER.fetch_add(1, Ordering::Relaxed);

    std::env::temp_dir().join(format!(
        "vitrail_sqlite_{}_{}_{}.db",
        std::process::id(),
        unix_nanos,
        counter
    ))
}

fn sqlite_url(database_path: &Path) -> String {
    format!("sqlite://{}", database_path.display())
}

fn remove_database_files(database_path: &Path, strict: bool) {
    for path in [
        database_path.to_path_buf(),
        path_with_suffix(database_path, "-wal"),
        path_with_suffix(database_path, "-shm"),
        path_with_suffix(database_path, "-journal"),
    ] {
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) if strict => {
                panic!(
                    "should remove temporary SQLite database file {}: {error}",
                    path.display()
                );
            }
            Err(_) => {}
        }
    }
}

fn path_with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = OsString::from(path.as_os_str());
    value.push(suffix);
    PathBuf::from(value)
}
