use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::support::TestDatabase;
use sqlx::Row as _;
use sqlx::postgres::PgPoolOptions;
use vitrail_pg::{MigrationSource, PostgresMigrator, PostgresSchema, embed_migrations, schema};

schema! {
    name migrator_base_schema

    model user {
        id Int @id @default(autoincrement())
        email String @unique
        name String
    }
}

schema! {
    name migrator_expanded_schema

    model user {
        id Int @id @default(autoincrement())
        email String @unique
        name String
        created_at DateTime @default(now())
    }

    model post {
        id Int @id @default(autoincrement())
        author_id Int
        title String
        author user @relation(fields: [author_id], references: [id])
    }
}

#[test]
fn embed_migrations_macro_embeds_directory_migrations() {
    let migrations = embed_migrations!("tests/fixtures/embedded_migrations");
    let migrations = migrations
        .read_all()
        .expect("embedded migrations should be readable");

    assert_eq!(migrations.len(), 2);
    assert_eq!(migrations[0].name(), "20240101000000_first");
    assert_eq!(migrations[0].sql(), "SELECT 1;\n");
    assert_eq!(migrations[1].name(), "20240102000000_second");
    assert_eq!(migrations[1].sql(), "SELECT 2;\n");
}

#[test]
fn embed_migrations_macro_accepts_empty_directory() {
    let migrations = embed_migrations!("tests/fixtures/empty_embedded_migrations");
    let migrations = migrations
        .read_all()
        .expect("empty embedded migrations should be readable");

    assert!(migrations.is_empty());
}

#[tokio::test]
async fn apply_all_accepts_embedded_migrations_without_filesystem_source() {
    let database = TestDatabase::new().await;
    let migrator = PostgresMigrator::embedded(
        database.url(),
        [(
            "20240101000000_create_embedded_table",
            r#"
CREATE TABLE "embedded_user" (
    "id" SERIAL PRIMARY KEY,
    "email" TEXT NOT NULL
);
"#,
        )],
    );

    let report = migrator
        .apply_all()
        .await
        .expect("should apply embedded migrations");

    assert_eq!(report.applied().len(), 1);
    assert!(report.skipped().is_empty());
    assert_eq!(
        report.applied()[0].name(),
        "20240101000000_create_embedded_table"
    );

    let second_report = migrator
        .apply_all()
        .await
        .expect("re-running embedded migrations should be idempotent");

    assert!(second_report.applied().is_empty());
    assert_eq!(second_report.skipped().len(), 1);

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database.url())
        .await
        .expect("should connect to migrated database");

    let row = sqlx::query(
        r#"
SELECT EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name = 'embedded_user'
) AS "exists"
"#,
    )
    .fetch_one(&pool)
    .await
    .expect("should check whether embedded migration table exists");

    assert!(
        row.get::<bool, _>("exists"),
        "embedded migration SQL should create the expected table"
    );

    pool.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn generate_migration_writes_directory_for_pending_change() {
    let database = TestDatabase::new().await;
    let migrations_path = temporary_migrations_path("generate");
    let migrator = PostgresMigrator::new(database.url(), &migrations_path);

    let generated = migrator
        .generate_migration::<migrator_base_schema::Schema>("create user table")
        .await
        .expect("should generate migration")
        .expect("should create a migration for an empty database");

    assert!(
        generated.migration().name().ends_with("_create_user_table"),
        "generated migration directory should use a timestamp_name format, got `{}`",
        generated.migration().name()
    );
    assert!(
        generated
            .migration()
            .directory()
            .expect("generated migration should always have a filesystem directory")
            .is_dir(),
        "migration directory should exist on disk"
    );
    assert!(
        generated
            .migration()
            .sql_path()
            .expect("generated migration should always have a filesystem path")
            .is_file(),
        "migration.sql should exist on disk"
    );

    let expected_sql = PostgresSchema::from_schema_access::<migrator_base_schema::Schema>()
        .migrate_from(&PostgresSchema::empty())
        .to_sql();

    assert_eq!(normalize_sql(generated.sql()), normalize_sql(&expected_sql));

    let migration_sql = fs::read_to_string(
        generated
            .migration()
            .sql_path()
            .expect("generated migration should always have a filesystem path"),
    )
    .expect("should read generated migration from disk");
    assert_eq!(normalize_sql(&migration_sql), normalize_sql(&expected_sql));

    remove_migrations_dir(&migrations_path);
    database.cleanup().await;
}

#[tokio::test]
async fn apply_all_replays_generated_migrations_and_tracks_history() {
    let database = TestDatabase::new().await;
    let migrations_path = temporary_migrations_path("deploy");
    let migrator = PostgresMigrator::new(database.url(), &migrations_path);

    let first = migrator
        .generate_migration::<migrator_base_schema::Schema>("create user table")
        .await
        .expect("should generate initial migration")
        .expect("initial schema should produce a migration");

    let second = migrator
        .generate_migration::<migrator_expanded_schema::Schema>("expand with posts")
        .await
        .expect("should generate follow-up migration")
        .expect("expanded schema should produce a migration");

    let report = migrator
        .apply_all()
        .await
        .expect("should apply all migrations");

    assert_eq!(report.applied().len(), 2);
    assert!(report.skipped().is_empty());
    assert_eq!(report.applied()[0].name(), first.migration().name());
    assert_eq!(report.applied()[1].name(), second.migration().name());

    let current = PostgresSchema::introspect(database.url())
        .await
        .expect("should introspect migrated database");
    let target = PostgresSchema::from_schema_access::<migrator_expanded_schema::Schema>();
    let remaining = target.migrate_from(&current);

    assert!(
        remaining.is_empty(),
        "database should match the expanded schema after deploying migrations, got:\n{}",
        remaining.to_sql()
    );

    let applied_migrations = migrator
        .applied_migrations()
        .await
        .expect("should list applied migrations");

    assert_eq!(applied_migrations.len(), 2);
    assert_eq!(applied_migrations[0].name(), first.migration().name());
    assert_eq!(applied_migrations[1].name(), second.migration().name());

    let second_report = migrator
        .apply_all()
        .await
        .expect("re-running deployment should be idempotent");

    assert!(second_report.applied().is_empty());
    assert_eq!(second_report.skipped().len(), 2);

    remove_migrations_dir(&migrations_path);
    database.cleanup().await;
}

fn normalize_sql(sql: &str) -> String {
    sql.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn temporary_migrations_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!("vitrail_migrator_{label}_{}", unique_suffix()))
}

fn remove_migrations_dir(path: &PathBuf) {
    if path.exists() {
        fs::remove_dir_all(path).expect("should remove temporary migrations directory");
    }
}

fn unique_suffix() -> String {
    let unix_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    format!("{}_{}", std::process::id(), unix_nanos)
}
