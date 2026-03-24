use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

use sqlx::Connection as _;
use sqlx::postgres::{PgConnection, PgPoolOptions};
use vitrail_pg::PostgresSchema;

const DEFAULT_POSTGRES_URL: &str = "postgres://postgres:postgres@127.0.0.1:5432/vitrail";

fn postgres_test_setup_help(database_url: &str) -> String {
    format!(
        "Postgres integration tests require an existing server. \
        Expected base URL: {default_url}\n\
        Resolved base URL: {database_url}\n\
        You can override it with VITRAIL_POSTGRES_URL.\n\
        Example:\n  \
          docker run --rm -e POSTGRES_USER=postgres \
            -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=vitrail \
            -p 127.0.0.1:5432:5432 postgres:16-alpine",
        default_url = DEFAULT_POSTGRES_URL,
    )
}

pub(super) struct TestDatabase {
    admin_database_url: String,
    database_name: String,
    database_url: String,
}

impl TestDatabase {
    pub(super) async fn new() -> Self {
        let base_url =
            env::var("VITRAIL_POSTGRES_URL").unwrap_or_else(|_| DEFAULT_POSTGRES_URL.to_owned());
        let database_name = format!("vitrail_{}", unique_suffix());
        let admin_database_url = replace_database_name(&base_url, "postgres");

        let mut admin = PgConnection::connect(&admin_database_url)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "should connect to postgres admin database at {admin_database_url}: {error}\n{}",
                    postgres_test_setup_help(&base_url)
                )
            });

        sqlx::query(format!(r#"CREATE DATABASE "{}""#, database_name).as_str())
            .execute(&mut admin)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "should create temporary postgres test database {database_name}: {error}\n{}",
                    postgres_test_setup_help(&base_url)
                )
            });

        Self {
            admin_database_url,
            database_url: replace_database_name(&base_url, &database_name),
            database_name,
        }
    }

    pub(super) fn url(&self) -> &str {
        &self.database_url
    }

    pub(super) async fn cleanup(&self) {
        let mut admin = PgConnection::connect(&self.admin_database_url)
            .await
            .expect("should connect to postgres admin database for cleanup");

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
        .expect("should terminate temporary postgres test database connections");

        sqlx::query(format!(r#"DROP DATABASE "{}""#, self.database_name).as_str())
            .execute(&mut admin)
            .await
            .expect("should drop temporary postgres test database");
    }
}

fn replace_database_name(database_url: &str, database_name: &str) -> String {
    let (base, query) = match database_url.split_once('?') {
        Some((base, query)) => (base, Some(query)),
        None => (database_url, None),
    };

    let slash_index = base
        .rfind('/')
        .expect("postgres url should include a database name");

    let mut updated = format!("{}/{}", &base[..slash_index], database_name);

    if let Some(query) = query {
        updated.push('?');
        updated.push_str(query);
    }

    updated
}

fn unique_suffix() -> String {
    let unix_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    format!("{}_{}", std::process::id(), unix_nanos)
}

pub(super) async fn apply_sql_script(database_url: &str, sql: &str) {
    if sql.is_empty() {
        return;
    }

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
        .expect("should connect to postgres");

    let mut statement = String::new();

    for line in sql.lines() {
        if line.trim_start().starts_with("--") {
            continue;
        }

        if line.trim().is_empty() && statement.is_empty() {
            continue;
        }

        if !statement.is_empty() {
            statement.push('\n');
        }
        statement.push_str(line);

        if line.trim_end().ends_with(';') {
            sqlx::query(&statement)
                .execute(&pool)
                .await
                .unwrap_or_else(|error| {
                    panic!("should execute migration statement `{statement}`: {error}")
                });
            statement.clear();
        }
    }

    assert!(
        statement.trim().is_empty(),
        "migration script should not end with a partial statement: {statement}"
    );

    pool.close().await;
}

pub(super) async fn apply_schema(database_url: &str, schema: &PostgresSchema) {
    let sql = schema.migrate_from(&PostgresSchema::empty()).to_sql();
    apply_sql_script(database_url, &sql).await;
}
