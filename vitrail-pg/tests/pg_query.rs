use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

use sqlx::Connection as _;
use sqlx::postgres::{PgConnection, PgPoolOptions};
use vitrail_pg::{query, schema};

const DEFAULT_POSTGRES_URL: &str = "postgres://postgres:postgres@127.0.0.1:5432/vitrail";

schema! {
    name my_schema

    model user {
        id         Int      @id @default(autoincrement())
        email      String   @unique
        name       String
        created_at DateTime @default(now())
    }

    model post {
        id         Int      @id @default(autoincrement())
        title      String
        body       String?
        published  Boolean
        author_id  Int
        created_at DateTime @default(now())
        author     user     @relation(fields: [author_id], references: [id])
    }
}

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

struct TestDatabase {
    admin_database_url: String,
    database_name: String,
    url: String,
}

impl TestDatabase {
    async fn new() -> Self {
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
            database_name: database_name.clone(),
            url: replace_database_name(&base_url, &database_name),
        }
    }

    fn url(&self) -> &str {
        &self.url
    }

    async fn cleanup(&self) {
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
                database_name = self.database_name
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

async fn setup_database(database_url: &str) -> i64 {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
        .expect("should connect to postgres");

    sqlx::query(
        r#"
        CREATE TABLE "user" (
            "id" BIGSERIAL PRIMARY KEY,
            "email" TEXT NOT NULL UNIQUE,
            "name" TEXT NOT NULL,
            "created_at" TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        "#,
    )
    .execute(&pool)
    .await
    .expect("should create user table");

    sqlx::query(
        r#"
        CREATE TABLE "post" (
            "id" BIGSERIAL PRIMARY KEY,
            "title" TEXT NOT NULL,
            "body" TEXT NULL,
            "published" BOOLEAN NOT NULL,
            "author_id" BIGINT NOT NULL REFERENCES "user" ("id"),
            "created_at" TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        "#,
    )
    .execute(&pool)
    .await
    .expect("should create post table");

    let author_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO "user" ("email", "name")
        VALUES ('alice@example.com', 'Alice')
        RETURNING "id"::bigint
        "#,
    )
    .fetch_one(&pool)
    .await
    .expect("should insert author");

    sqlx::query(
        r#"
        INSERT INTO "post" ("title", "body", "published", "author_id")
        VALUES ($1, $2, $3, $4)
        "#,
    )
    .bind("Hello from Vitrail")
    .bind(Some("This is the post body"))
    .bind(true)
    .bind(author_id)
    .execute(&pool)
    .await
    .expect("should insert post");

    pool.close().await;
    author_id
}

#[tokio::test]
async fn simple_query_on_postgres() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();
    let author_id = setup_database(&database_url).await;

    let client = my_schema::VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let posts = client
        .find_many(query! {
            crate::my_schema,
            post {
                select: {
                    id: true,
                    title: true,
                },
                include: {
                    author: true,
                },
            }
        })
        .await
        .expect("query should succeed");

    assert_eq!(posts.len(), 1);

    let post = &posts[0];
    let _: &i64 = &post.id;
    let _: &String = &post.title;
    let _: &i64 = &post.author.id;
    let _: &String = &post.author.email;
    let _: &String = &post.author.name;
    let _: &chrono::DateTime<chrono::Utc> = &post.author.created_at;

    assert_eq!(post.id, 1);
    assert_eq!(post.title, "Hello from Vitrail");
    assert_eq!(post.author.id, author_id);
    assert_eq!(post.author.email, "alice@example.com");
    assert_eq!(post.author.name, "Alice");
    assert!(post.author.created_at <= chrono::Utc::now());

    database.cleanup().await;
}
