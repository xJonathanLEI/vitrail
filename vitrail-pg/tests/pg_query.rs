use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

use sqlx::Connection as _;
use sqlx::postgres::{PgConnection, PgPoolOptions};
use vitrail_pg::{QueryResult, query, schema};

const DEFAULT_POSTGRES_URL: &str = "postgres://postgres:postgres@127.0.0.1:5432/vitrail";

schema! {
    name my_schema

    model user {
        id         Int      @id @default(autoincrement())
        email      String   @unique
        name       String
        created_at DateTime @default(now())
        posts      post[]
    }

    model post {
        id         Int      @id @default(autoincrement())
        title      String
        body       String?
        published  Boolean
        author_id  Int
        created_at DateTime @default(now())
        author     user     @relation(fields: [author_id], references: [id])
        comments   comment[]
    }

    model comment {
        id      Int    @id @default(autoincrement())
        body    String
        post_id Int
        post    post   @relation(fields: [post_id], references: [id])
    }
}

#[derive(QueryResult)]
#[vitrail(schema = crate::my_schema::Schema, model = user)]
struct UserSummary {
    id: i64,
    email: String,
    name: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(QueryResult)]
#[vitrail(schema = crate::my_schema::Schema, model = post)]
struct PostSummary {
    id: i64,
    title: String,
}

#[derive(QueryResult)]
#[vitrail(schema = crate::my_schema::Schema, model = comment)]
struct CommentSummary {
    id: i64,
    body: String,
}

#[derive(QueryResult)]
#[vitrail(schema = crate::my_schema::Schema, model = post)]
struct PostWithAuthor {
    id: i64,
    title: String,
    #[vitrail(include)]
    author: UserSummary,
}

#[derive(QueryResult)]
#[vitrail(schema = crate::my_schema::Schema, model = post)]
struct PostWithComments {
    id: i64,
    title: String,
    #[vitrail(include)]
    comments: Vec<CommentSummary>,
}

#[derive(QueryResult)]
#[vitrail(schema = crate::my_schema::Schema, model = user)]
struct UserWithPosts {
    id: i64,
    email: String,
    name: String,
    created_at: chrono::DateTime<chrono::Utc>,
    #[vitrail(include)]
    posts: Vec<PostSummary>,
}

#[derive(QueryResult)]
#[vitrail(schema = crate::my_schema::Schema, model = user)]
struct UserWithPostsAndComments {
    id: i64,
    email: String,
    #[vitrail(include)]
    posts: Vec<PostWithComments>,
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

    sqlx::query(
        r#"
        CREATE TABLE "comment" (
            "id" BIGSERIAL PRIMARY KEY,
            "body" TEXT NOT NULL,
            "post_id" BIGINT NOT NULL REFERENCES "post" ("id")
        );
        "#,
    )
    .execute(&pool)
    .await
    .expect("should create comment table");

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

    let mut post_ids = Vec::new();

    for (title, body, published) in [
        ("Hello from Vitrail", Some("This is the post body"), true),
        ("Second post", None, false),
    ] {
        let post_id: i64 = sqlx::query_scalar(
            r#"
            INSERT INTO "post" ("title", "body", "published", "author_id")
            VALUES ($1, $2, $3, $4)
            RETURNING "id"::bigint
            "#,
        )
        .bind(title)
        .bind(body)
        .bind(published)
        .bind(author_id)
        .fetch_one(&pool)
        .await
        .expect("should insert post");

        post_ids.push(post_id);
    }

    for (post_id, body) in [
        (post_ids[0], "First comment on first post"),
        (post_ids[0], "Second comment on first post"),
        (post_ids[1], "Only comment on second post"),
    ] {
        sqlx::query(
            r#"
            INSERT INTO "comment" ("body", "post_id")
            VALUES ($1, $2)
            "#,
        )
        .bind(body)
        .bind(post_id)
        .execute(&pool)
        .await
        .expect("should insert comment");
    }

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

    let users = client
        .find_many(query! {
            crate::my_schema,
            user {
                select: {
                    id: true,
                    email: true,
                    name: true,
                },
                include: {
                    posts: {
                        select: {
                            id: true,
                            title: true,
                        },
                    },
                },
            }
        })
        .await
        .expect("query should succeed");

    assert_eq!(users.len(), 1);

    let user = &users[0];
    assert_eq!(user.id, author_id);
    assert_eq!(user.email, "alice@example.com");
    assert_eq!(user.name, "Alice");
    assert_eq!(user.posts.len(), 2);
    assert_eq!(user.posts[0].id, 1);
    assert_eq!(user.posts[0].title, "Hello from Vitrail");
    assert_eq!(user.posts[1].id, 2);
    assert_eq!(user.posts[1].title, "Second post");

    database.cleanup().await;
}

#[tokio::test]
async fn model_first_named_query_on_postgres() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();
    let author_id = setup_database(&database_url).await;

    let client = my_schema::VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let users = client
        .find_many(my_schema::query::<UserWithPosts>())
        .await
        .expect("query should succeed");

    assert_eq!(users.len(), 1);

    let user = &users[0];
    assert_eq!(user.id, author_id);
    assert_eq!(user.email, "alice@example.com");
    assert_eq!(user.name, "Alice");
    assert!(user.created_at <= chrono::Utc::now());
    assert_eq!(user.posts.len(), 2);
    assert_eq!(user.posts[0].id, 1);
    assert_eq!(user.posts[0].title, "Hello from Vitrail");
    assert_eq!(user.posts[1].id, 2);
    assert_eq!(user.posts[1].title, "Second post");

    database.cleanup().await;
}

#[tokio::test]
async fn model_first_to_one_include_query_on_postgres() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();
    let author_id = setup_database(&database_url).await;

    let client = my_schema::VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let posts = client
        .find_many(my_schema::query::<PostWithAuthor>())
        .await
        .expect("query should succeed");

    assert_eq!(posts.len(), 2);

    let post = &posts[0];
    assert_eq!(post.id, 1);
    assert_eq!(post.title, "Hello from Vitrail");
    assert_eq!(post.author.id, author_id);
    assert_eq!(post.author.email, "alice@example.com");
    assert_eq!(post.author.name, "Alice");
    assert!(post.author.created_at <= chrono::Utc::now());

    database.cleanup().await;
}

#[tokio::test]
async fn model_first_recursive_nested_include_query_on_postgres() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();
    let author_id = setup_database(&database_url).await;

    let client = my_schema::VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let users = client
        .find_many(my_schema::query::<UserWithPostsAndComments>())
        .await
        .expect("query should succeed");

    assert_eq!(users.len(), 1);

    let user = &users[0];
    assert_eq!(user.id, author_id);
    assert_eq!(user.email, "alice@example.com");
    assert_eq!(user.posts.len(), 2);

    assert_eq!(user.posts[0].id, 1);
    assert_eq!(user.posts[0].title, "Hello from Vitrail");
    assert_eq!(user.posts[0].comments.len(), 2);
    assert_eq!(user.posts[0].comments[0].id, 1);
    assert_eq!(
        user.posts[0].comments[0].body,
        "First comment on first post"
    );
    assert_eq!(user.posts[0].comments[1].id, 2);
    assert_eq!(
        user.posts[0].comments[1].body,
        "Second comment on first post"
    );

    assert_eq!(user.posts[1].id, 2);
    assert_eq!(user.posts[1].title, "Second post");
    assert_eq!(user.posts[1].comments.len(), 1);
    assert_eq!(user.posts[1].comments[0].id, 3);
    assert_eq!(
        user.posts[1].comments[0].body,
        "Only comment on second post"
    );

    database.cleanup().await;
}
