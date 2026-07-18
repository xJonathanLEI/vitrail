use crate::support::{TestDatabase, apply_schema};
use sqlx::sqlite::SqlitePoolOptions;
use vitrail_sqlite::{QueryResult, QueryVariables, SqliteSchema, VitrailClient, query, schema};

schema! {
    name query_schema

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

pub(crate) use self::query_schema as sqlite_query_schema;

#[derive(QueryResult)]
#[vitrail(schema = crate::query_schema::Schema, model = user)]
struct UserSummary {
    id: i64,
    email: String,
    name: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(QueryVariables)]
struct UserByIdVariables {
    user_id: i64,
}

#[derive(QueryResult)]
#[vitrail(
    schema = crate::query_schema::Schema,
    model = user,
    variables = UserByIdVariables,
    where(id = eq(user_id))
)]
struct UserById {
    id: i64,
    email: String,
    name: String,
}

#[derive(QueryResult)]
#[vitrail(
    schema = crate::query_schema::Schema,
    model = post,
    where(body = null)
)]
struct PostWithNullBody {
    id: i64,
    title: String,
    body: Option<String>,
}

#[derive(QueryResult)]
#[vitrail(
    schema = crate::query_schema::Schema,
    model = post,
    where(body = not(null))
)]
struct PostWithNonNullBody {
    id: i64,
    title: String,
    body: Option<String>,
}

#[derive(QueryVariables)]
struct PostByExcludedTitleVariables {
    excluded_title: String,
}

#[derive(QueryResult)]
#[vitrail(
    schema = crate::query_schema::Schema,
    model = post,
    variables = PostByExcludedTitleVariables,
    where(title = not(excluded_title))
)]
struct PostWithDifferentTitle {
    id: i64,
    title: String,
}

async fn setup_database(database_url: &str) -> i64 {
    apply_schema(
        database_url,
        &SqliteSchema::from_schema_access::<crate::query_schema::Schema>(),
    )
    .await;

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
        .expect("should connect to SQLite");

    let author_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO "user" ("email", "name")
        VALUES ('alice@example.com', 'Alice')
        RETURNING "id"
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
            VALUES (?, ?, ?, ?)
            RETURNING "id"
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
            VALUES (?, ?)
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
async fn ad_hoc_where_query_on_sqlite() {
    let database = TestDatabase::new();
    let database_url = database.url().to_owned();
    let author_id = setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let users = client
        .find_many(query! {
            crate::query_schema,
            user {
                select: {
                    id: true,
                    email: true,
                    name: true,
                },
                where: {
                    id: {
                        eq: author_id
                    }
                },
            }
        })
        .await
        .expect("query should succeed");

    assert_eq!(users.len(), 1);
    assert_eq!(users[0].id, author_id);
    assert_eq!(users[0].email, "alice@example.com");
    assert_eq!(users[0].name, "Alice");

    client.close().await;
    database.cleanup();
}

#[tokio::test]
async fn ad_hoc_null_where_query_on_sqlite() {
    let database = TestDatabase::new();
    let database_url = database.url().to_owned();
    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let posts = client
        .find_many(query! {
            crate::query_schema,
            post {
                select: {
                    id: true,
                    title: true,
                    body: true,
                },
                where: {
                    body: null
                },
            }
        })
        .await
        .expect("query should succeed");

    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0].id, 2);
    assert_eq!(posts[0].title, "Second post");
    assert_eq!(posts[0].body, None);

    client.close().await;
    database.cleanup();
}

#[tokio::test]
async fn ad_hoc_not_null_where_query_on_sqlite() {
    let database = TestDatabase::new();
    let database_url = database.url().to_owned();
    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let posts = client
        .find_many(query! {
            crate::query_schema,
            post {
                select: {
                    id: true,
                    title: true,
                    body: true,
                },
                where: {
                    body: {
                        not: null
                    }
                },
            }
        })
        .await
        .expect("query should succeed");

    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0].id, 1);
    assert_eq!(posts[0].title, "Hello from Vitrail");
    assert_eq!(posts[0].body.as_deref(), Some("This is the post body"));

    client.close().await;
    database.cleanup();
}

#[tokio::test]
async fn model_first_null_where_query_on_sqlite() {
    let database = TestDatabase::new();
    let database_url = database.url().to_owned();
    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let posts = client
        .find_many(crate::query_schema::query::<PostWithNullBody>())
        .await
        .expect("query should succeed");

    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0].id, 2);
    assert_eq!(posts[0].title, "Second post");
    assert_eq!(posts[0].body, None);

    client.close().await;
    database.cleanup();
}

#[tokio::test]
async fn model_first_not_null_where_query_on_sqlite() {
    let database = TestDatabase::new();
    let database_url = database.url().to_owned();
    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let posts = client
        .find_many(crate::query_schema::query::<PostWithNonNullBody>())
        .await
        .expect("query should succeed");

    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0].id, 1);
    assert_eq!(posts[0].title, "Hello from Vitrail");
    assert_eq!(posts[0].body.as_deref(), Some("This is the post body"));

    client.close().await;
    database.cleanup();
}

#[tokio::test]
async fn ad_hoc_not_equal_where_query_on_sqlite() {
    let database = TestDatabase::new();
    let database_url = database.url().to_owned();
    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let posts = client
        .find_many(query! {
            crate::query_schema,
            post {
                select: {
                    id: true,
                    title: true,
                },
                where: {
                    title: {
                        not: "Second post".to_owned()
                    }
                },
            }
        })
        .await
        .expect("query should succeed");

    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0].id, 1);
    assert_eq!(posts[0].title, "Hello from Vitrail");

    client.close().await;
    database.cleanup();
}

#[tokio::test]
async fn model_first_not_equal_where_query_on_sqlite() {
    let database = TestDatabase::new();
    let database_url = database.url().to_owned();
    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let posts = client
        .find_many(crate::query_schema::query_with_variables::<
            PostWithDifferentTitle,
        >(PostByExcludedTitleVariables {
            excluded_title: "Second post".to_owned(),
        }))
        .await
        .expect("query should succeed");

    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0].id, 1);
    assert_eq!(posts[0].title, "Hello from Vitrail");

    client.close().await;
    database.cleanup();
}

#[tokio::test]
async fn model_first_where_query_on_sqlite() {
    let database = TestDatabase::new();
    let database_url = database.url().to_owned();
    let author_id = setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let user = client
        .find_first(crate::query_schema::query_with_variables::<UserById>(
            UserByIdVariables { user_id: author_id },
        ))
        .await
        .expect("query should succeed");

    assert_eq!(user.id, author_id);
    assert_eq!(user.email, "alice@example.com");
    assert_eq!(user.name, "Alice");

    client.close().await;
    database.cleanup();
}

#[tokio::test]
async fn find_optional_returns_some_when_a_row_exists() {
    let database = TestDatabase::new();
    let database_url = database.url().to_owned();
    let author_id = setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let user = client
        .find_optional(crate::query_schema::query::<UserSummary>())
        .await
        .expect("query should succeed")
        .expect("query should return a row");

    assert_eq!(user.id, author_id);
    assert_eq!(user.email, "alice@example.com");
    assert_eq!(user.name, "Alice");
    assert!(user.created_at <= chrono::Utc::now());

    client.close().await;
    database.cleanup();
}

#[tokio::test]
async fn find_optional_returns_none_when_no_rows_exist() {
    let database = TestDatabase::new();
    let database_url = database.url().to_owned();

    apply_schema(
        &database_url,
        &SqliteSchema::from_schema_access::<crate::query_schema::Schema>(),
    )
    .await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let user = client
        .find_optional(crate::query_schema::query::<UserSummary>())
        .await
        .expect("query should succeed");

    assert!(user.is_none());

    client.close().await;
    database.cleanup();
}

#[tokio::test]
async fn find_first_returns_the_first_row_when_one_exists() {
    let database = TestDatabase::new();
    let database_url = database.url().to_owned();
    let author_id = setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let user = client
        .find_first(crate::query_schema::query::<UserSummary>())
        .await
        .expect("query should succeed");

    assert_eq!(user.id, author_id);
    assert_eq!(user.email, "alice@example.com");
    assert_eq!(user.name, "Alice");
    assert!(user.created_at <= chrono::Utc::now());

    client.close().await;
    database.cleanup();
}

#[tokio::test]
async fn find_first_returns_row_not_found_when_no_rows_exist() {
    let database = TestDatabase::new();
    let database_url = database.url().to_owned();

    apply_schema(
        &database_url,
        &SqliteSchema::from_schema_access::<crate::query_schema::Schema>(),
    )
    .await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let result = client
        .find_first(crate::query_schema::query::<UserSummary>())
        .await;

    assert!(matches!(result, Err(sqlx::Error::RowNotFound)));

    client.close().await;
    database.cleanup();
}
