use crate::support::{TestDatabase, apply_schema};
use sqlx::postgres::PgPoolOptions;
use vitrail_pg::{PostgresSchema, QueryResult, QueryVariables, VitrailClient, query, schema};

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

pub(crate) use self::query_schema as pg_query_schema;

#[derive(QueryResult)]
#[vitrail(schema = crate::query_schema::Schema, model = user)]
struct UserSummary {
    id: i64,
    email: String,
    name: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(QueryResult)]
#[vitrail(schema = crate::query_schema::Schema, model = post)]
struct PostSummary {
    id: i64,
    title: String,
}

#[derive(QueryResult)]
#[vitrail(schema = crate::query_schema::Schema, model = comment)]
struct CommentSummary {
    id: i64,
    body: String,
}

#[derive(QueryResult)]
#[vitrail(schema = crate::query_schema::Schema, model = post)]
struct PostWithAuthor {
    id: i64,
    title: String,
    #[vitrail(include)]
    author: UserSummary,
}

#[derive(QueryResult)]
#[vitrail(schema = crate::query_schema::Schema, model = post)]
struct PostWithComments {
    id: i64,
    title: String,
    #[vitrail(include)]
    comments: Vec<CommentSummary>,
}

#[derive(QueryResult)]
#[vitrail(schema = crate::query_schema::Schema, model = user)]
struct UserWithPosts {
    id: i64,
    email: String,
    name: String,
    created_at: chrono::DateTime<chrono::Utc>,
    #[vitrail(include)]
    posts: Vec<PostSummary>,
}

#[derive(QueryResult)]
#[vitrail(schema = crate::query_schema::Schema, model = user)]
struct UserWithPostsAndComments {
    id: i64,
    email: String,
    #[vitrail(include)]
    posts: Vec<PostWithComments>,
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

#[derive(QueryVariables)]
struct UserWithFilteredPostsVariables {
    user_id: i64,
    post_id: i64,
}

#[derive(QueryResult)]
#[vitrail(
    schema = crate::query_schema::Schema,
    model = post,
    variables = UserWithFilteredPostsVariables,
    where(id = eq(post_id))
)]
struct FilteredPostSummary {
    id: i64,
    title: String,
}

#[derive(QueryResult)]
#[vitrail(
    schema = crate::query_schema::Schema,
    model = user,
    variables = UserWithFilteredPostsVariables,
    where(id = eq(user_id))
)]
struct UserWithFilteredPosts {
    id: i64,
    email: String,
    #[vitrail(include)]
    posts: Vec<FilteredPostSummary>,
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

#[derive(QueryVariables)]
struct PostsByIdsVariables {
    post_ids: Vec<i64>,
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

#[derive(QueryResult)]
#[vitrail(
    schema = crate::query_schema::Schema,
    model = post,
    variables = PostsByIdsVariables,
    where(id = in(post_ids))
)]
struct PostByIds {
    id: i64,
    title: String,
}

async fn setup_database(database_url: &str) -> i64 {
    apply_schema(
        database_url,
        &PostgresSchema::from_schema_access::<crate::query_schema::Schema>(),
    )
    .await;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
        .expect("should connect to postgres");

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
async fn ad_hoc_where_query_on_postgres() {
    let database = TestDatabase::new().await;
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

    database.cleanup().await;
}

#[tokio::test]
async fn ad_hoc_null_where_query_on_postgres() {
    let database = TestDatabase::new().await;
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

    database.cleanup().await;
}

#[tokio::test]
async fn ad_hoc_not_null_where_query_on_postgres() {
    let database = TestDatabase::new().await;
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

    database.cleanup().await;
}

#[tokio::test]
async fn model_first_null_where_query_on_postgres() {
    let database = TestDatabase::new().await;
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

    database.cleanup().await;
}

#[tokio::test]
async fn model_first_not_null_where_query_on_postgres() {
    let database = TestDatabase::new().await;
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

    database.cleanup().await;
}

#[tokio::test]
async fn ad_hoc_in_where_query_on_postgres() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();
    let author_id = setup_database(&database_url).await;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("should connect to postgres");

    let post_ids = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT "id"::bigint
        FROM "post"
        WHERE "author_id" = $1
        ORDER BY "id"
        "#,
    )
    .bind(author_id)
    .fetch_all(&pool)
    .await
    .expect("should fetch post ids");

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
                    id: {
                        in: post_ids.clone()
                    }
                },
            }
        })
        .await
        .expect("query should succeed");

    assert_eq!(posts.len(), 2);
    assert_eq!(posts[0].id, post_ids[0]);
    assert_eq!(posts[0].title, "Hello from Vitrail");
    assert_eq!(posts[1].id, post_ids[1]);
    assert_eq!(posts[1].title, "Second post");

    client.close().await;
    pool.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn ad_hoc_not_equal_where_query_on_postgres() {
    let database = TestDatabase::new().await;
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

    database.cleanup().await;
}

#[tokio::test]
async fn model_first_in_where_query_on_postgres() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();
    let author_id = setup_database(&database_url).await;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("should connect to postgres");

    let post_ids = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT "id"::bigint
        FROM "post"
        WHERE "author_id" = $1
        ORDER BY "id"
        "#,
    )
    .bind(author_id)
    .fetch_all(&pool)
    .await
    .expect("should fetch post ids");

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let posts = client
        .find_many(crate::query_schema::query_with_variables::<PostByIds>(
            PostsByIdsVariables {
                post_ids: post_ids.clone(),
            },
        ))
        .await
        .expect("query should succeed");

    assert_eq!(posts.len(), 2);
    assert_eq!(posts[0].id, post_ids[0]);
    assert_eq!(posts[0].title, "Hello from Vitrail");
    assert_eq!(posts[1].id, post_ids[1]);
    assert_eq!(posts[1].title, "Second post");

    client.close().await;
    pool.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn model_first_not_equal_where_query_on_postgres() {
    let database = TestDatabase::new().await;
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

    database.cleanup().await;
}

#[tokio::test]
async fn model_first_named_query_on_postgres() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();
    let author_id = setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let users = client
        .find_many(crate::query_schema::query::<UserWithPosts>())
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
async fn model_first_where_query_on_postgres() {
    let database = TestDatabase::new().await;
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

    database.cleanup().await;
}

#[tokio::test]
async fn nested_model_first_where_query_on_postgres() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();
    let author_id = setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let user = client
        .find_first(crate::query_schema::query_with_variables::<
            UserWithFilteredPosts,
        >(UserWithFilteredPostsVariables {
            user_id: author_id,
            post_id: 2,
        }))
        .await
        .expect("query should succeed");

    assert_eq!(user.id, author_id);
    assert_eq!(user.email, "alice@example.com");
    assert_eq!(user.posts.len(), 1);
    assert_eq!(user.posts[0].id, 2);
    assert_eq!(user.posts[0].title, "Second post");

    database.cleanup().await;
}

#[tokio::test]
async fn model_first_to_one_include_query_on_postgres() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();
    let author_id = setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let posts = client
        .find_many(crate::query_schema::query::<PostWithAuthor>())
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

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let users = client
        .find_many(crate::query_schema::query::<UserWithPostsAndComments>())
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

#[tokio::test]
async fn find_optional_returns_some_when_a_row_exists() {
    let database = TestDatabase::new().await;
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

    database.cleanup().await;
}

#[tokio::test]
async fn find_optional_returns_none_when_no_rows_exist() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();

    apply_schema(
        &database_url,
        &PostgresSchema::from_schema_access::<crate::query_schema::Schema>(),
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

    database.cleanup().await;
}

#[tokio::test]
async fn find_first_returns_the_first_row_when_one_exists() {
    let database = TestDatabase::new().await;
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

    database.cleanup().await;
}

#[tokio::test]
async fn find_first_returns_row_not_found_when_no_rows_exist() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();

    apply_schema(
        &database_url,
        &PostgresSchema::from_schema_access::<crate::query_schema::Schema>(),
    )
    .await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let result = client
        .find_first(crate::query_schema::query::<UserSummary>())
        .await;

    assert!(matches!(result, Err(sqlx::Error::RowNotFound)));

    database.cleanup().await;
}

schema! {
    name compound_query_schema

    model post {
        id      Int           @id @default(autoincrement())
        title   String
        locales post_locale[]
    }

    model post_locale {
        post_id Int
        locale  String
        title   String
        post    post               @relation(fields: [post_id], references: [id])
        notes   translation_note[]

        @@id([post_id, locale])
    }

    model translation_note {
        id          Int         @id @default(autoincrement())
        post_id     Int
        locale      String
        body        String
        translation post_locale @relation(fields: [post_id, locale], references: [post_id, locale])
    }
}

#[derive(QueryResult)]
#[vitrail(schema = crate::query::compound_query_schema::Schema, model = post_locale)]
struct CompoundPostLocaleSummary {
    post_id: i64,
    locale: String,
    title: String,
}

#[derive(QueryResult)]
#[vitrail(schema = crate::query::compound_query_schema::Schema, model = translation_note)]
struct CompoundTranslationNoteWithTranslation {
    id: i64,
    body: String,
    #[vitrail(include)]
    translation: CompoundPostLocaleSummary,
}

#[derive(QueryResult)]
#[vitrail(schema = crate::query::compound_query_schema::Schema, model = post)]
struct CompoundPostWithLocales {
    id: i64,
    title: String,
    #[vitrail(include)]
    locales: Vec<CompoundPostLocaleSummary>,
}

async fn setup_compound_database(database_url: &str) -> i64 {
    apply_schema(
        database_url,
        &PostgresSchema::from_schema_access::<crate::query::compound_query_schema::Schema>(),
    )
    .await;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
        .expect("should connect to postgres");

    let post_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO "post" ("title")
        VALUES ('Localized post')
        RETURNING "id"::bigint
        "#,
    )
    .fetch_one(&pool)
    .await
    .expect("should insert post");

    for (locale, title) in [("en", "Hello"), ("fr", "Bonjour")] {
        sqlx::query(
            r#"
            INSERT INTO "post_locale" ("post_id", "locale", "title")
            VALUES ($1, $2, $3)
            "#,
        )
        .bind(post_id)
        .bind(locale)
        .bind(title)
        .execute(&pool)
        .await
        .expect("should insert post locale");
    }

    for (locale, body) in [
        ("fr", "French translation note"),
        ("en", "English translation note"),
    ] {
        sqlx::query(
            r#"
            INSERT INTO "translation_note" ("post_id", "locale", "body")
            VALUES ($1, $2, $3)
            "#,
        )
        .bind(post_id)
        .bind(locale)
        .bind(body)
        .execute(&pool)
        .await
        .expect("should insert translation note");
    }

    pool.close().await;
    post_id
}

#[tokio::test]
async fn compound_to_one_include_query_on_postgres() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();
    let post_id = setup_compound_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let notes = client
        .find_many(crate::query::compound_query_schema::query::<
            CompoundTranslationNoteWithTranslation,
        >())
        .await
        .expect("query should succeed");

    assert_eq!(notes.len(), 2);
    assert_eq!(notes[0].id, 1);
    assert_eq!(notes[0].body, "French translation note");
    assert_eq!(notes[0].translation.post_id, post_id);
    assert_eq!(notes[0].translation.locale, "fr");
    assert_eq!(notes[0].translation.title, "Bonjour");
    assert_eq!(notes[1].id, 2);
    assert_eq!(notes[1].body, "English translation note");
    assert_eq!(notes[1].translation.post_id, post_id);
    assert_eq!(notes[1].translation.locale, "en");
    assert_eq!(notes[1].translation.title, "Hello");

    database.cleanup().await;
}

#[tokio::test]
async fn compound_to_many_include_query_on_postgres() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();
    let post_id = setup_compound_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let posts = client
        .find_many(crate::query::compound_query_schema::query::<
            CompoundPostWithLocales,
        >())
        .await
        .expect("query should succeed");

    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0].id, post_id);
    assert_eq!(posts[0].title, "Localized post");
    assert_eq!(posts[0].locales.len(), 2);
    assert_eq!(posts[0].locales[0].post_id, post_id);
    assert_eq!(posts[0].locales[0].locale, "en");
    assert_eq!(posts[0].locales[0].title, "Hello");
    assert_eq!(posts[0].locales[1].post_id, post_id);
    assert_eq!(posts[0].locales[1].locale, "fr");
    assert_eq!(posts[0].locales[1].title, "Bonjour");

    database.cleanup().await;
}
