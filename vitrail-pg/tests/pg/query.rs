use crate::support::{TestDatabase, apply_schema};
use sqlx::postgres::PgPoolOptions;
use vitrail_pg::{PostgresSchema, QueryResult, VitrailClient, query, schema};

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
