use crate::support::{TestDatabase, apply_schema};
use sqlx::postgres::PgPoolOptions;
use vitrail_pg::{
    DeleteMany, DeleteManyModel, PostgresSchema, QueryFilter, QueryFilterValue, QueryVariableValue,
    QueryVariables, VitrailClient, delete, schema,
};

schema! {
    name delete_schema

    model user {
        id        Int    @id @default(autoincrement())
        email     String @unique
        name      String
        age       Int
        posts     post[]
    }

    model post {
        id        Int     @id @default(autoincrement())
        title     String
        body      String?
        published Boolean
        author_id Int
        author    user    @relation(fields: [author_id], references: [id])
        comments  comment[]
    }

    model comment {
        id       Int     @id @default(autoincrement())
        body     String
        reviewed Boolean
        post_id  Int
        post     post    @relation(fields: [post_id], references: [id])
    }
}

pub(crate) use self::delete_schema as pg_delete_schema;

#[derive(vitrail_pg::QueryVariables)]
struct AuthorAgeVariables {
    author_age: i64,
}

#[derive(DeleteMany)]
#[vitrail(schema = crate::delete_schema::Schema, model = comment)]
struct DerivedDeleteAllComments;

#[derive(DeleteMany)]
#[vitrail(
    schema = crate::delete_schema::Schema,
    model = comment,
    variables = AuthorAgeVariables,
    where(post.author.age = eq(author_age))
)]
struct DerivedDeleteCommentsByPostAuthorAge;

#[derive(DeleteMany)]
#[vitrail(
    schema = crate::delete_schema::Schema,
    model = comment,
    where(post.body = null)
)]
struct DerivedDeletePostsWithNullBody;

#[derive(DeleteMany)]
#[vitrail(
    schema = crate::delete_schema::Schema,
    model = comment,
    where(post.body = not(null))
)]
struct DerivedDeletePostsWithNonNullBody;

#[derive(vitrail_pg::QueryVariables)]
struct PostBodyVariables {
    excluded_body: String,
}

#[derive(DeleteMany)]
#[vitrail(
    schema = crate::delete_schema::Schema,
    model = comment,
    variables = PostBodyVariables,
    where(post.body = not(excluded_body))
)]
struct DerivedDeleteCommentsByPostBody;

struct DeleteReviewedComments;

impl DeleteManyModel for DeleteReviewedComments {
    type Schema = crate::delete_schema::Schema;
    type Variables = ();

    fn model_name() -> &'static str {
        "comment"
    }

    fn filter() -> Option<QueryFilter> {
        Some(QueryFilter::eq("reviewed", false))
    }
}

struct DeleteCommentsByPostAuthorAge;

impl DeleteManyModel for DeleteCommentsByPostAuthorAge {
    type Schema = crate::delete_schema::Schema;
    type Variables = QueryVariables;

    fn model_name() -> &'static str {
        "comment"
    }

    fn filter_with_variables(_variables: &QueryVariables) -> Option<QueryFilter> {
        Some(QueryFilter::relation(
            "post",
            QueryFilter::relation(
                "author",
                QueryFilter::eq("age", QueryFilterValue::variable("author_age")),
            ),
        ))
    }
}

async fn setup_database(database_url: &str) {
    apply_schema(
        database_url,
        &PostgresSchema::from_schema_access::<crate::delete_schema::Schema>(),
    )
    .await;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
        .expect("should connect to postgres");

    let alice_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO "user" ("email", "name", "age")
        VALUES ('alice@example.com', 'Alice', 35)
        RETURNING "id"::bigint
        "#,
    )
    .fetch_one(&pool)
    .await
    .expect("should insert alice");

    let bob_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO "user" ("email", "name", "age")
        VALUES ('bob@example.com', 'Bob', 28)
        RETURNING "id"::bigint
        "#,
    )
    .fetch_one(&pool)
    .await
    .expect("should insert bob");

    let alice_post_1: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO "post" ("title", "body", "published", "author_id")
        VALUES ('Alice draft 1', 'Alice draft body 1', false, $1)
        RETURNING "id"::bigint
        "#,
    )
    .bind(alice_id)
    .fetch_one(&pool)
    .await
    .expect("should insert alice post 1");

    let alice_post_2: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO "post" ("title", "body", "published", "author_id")
        VALUES ('Alice draft 2', NULL, false, $1)
        RETURNING "id"::bigint
        "#,
    )
    .bind(alice_id)
    .fetch_one(&pool)
    .await
    .expect("should insert alice post 2");

    let bob_post: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO "post" ("title", "body", "published", "author_id")
        VALUES ('Bob already published', 'Bob published body', true, $1)
        RETURNING "id"::bigint
        "#,
    )
    .bind(bob_id)
    .fetch_one(&pool)
    .await
    .expect("should insert bob post");

    for (body, reviewed, post_id) in [
        ("Alice comment 1", false, alice_post_1),
        ("Alice comment 2", false, alice_post_2),
        ("Bob comment", false, bob_post),
    ] {
        sqlx::query(
            r#"
            INSERT INTO "comment" ("body", "reviewed", "post_id")
            VALUES ($1, $2, $3)
            "#,
        )
        .bind(body)
        .bind(reviewed)
        .bind(post_id)
        .execute(&pool)
        .await
        .expect("should insert comment");
    }

    pool.close().await;
}

#[tokio::test]
async fn delete_many_deletes_multiple_rows_and_returns_count() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();
    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let count = client
        .delete_many(DeleteMany::<
            crate::delete_schema::Schema,
            DeleteReviewedComments,
        >::new())
        .await
        .expect("delete should succeed");

    assert_eq!(count, 3);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("should connect to postgres");

    let remaining_comments: i64 = sqlx::query_scalar(r#"SELECT COUNT(*)::bigint FROM "comment""#)
        .fetch_one(&pool)
        .await
        .expect("should count comments");

    assert_eq!(remaining_comments, 0);

    pool.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn delete_many_deletes_rows_matching_not_equal_filter() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();
    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let count = client
        .delete_many(crate::delete_schema::delete_many_with_variables::<
            DerivedDeleteCommentsByPostBody,
        >(PostBodyVariables {
            excluded_body: "Bob published body".to_owned(),
        }))
        .await
        .expect("delete should succeed");

    assert_eq!(count, 1);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("should connect to postgres");

    let remaining_comments: i64 = sqlx::query_scalar(r#"SELECT COUNT(*)::bigint FROM "comment""#)
        .fetch_one(&pool)
        .await
        .expect("should count comments");

    assert_eq!(remaining_comments, 2);

    let remaining_bob_comments: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::bigint
        FROM "comment" AS "c"
        JOIN "post" AS "p" ON "p"."id" = "c"."post_id"
        WHERE "p"."body" = 'Bob published body'
        "#,
    )
    .fetch_one(&pool)
    .await
    .expect("should count bob comments");

    assert_eq!(remaining_bob_comments, 1);

    pool.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn delete_many_supports_deeply_nested_relation_filters() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();
    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let count = client
        .delete_many(DeleteMany::<
            crate::delete_schema::Schema,
            DeleteCommentsByPostAuthorAge,
        >::new_with_variables(QueryVariables::from_values(
            vec![("author_age", QueryVariableValue::from(35_i64))],
        )))
        .await
        .expect("delete should succeed");

    assert_eq!(count, 2);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("should connect to postgres");

    let remaining_comments: i64 = sqlx::query_scalar(r#"SELECT COUNT(*)::bigint FROM "comment""#)
        .fetch_one(&pool)
        .await
        .expect("should count comments");

    let bob_comments: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::bigint
        FROM "comment"
        WHERE "post_id" IN (
            SELECT "id"
            FROM "post"
            WHERE "author_id" = (SELECT "id" FROM "user" WHERE "email" = 'bob@example.com')
        )
        "#,
    )
    .fetch_one(&pool)
    .await
    .expect("should count bob comments");

    assert_eq!(remaining_comments, 1);
    assert_eq!(bob_comments, 1);

    pool.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn derived_delete_many_deletes_rows_and_returns_count() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();
    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let count = client
        .delete_many(crate::delete_schema::delete_many::<DerivedDeleteAllComments>())
        .await
        .expect("delete should succeed");

    assert_eq!(count, 3);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("should connect to postgres");

    let remaining_comments: i64 = sqlx::query_scalar(r#"SELECT COUNT(*)::bigint FROM "comment""#)
        .fetch_one(&pool)
        .await
        .expect("should count comments");

    assert_eq!(remaining_comments, 0);

    pool.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn derived_delete_many_supports_deeply_nested_relation_filters() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();
    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let count = client
        .delete_many(crate::delete_schema::delete_many_with_variables::<
            DerivedDeleteCommentsByPostAuthorAge,
        >(AuthorAgeVariables { author_age: 35 }))
        .await
        .expect("delete should succeed");

    assert_eq!(count, 2);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("should connect to postgres");

    let remaining_comments: i64 = sqlx::query_scalar(r#"SELECT COUNT(*)::bigint FROM "comment""#)
        .fetch_one(&pool)
        .await
        .expect("should count comments");

    let bob_comments: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::bigint
        FROM "comment"
        WHERE "post_id" IN (
            SELECT "id"
            FROM "post"
            WHERE "author_id" = (SELECT "id" FROM "user" WHERE "email" = 'bob@example.com')
        )
        "#,
    )
    .fetch_one(&pool)
    .await
    .expect("should count bob comments");

    assert_eq!(remaining_comments, 1);
    assert_eq!(bob_comments, 1);

    pool.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn helper_delete_many_deletes_rows_and_returns_count() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();
    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let count = client
        .delete_many(delete! {
            crate::delete_schema,
            comment {}
        })
        .await
        .expect("delete should succeed");

    assert_eq!(count, 3);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("should connect to postgres");

    let remaining_comments: i64 = sqlx::query_scalar(r#"SELECT COUNT(*)::bigint FROM "comment""#)
        .fetch_one(&pool)
        .await
        .expect("should count comments");

    assert_eq!(remaining_comments, 0);

    pool.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn helper_delete_many_supports_deeply_nested_relation_filters() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();
    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let count = client
        .delete_many(delete! {
            crate::delete_schema,
            comment {
                where: {
                    post: {
                        author: {
                            age: {
                                eq: 35
                            }
                        }
                    },
                },
            }
        })
        .await
        .expect("delete should succeed");

    assert_eq!(count, 2);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("should connect to postgres");

    let remaining_comments: i64 = sqlx::query_scalar(r#"SELECT COUNT(*)::bigint FROM "comment""#)
        .fetch_one(&pool)
        .await
        .expect("should count comments");

    let bob_comments: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::bigint
        FROM "comment"
        WHERE "post_id" IN (
            SELECT "id"
            FROM "post"
            WHERE "author_id" = (SELECT "id" FROM "user" WHERE "email" = 'bob@example.com')
        )
        "#,
    )
    .fetch_one(&pool)
    .await
    .expect("should count bob comments");

    assert_eq!(remaining_comments, 1);
    assert_eq!(bob_comments, 1);

    pool.close().await;
    database.cleanup().await;
}

#[test]
fn delete_many_generates_expected_sql_for_nested_relation_filter() {
    let sql =
        DeleteMany::<crate::delete_schema::Schema, DeleteCommentsByPostAuthorAge>::new_with_variables(
            QueryVariables::from_values(vec![("author_age", QueryVariableValue::from(35_i64))]),
        )
        .to_sql()
        .expect("sql generation should succeed");

    assert_eq!(
        sql,
        [
            r#"DELETE FROM "comment" AS "t0""#,
            r#"WHERE EXISTS (SELECT 1 FROM "post" AS "t1" WHERE "t1"."id" = "t0"."post_id" AND EXISTS (SELECT 1 FROM "user" AS "t2" WHERE "t2"."id" = "t1"."author_id" AND ("t2"."age")::bigint = $1))"#,
        ]
        .join(" ")
    );
}

#[tokio::test]
async fn delete_many_deletes_rows_matching_null_filter() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();
    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let deleted = client
        .delete_many(crate::delete_schema::delete_many::<
            DerivedDeletePostsWithNullBody,
        >())
        .await
        .expect("delete should succeed");

    assert_eq!(deleted, 1);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("should connect to postgres");

    let remaining_comments_with_null_body_post: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::bigint
        FROM "comment" AS "c"
        INNER JOIN "post" AS "p" ON "p"."id" = "c"."post_id"
        WHERE "p"."body" IS NULL
        "#,
    )
    .fetch_one(&pool)
    .await
    .expect("should count comments for posts with null body");

    let remaining_comments_with_non_null_body_post: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::bigint
        FROM "comment" AS "c"
        INNER JOIN "post" AS "p" ON "p"."id" = "c"."post_id"
        WHERE "p"."body" IS NOT NULL
        "#,
    )
    .fetch_one(&pool)
    .await
    .expect("should count comments for posts with non-null body");

    assert_eq!(remaining_comments_with_null_body_post, 0);
    assert_eq!(remaining_comments_with_non_null_body_post, 2);

    pool.close().await;
    client.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn delete_many_deletes_rows_matching_not_null_filter() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();
    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let deleted = client
        .delete_many(crate::delete_schema::delete_many::<
            DerivedDeletePostsWithNonNullBody,
        >())
        .await
        .expect("delete should succeed");

    assert_eq!(deleted, 2);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("should connect to postgres");

    let remaining_comments_with_null_body_post: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::bigint
        FROM "comment" AS "c"
        INNER JOIN "post" AS "p" ON "p"."id" = "c"."post_id"
        WHERE "p"."body" IS NULL
        "#,
    )
    .fetch_one(&pool)
    .await
    .expect("should count comments for posts with null body");

    let remaining_comments_with_non_null_body_post: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::bigint
        FROM "comment" AS "c"
        INNER JOIN "post" AS "p" ON "p"."id" = "c"."post_id"
        WHERE "p"."body" IS NOT NULL
        "#,
    )
    .fetch_one(&pool)
    .await
    .expect("should count comments for posts with non-null body");

    assert_eq!(remaining_comments_with_null_body_post, 1);
    assert_eq!(remaining_comments_with_non_null_body_post, 0);

    pool.close().await;
    client.close().await;
    database.cleanup().await;
}

#[test]
fn delete_many_generates_expected_sql_for_null_filter() {
    let sql = crate::delete_schema::delete_many::<DerivedDeletePostsWithNullBody>()
        .to_sql()
        .expect("sql generation should succeed");

    assert_eq!(
        sql,
        [
            r#"DELETE FROM "comment" AS "t0""#,
            r#"WHERE EXISTS (SELECT 1 FROM "post" AS "t1" WHERE "t1"."id" = "t0"."post_id" AND "t1"."body" IS NULL)"#,
        ]
        .join(" ")
    );
}

#[test]
fn delete_helper_generates_expected_sql_for_not_null_filter() {
    let sql = delete! {
        crate::delete_schema,
        comment {
            where: {
                post: {
                    body: {
                        not: null
                    },
                },
            },
        }
    }
    .to_sql()
    .expect("sql generation should succeed");

    assert_eq!(
        sql,
        [
            r#"DELETE FROM "comment" AS "t0""#,
            r#"WHERE EXISTS (SELECT 1 FROM "post" AS "t1" WHERE "t1"."id" = "t0"."post_id" AND "t1"."body" IS NOT NULL)"#,
        ]
        .join(" ")
    );
}

#[test]
fn delete_many_generates_expected_sql_for_not_equal_filter() {
    let sql = crate::delete_schema::delete_many_with_variables::<DerivedDeleteCommentsByPostBody>(
        PostBodyVariables {
            excluded_body: "Bob published body".to_owned(),
        },
    )
    .to_sql()
    .expect("sql generation should succeed");

    assert_eq!(
        sql,
        [
            r#"DELETE FROM "comment" AS "t0""#,
            r#"WHERE EXISTS (SELECT 1 FROM "post" AS "t1" WHERE "t1"."id" = "t0"."post_id" AND "t1"."body" <> $1)"#,
        ]
        .join(" ")
    );
}

#[test]
fn delete_helper_generates_expected_sql_for_not_equal_filter() {
    let sql = delete! {
        crate::delete_schema,
        comment {
            where: {
                post: {
                    body: {
                        not: "Bob published body".to_owned()
                    },
                },
            },
        }
    }
    .to_sql()
    .expect("sql generation should succeed");

    assert_eq!(
        sql,
        [
            r#"DELETE FROM "comment" AS "t0""#,
            r#"WHERE EXISTS (SELECT 1 FROM "post" AS "t1" WHERE "t1"."id" = "t0"."post_id" AND "t1"."body" <> $1)"#,
        ]
        .join(" ")
    );
}
