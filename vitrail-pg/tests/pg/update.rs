use crate::support::{TestDatabase, apply_schema};
use sqlx::postgres::PgPoolOptions;
use vitrail_pg::{
    PostgresSchema, QueryFilter, QueryFilterValue, QueryVariableValue, QueryVariables, UpdateMany,
    UpdateManyModel, UpdateValue, UpdateValues, VitrailClient, schema,
};

schema! {
    name update_schema

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

pub(crate) use self::update_schema as pg_update_schema;

struct PublishUnpublishedPosts;

impl UpdateManyModel for PublishUnpublishedPosts {
    type Schema = crate::update_schema::Schema;
    type Values = UpdateValues;
    type Variables = ();

    fn model_name() -> &'static str {
        "post"
    }

    fn filter() -> Option<QueryFilter> {
        Some(QueryFilter::eq("published", false))
    }
}

struct PublishPostsByAuthorAge;

impl UpdateManyModel for PublishPostsByAuthorAge {
    type Schema = crate::update_schema::Schema;
    type Values = UpdateValues;
    type Variables = QueryVariables;

    fn model_name() -> &'static str {
        "post"
    }

    fn filter_with_variables(_variables: &QueryVariables) -> Option<QueryFilter> {
        Some(QueryFilter::relation(
            "author",
            QueryFilter::eq("age", QueryFilterValue::variable("author_age")),
        ))
    }
}

struct ReviewCommentsByPostAuthorAge;

impl UpdateManyModel for ReviewCommentsByPostAuthorAge {
    type Schema = crate::update_schema::Schema;
    type Values = UpdateValues;
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
        &PostgresSchema::from_schema_access::<crate::update_schema::Schema>(),
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
        INSERT INTO "post" ("title", "published", "author_id")
        VALUES ('Alice draft 1', false, $1)
        RETURNING "id"::bigint
        "#,
    )
    .bind(alice_id)
    .fetch_one(&pool)
    .await
    .expect("should insert alice post 1");

    let alice_post_2: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO "post" ("title", "published", "author_id")
        VALUES ('Alice draft 2', false, $1)
        RETURNING "id"::bigint
        "#,
    )
    .bind(alice_id)
    .fetch_one(&pool)
    .await
    .expect("should insert alice post 2");

    let bob_post: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO "post" ("title", "published", "author_id")
        VALUES ('Bob already published', true, $1)
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
async fn update_many_updates_multiple_rows_and_returns_count() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();
    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let count = client
        .update_many(UpdateMany::<
            crate::update_schema::Schema,
            PublishUnpublishedPosts,
        >::new(UpdateValues::from_values(vec![(
            "published",
            UpdateValue::from(true),
        )])))
        .await
        .expect("update should succeed");

    assert_eq!(count, 2);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("should connect to postgres");

    let published_count: i64 =
        sqlx::query_scalar(r#"SELECT COUNT(*)::bigint FROM "post" WHERE "published" = true"#)
            .fetch_one(&pool)
            .await
            .expect("should count published posts");

    assert_eq!(published_count, 3);

    pool.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn update_many_supports_nested_to_one_relation_filters() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();
    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let count = client
        .update_many(UpdateMany::<
            crate::update_schema::Schema,
            PublishPostsByAuthorAge,
        >::new_with_variables(
            QueryVariables::from_values(vec![("author_age", QueryVariableValue::from(35_i64))]),
            UpdateValues::from_values(vec![("published", UpdateValue::from(true))]),
        ))
        .await
        .expect("update should succeed");

    assert_eq!(count, 2);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("should connect to postgres");

    let alice_published: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::bigint
        FROM "post"
        WHERE "published" = true
          AND "author_id" = (SELECT "id" FROM "user" WHERE "email" = 'alice@example.com')
        "#,
    )
    .fetch_one(&pool)
    .await
    .expect("should count alice posts");

    let bob_published: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::bigint
        FROM "post"
        WHERE "published" = true
          AND "author_id" = (SELECT "id" FROM "user" WHERE "email" = 'bob@example.com')
        "#,
    )
    .fetch_one(&pool)
    .await
    .expect("should count bob posts");

    assert_eq!(alice_published, 2);
    assert_eq!(bob_published, 1);

    pool.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn update_many_supports_deeply_nested_relation_filters() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();
    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let count = client
        .update_many(UpdateMany::<
            crate::update_schema::Schema,
            ReviewCommentsByPostAuthorAge,
        >::new_with_variables(
            QueryVariables::from_values(vec![("author_age", QueryVariableValue::from(35_i64))]),
            UpdateValues::from_values(vec![("reviewed", UpdateValue::from(true))]),
        ))
        .await
        .expect("update should succeed");

    assert_eq!(count, 2);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("should connect to postgres");

    let reviewed_comments: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::bigint
        FROM "comment"
        WHERE "reviewed" = true
        "#,
    )
    .fetch_one(&pool)
    .await
    .expect("should count reviewed comments");

    let bob_reviewed: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::bigint
        FROM "comment"
        WHERE "reviewed" = true
          AND "post_id" IN (
            SELECT "id"
            FROM "post"
            WHERE "author_id" = (SELECT "id" FROM "user" WHERE "email" = 'bob@example.com')
          )
        "#,
    )
    .fetch_one(&pool)
    .await
    .expect("should count bob reviewed comments");

    assert_eq!(reviewed_comments, 2);
    assert_eq!(bob_reviewed, 0);

    pool.close().await;
    database.cleanup().await;
}

#[test]
fn update_many_generates_expected_sql_for_nested_relation_filter() {
    let sql =
        UpdateMany::<crate::update_schema::Schema, PublishPostsByAuthorAge>::new_with_variables(
            QueryVariables::from_values(vec![("author_age", QueryVariableValue::from(35_i64))]),
            UpdateValues::from_values(vec![("published", UpdateValue::from(true))]),
        )
        .to_sql()
        .expect("sql generation should succeed");

    assert_eq!(
        sql,
        [
            r#"UPDATE "post" AS "t0""#,
            r#"SET "published" = $1"#,
            r#"WHERE EXISTS (SELECT 1 FROM "user" AS "t1" WHERE "t1"."id" = "t0"."author_id" AND ("t1"."age")::bigint = $2)"#,
        ]
        .join(" ")
    );
}

#[test]
fn update_many_rejects_relation_field_write() {
    let error = UpdateMany::<crate::update_schema::Schema, PublishUnpublishedPosts>::with_values(
        UpdateValues::from_values(vec![("author", UpdateValue::from(1_i64))]),
    )
    .to_sql()
    .expect_err("update should fail");

    assert!(
        error
            .to_string()
            .contains("relation field `author` cannot be written in update for model `post`")
    );
}

#[test]
fn update_many_rejects_empty_update_payload() {
    let error = UpdateMany::<crate::update_schema::Schema, PublishUnpublishedPosts>::with_values(
        UpdateValues::new(),
    )
    .to_sql()
    .expect_err("update should fail");

    assert!(
        error
            .to_string()
            .contains("update on model `post` must write at least one scalar field")
    );
}
