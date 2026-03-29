use crate::support::{TestDatabase, apply_schema};
use sqlx::Row as _;
use sqlx::postgres::PgPoolOptions;
use vitrail_pg::{
    Insert, InsertInput, InsertModel, InsertResult, InsertValue, InsertValueSet, InsertValues,
    PostgresSchema, VitrailClient, alias_name, insert, row_as_datetime_utc, schema,
};

schema! {
    name insert_schema

    model user {
        id         Int      @id @default(autoincrement())
        email      String   @unique
        name       String
        created_at DateTime @default(now())
        posts      post[]
    }

    model post {
        id         Int           @id @default(autoincrement())
        title      String
        body       String?
        published  Boolean
        author_id  Int
        created_at DateTime      @default(now())
        author     user          @relation(fields: [author_id], references: [id])
        locales    post_locale[]
    }

    model post_locale {
        id      Int    @id @default(autoincrement())
        post_id Int
        locale  String
        title   String
        post    post   @relation(fields: [post_id], references: [id])

        @@unique([post_id, locale])
    }
}

pub(crate) use self::insert_schema as pg_insert_schema;

struct NewUserValues {
    email: String,
    name: String,
}

impl InsertValueSet for NewUserValues {
    fn into_insert_values(self) -> InsertValues {
        InsertValues::from_values(vec![
            ("email", InsertValue::from(self.email)),
            ("name", InsertValue::from(self.name)),
        ])
    }
}

struct NewPostValues {
    title: String,
    body: Option<String>,
    published: bool,
    author_id: i64,
}

impl InsertValueSet for NewPostValues {
    fn into_insert_values(self) -> InsertValues {
        let mut values = InsertValues::new();
        values
            .push("title", self.title.into())
            .expect("title should be unique");
        values
            .push("published", self.published.into())
            .expect("published should be unique");
        values
            .push("author_id", self.author_id.into())
            .expect("author_id should be unique");

        if let Some(body) = self.body {
            values
                .push("body", body.into())
                .expect("body should be unique");
        }

        values
    }
}

#[derive(Debug)]
struct InsertedUser {
    id: i64,
    email: String,
    name: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

impl InsertModel for InsertedUser {
    type Schema = crate::insert::insert_schema::Schema;
    type Values = NewUserValues;

    fn model_name() -> &'static str {
        "user"
    }

    fn returning_fields() -> &'static [&'static str] {
        &["id", "email", "name", "created_at"]
    }

    fn from_row(row: &sqlx::postgres::PgRow, prefix: &str) -> Result<Self, sqlx::Error> {
        let id_alias = alias_name(prefix, "id");
        let email_alias = alias_name(prefix, "email");
        let name_alias = alias_name(prefix, "name");
        let created_at_alias = alias_name(prefix, "created_at");

        Ok(Self {
            id: row.try_get(id_alias.as_str())?,
            email: row.try_get(email_alias.as_str())?,
            name: row.try_get(name_alias.as_str())?,
            created_at: row_as_datetime_utc(row, created_at_alias.as_str())?,
        })
    }
}

#[derive(Debug)]
struct InsertedPost {
    id: i64,
    title: String,
    body: Option<String>,
    published: bool,
    author_id: i64,
    created_at: chrono::DateTime<chrono::Utc>,
}

impl InsertModel for InsertedPost {
    type Schema = crate::insert::insert_schema::Schema;
    type Values = NewPostValues;

    fn model_name() -> &'static str {
        "post"
    }

    fn returning_fields() -> &'static [&'static str] {
        &[
            "id",
            "title",
            "body",
            "published",
            "author_id",
            "created_at",
        ]
    }

    fn from_row(row: &sqlx::postgres::PgRow, prefix: &str) -> Result<Self, sqlx::Error> {
        let id_alias = alias_name(prefix, "id");
        let title_alias = alias_name(prefix, "title");
        let body_alias = alias_name(prefix, "body");
        let published_alias = alias_name(prefix, "published");
        let author_id_alias = alias_name(prefix, "author_id");
        let created_at_alias = alias_name(prefix, "created_at");

        Ok(Self {
            id: row.try_get(id_alias.as_str())?,
            title: row.try_get(title_alias.as_str())?,
            body: row.try_get(body_alias.as_str())?,
            published: row.try_get(published_alias.as_str())?,
            author_id: row.try_get(author_id_alias.as_str())?,
            created_at: row_as_datetime_utc(row, created_at_alias.as_str())?,
        })
    }
}

#[allow(dead_code)]
#[derive(InsertInput)]
#[vitrail(schema = crate::insert::insert_schema::Schema, model = user)]
struct DerivedNewUser {
    email: String,
    name: String,
}

#[allow(dead_code)]
#[derive(Debug, InsertResult)]
#[vitrail(schema = crate::insert::insert_schema::Schema, model = user, input = DerivedNewUser)]
struct DerivedInsertedUser {
    id: i64,
    email: String,
    name: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[allow(dead_code)]
#[derive(InsertInput)]
#[vitrail(schema = crate::insert::insert_schema::Schema, model = post)]
struct DerivedNewPost {
    title: String,
    body: Option<String>,
    published: bool,
    author_id: i64,
}

#[allow(dead_code)]
#[derive(Debug, InsertResult)]
#[vitrail(schema = crate::insert::insert_schema::Schema, model = post, input = DerivedNewPost)]
struct DerivedInsertedPost {
    id: i64,
    title: String,
    body: Option<String>,
    published: bool,
    author_id: i64,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[tokio::test]
async fn derived_scalar_insert_returns_generated_fields_on_postgres() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();

    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let user = client
        .insert(crate::insert_schema::insert::<DerivedInsertedUser>(
            DerivedNewUser {
                email: "bob@example.com".to_owned(),
                name: "Bob".to_owned(),
            },
        ))
        .await
        .expect("insert should succeed");

    assert!(user.id > 0, "generated id should be returned");
    assert_eq!(user.email, "bob@example.com");
    assert_eq!(user.name, "Bob");
    assert!(user.created_at <= chrono::Utc::now());

    client.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn derived_scalar_insert_nullable_field_round_trips_as_null_on_postgres() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();

    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let author = client
        .insert(crate::insert_schema::insert::<DerivedInsertedUser>(
            DerivedNewUser {
                email: "charlie@example.com".to_owned(),
                name: "Charlie".to_owned(),
            },
        ))
        .await
        .expect("author insert should succeed");

    let post = client
        .insert(crate::insert_schema::insert::<DerivedInsertedPost>(
            DerivedNewPost {
                title: "Hello from derive".to_owned(),
                body: None,
                published: true,
                author_id: author.id,
            },
        ))
        .await
        .expect("post insert should succeed");

    assert_eq!(post.body, None);
    assert_eq!(post.author_id, author.id);

    client.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn helper_scalar_insert_defaults_to_all_scalar_fields_on_postgres() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();

    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let user = client
        .insert(insert! {
            crate::insert_schema,
            user {
                data: {
                    email: "dana@example.com".to_owned(),
                    name: "Dana".to_owned(),
                },
            }
        })
        .await
        .expect("insert should succeed");

    assert!(user.id > 0, "generated id should be returned");
    assert_eq!(user.email, "dana@example.com");
    assert_eq!(user.name, "Dana");
    assert!(user.created_at <= chrono::Utc::now());

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("should connect to postgres");

    let stored = sqlx::query_as::<_, (i64, String, String, chrono::NaiveDateTime)>(
        r#"
        SELECT "id"::bigint, "email", "name", "created_at"
        FROM "user"
        WHERE "id" = $1
        "#,
    )
    .bind(user.id)
    .fetch_one(&pool)
    .await
    .expect("should fetch inserted user");

    assert_eq!(stored.0, user.id);
    assert_eq!(stored.1, user.email);
    assert_eq!(stored.2, user.name);
    assert_eq!(stored.3.and_utc(), user.created_at);

    pool.close().await;
    client.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn helper_scalar_insert_nullable_field_round_trips_as_null_on_postgres() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();

    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let author = client
        .insert(insert! {
            crate::insert_schema,
            user {
                data: {
                    email: "eve@example.com".to_owned(),
                    name: "Eve".to_owned(),
                },
                select: {
                    id: true,
                },
            }
        })
        .await
        .expect("author insert should succeed");

    let post = client
        .insert(insert! {
            crate::insert_schema,
            post {
                data: {
                    title: "Hello from helper".to_owned(),
                    body: None,
                    published: true,
                    author_id: author.id,
                },
                select: {
                    id: true,
                    title: true,
                    body: true,
                    author_id: true,
                },
            }
        })
        .await
        .expect("post insert should succeed");

    assert!(post.id > 0, "generated id should be returned");
    assert_eq!(post.title, "Hello from helper");
    assert_eq!(post.body, None);
    assert_eq!(post.author_id, author.id);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("should connect to postgres");

    let stored = sqlx::query_as::<_, (i64, Option<String>, i64)>(
        r#"
        SELECT "id"::bigint, "body", "author_id"::bigint
        FROM "post"
        WHERE "id" = $1
        "#,
    )
    .bind(post.id)
    .fetch_one(&pool)
    .await
    .expect("should fetch inserted post");

    assert_eq!(stored.0, post.id);
    assert_eq!(stored.1, post.body);
    assert_eq!(stored.2, post.author_id);

    pool.close().await;
    client.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn duplicate_insert_violates_composite_unique_constraint_on_postgres() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();

    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let user = client
        .insert(insert! {
            crate::insert_schema,
            user {
                data: {
                    email: "frank@example.com".to_owned(),
                    name: "Frank".to_owned(),
                },
                select: {
                    id: true,
                },
            }
        })
        .await
        .expect("user insert should succeed");

    let post = client
        .insert(insert! {
            crate::insert_schema,
            post {
                data: {
                    title: "Composite unique".to_owned(),
                    body: None,
                    published: true,
                    author_id: user.id,
                },
                select: {
                    id: true,
                },
            }
        })
        .await
        .expect("post insert should succeed");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("should connect to postgres");

    sqlx::query(
        r#"
        INSERT INTO "post_locale" ("post_id", "locale", "title")
        VALUES ($1, $2, $3)
        "#,
    )
    .bind(post.id)
    .bind("en")
    .bind("Hello")
    .execute(&pool)
    .await
    .expect("first insert should succeed");

    let error = sqlx::query(
        r#"
        INSERT INTO "post_locale" ("post_id", "locale", "title")
        VALUES ($1, $2, $3)
        "#,
    )
    .bind(post.id)
    .bind("en")
    .bind("Hello again")
    .execute(&pool)
    .await
    .expect_err("duplicate insert should fail");

    let database_error = error
        .as_database_error()
        .expect("duplicate insert should return a database error");

    assert_eq!(database_error.code().as_deref(), Some("23505"));
    assert!(
        database_error
            .message()
            .contains("post_locale_post_id_locale_key"),
        "unexpected database error: {database_error}",
    );

    pool.close().await;
    client.close().await;
    database.cleanup().await;
}

#[test]
fn scalar_insert_rejects_missing_required_field() {
    let sql = Insert::<crate::insert_schema::Schema, InsertedUser>::with_values(
        InsertValues::from_values(vec![("email", InsertValue::from("alice@example.com"))]),
    )
    .to_sql();

    let error = sql.expect_err("insert should fail");
    assert!(
        error
            .to_string()
            .contains("missing required scalar field `name`"),
        "unexpected error: {error}",
    );
}

#[test]
fn scalar_insert_rejects_relation_field_write() {
    let sql = Insert::<crate::insert_schema::Schema, InsertedPost>::with_values(
        InsertValues::from_values(vec![
            ("title", InsertValue::from("Hello from Vitrail")),
            ("published", InsertValue::from(true)),
            ("author_id", InsertValue::from(7_i64)),
            ("author", InsertValue::from(7_i64)),
        ]),
    )
    .to_sql();

    let error = sql.expect_err("insert should fail");
    assert!(
        error
            .to_string()
            .contains("relation field `author` cannot be written"),
        "unexpected error: {error}",
    );
}

async fn setup_database(database_url: &str) {
    apply_schema(
        database_url,
        &PostgresSchema::from_schema_access::<crate::insert_schema::Schema>(),
    )
    .await;
}

#[tokio::test]
async fn manual_scalar_insert_returns_generated_fields_on_postgres() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();

    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let user = client
        .insert(Insert::<crate::insert_schema::Schema, InsertedUser>::new(
            NewUserValues {
                email: "alice@example.com".to_owned(),
                name: "Alice".to_owned(),
            },
        ))
        .await
        .expect("insert should succeed");

    assert!(user.id > 0, "generated id should be returned");
    assert_eq!(user.email, "alice@example.com");
    assert_eq!(user.name, "Alice");

    let now = chrono::Utc::now();
    assert!(
        user.created_at <= now,
        "database-generated created_at should not be in the future"
    );

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("should connect to postgres");

    let stored = sqlx::query_as::<_, (i64, String, String, chrono::NaiveDateTime)>(
        r#"
        SELECT "id"::bigint, "email", "name", "created_at"
        FROM "user"
        WHERE "id" = $1
        "#,
    )
    .bind(user.id)
    .fetch_one(&pool)
    .await
    .expect("should fetch inserted user");

    assert_eq!(stored.0, user.id);
    assert_eq!(stored.1, user.email);
    assert_eq!(stored.2, user.name);
    assert_eq!(stored.3.and_utc(), user.created_at);

    pool.close().await;
    client.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn manual_scalar_insert_omitting_optional_field_round_trips_as_null_on_postgres() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();

    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let author = client
        .insert(Insert::<crate::insert_schema::Schema, InsertedUser>::new(
            NewUserValues {
                email: "alice@example.com".to_owned(),
                name: "Alice".to_owned(),
            },
        ))
        .await
        .expect("author insert should succeed");

    let post = client
        .insert(Insert::<crate::insert_schema::Schema, InsertedPost>::new(
            NewPostValues {
                title: "Hello from Vitrail".to_owned(),
                body: None,
                published: true,
                author_id: author.id,
            },
        ))
        .await
        .expect("post insert should succeed");

    assert!(post.id > 0, "generated id should be returned");
    assert_eq!(post.title, "Hello from Vitrail");
    assert_eq!(post.body, None);
    assert!(post.published);
    assert_eq!(post.author_id, author.id);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("should connect to postgres");

    let stored = sqlx::query_as::<_, (i64, Option<String>, bool, i64, chrono::NaiveDateTime)>(
        r#"
        SELECT "id"::bigint, "body", "published", "author_id"::bigint, "created_at"
        FROM "post"
        WHERE "id" = $1
        "#,
    )
    .bind(post.id)
    .fetch_one(&pool)
    .await
    .expect("should fetch inserted post");

    assert_eq!(stored.0, post.id);
    assert_eq!(stored.1, None);
    assert_eq!(stored.2, post.published);
    assert_eq!(stored.3, post.author_id);
    assert_eq!(stored.4.and_utc(), post.created_at);

    pool.close().await;
    client.close().await;
    database.cleanup().await;
}
