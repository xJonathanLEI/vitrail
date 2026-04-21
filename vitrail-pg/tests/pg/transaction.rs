use crate::support::{TestDatabase, apply_schema};
use sqlx::postgres::PgPoolOptions;
use vitrail_pg::{
    DeleteMany, InsertInput, InsertResult, PostgresSchema, QueryResult, QueryVariables,
    TransactionIsolationLevel, TransactionOptions, UpdateData, UpdateMany, VitrailClient,
    VitrailTransaction, schema,
};

schema! {
    name transaction_schema

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
        published  Boolean
        author_id  Int
        created_at DateTime @default(now())
        author     user     @relation(fields: [author_id], references: [id])
    }
}

#[allow(dead_code)]
#[derive(InsertInput)]
#[vitrail(schema = crate::transaction::transaction_schema::Schema, model = user)]
struct NewUser {
    email: String,
    name: String,
}

#[allow(dead_code)]
#[derive(Debug, InsertResult)]
#[vitrail(schema = crate::transaction::transaction_schema::Schema, model = user, input = NewUser)]
struct InsertedUser {
    id: i64,
    email: String,
    name: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[allow(dead_code)]
#[derive(InsertInput)]
#[vitrail(schema = crate::transaction::transaction_schema::Schema, model = post)]
struct NewPost {
    title: String,
    published: bool,
    author_id: i64,
}

#[allow(dead_code)]
#[derive(Debug, InsertResult)]
#[vitrail(schema = crate::transaction::transaction_schema::Schema, model = post, input = NewPost)]
struct InsertedPost {
    id: i64,
    title: String,
    published: bool,
    author_id: i64,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(QueryResult)]
#[vitrail(
    schema = crate::transaction::transaction_schema::Schema,
    model = user,
    variables = UserEmailVariables,
    where(email = eq(email))
)]
struct UserByEmail {
    id: i64,
    email: String,
    name: String,
}

#[derive(QueryResult)]
#[vitrail(schema = crate::transaction::transaction_schema::Schema, model = post)]
struct PostRecord {
    id: i64,
    title: String,
    published: bool,
    author_id: i64,
}

#[derive(QueryVariables)]
struct UserEmailVariables {
    email: String,
}

#[derive(QueryVariables)]
struct AuthorAndPublishedVariables {
    author_id: i64,
    was_published: bool,
}

#[derive(UpdateData)]
#[vitrail(schema = crate::transaction::transaction_schema::Schema, model = post)]
struct PublishPostsData {
    published: bool,
}

#[derive(UpdateMany)]
#[vitrail(
    schema = crate::transaction::transaction_schema::Schema,
    model = post,
    data = PublishPostsData,
    variables = AuthorAndPublishedVariables,
    where(author_id = eq(author_id)),
    where(published = eq(was_published))
)]
struct PublishPostsByAuthorAndPublished;

#[derive(QueryVariables)]
struct PostTitleVariables {
    title: String,
}

#[derive(DeleteMany)]
#[vitrail(
    schema = crate::transaction::transaction_schema::Schema,
    model = post,
    variables = PostTitleVariables,
    where(title = eq(title))
)]
struct DeletePostsByTitle;

async fn setup_database(database_url: &str) {
    apply_schema(
        database_url,
        &PostgresSchema::from_schema_access::<crate::transaction::transaction_schema::Schema>(),
    )
    .await;
}

async fn table_row_count(database_url: &str, table_name: &str) -> i64 {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
        .expect("should connect to postgres");

    let sql = format!(r#"SELECT COUNT(*)::bigint FROM "{table_name}""#);
    let count: i64 = sqlx::query_scalar(&sql)
        .fetch_one(&pool)
        .await
        .expect("should count rows");

    pool.close().await;
    count
}

async fn insert_user(
    txn: &VitrailTransaction,
    email: &str,
    name: &str,
) -> Result<InsertedUser, sqlx::Error> {
    txn.insert(
        crate::transaction::transaction_schema::insert::<InsertedUser>(NewUser {
            email: email.to_owned(),
            name: name.to_owned(),
        }),
    )
    .await
}

async fn insert_post(
    txn: &VitrailTransaction,
    title: &str,
    published: bool,
    author_id: i64,
) -> Result<InsertedPost, sqlx::Error> {
    txn.insert(
        crate::transaction::transaction_schema::insert::<InsertedPost>(NewPost {
            title: title.to_owned(),
            published,
            author_id,
        }),
    )
    .await
}

async fn create_user_with_post(
    txn: &VitrailTransaction,
    email: &str,
    name: &str,
    title: &str,
) -> Result<(InsertedUser, InsertedPost), sqlx::Error> {
    let user = insert_user(txn, email, name).await?;
    let post = insert_post(txn, title, false, user.id).await?;
    Ok((user, post))
}

#[tokio::test]
async fn transaction_commit_persists_writes() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();

    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let txn = client.begin().await.expect("should begin transaction");

    let user = insert_user(&txn, "alice@example.com", "Alice")
        .await
        .expect("user insert should succeed");

    let post = insert_post(&txn, "Committed post", false, user.id)
        .await
        .expect("post insert should succeed");

    txn.commit().await.expect("commit should succeed");

    assert_eq!(table_row_count(&database_url, "user").await, 1);
    assert_eq!(table_row_count(&database_url, "post").await, 1);

    let persisted_user = client
        .find_first(
            crate::transaction::transaction_schema::query_with_variables::<UserByEmail>(
                UserEmailVariables {
                    email: "alice@example.com".to_owned(),
                },
            ),
        )
        .await
        .expect("committed user should be queryable");

    assert_eq!(persisted_user.id, user.id);
    assert_eq!(persisted_user.email, "alice@example.com");
    assert_eq!(persisted_user.name, "Alice");
    assert_eq!(post.title, "Committed post");

    client.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn transaction_rollback_discards_writes() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();

    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let txn = client.begin().await.expect("should begin transaction");

    insert_user(&txn, "bob@example.com", "Bob")
        .await
        .expect("user insert should succeed");

    txn.rollback().await.expect("rollback should succeed");

    assert_eq!(table_row_count(&database_url, "user").await, 0);
    assert_eq!(table_row_count(&database_url, "post").await, 0);

    client.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn dropped_transaction_rolls_back_uncommitted_writes() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();

    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    {
        let txn = client.begin().await.expect("should begin transaction");

        insert_user(&txn, "carol@example.com", "Carol")
            .await
            .expect("user insert should succeed");
    }

    assert_eq!(table_row_count(&database_url, "user").await, 0);

    client.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn transaction_reads_can_see_prior_writes_before_commit() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();

    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let txn = client.begin().await.expect("should begin transaction");

    let inserted = insert_user(&txn, "diana@example.com", "Diana")
        .await
        .expect("user insert should succeed");

    let queried = txn
        .find_first(
            crate::transaction::transaction_schema::query_with_variables::<UserByEmail>(
                UserEmailVariables {
                    email: "diana@example.com".to_owned(),
                },
            ),
        )
        .await
        .expect("transaction should read its own writes");

    assert_eq!(queried.id, inserted.id);
    assert_eq!(queried.email, "diana@example.com");
    assert_eq!(queried.name, "Diana");

    txn.rollback().await.expect("rollback should succeed");

    client.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn outside_client_cannot_see_uncommitted_transaction_writes() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();

    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let txn = client.begin().await.expect("should begin transaction");

    insert_user(&txn, "eve@example.com", "Eve")
        .await
        .expect("user insert should succeed");

    let before_commit = client
        .find_optional(
            crate::transaction::transaction_schema::query_with_variables::<UserByEmail>(
                UserEmailVariables {
                    email: "eve@example.com".to_owned(),
                },
            ),
        )
        .await
        .expect("outside query should succeed");

    assert!(before_commit.is_none());

    txn.commit().await.expect("commit should succeed");

    let after_commit = client
        .find_optional(
            crate::transaction::transaction_schema::query_with_variables::<UserByEmail>(
                UserEmailVariables {
                    email: "eve@example.com".to_owned(),
                },
            ),
        )
        .await
        .expect("outside query should succeed");

    let user = after_commit.expect("committed user should now be visible");
    assert_eq!(user.email, "eve@example.com");
    assert_eq!(user.name, "Eve");

    client.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn transaction_update_and_delete_work_and_can_be_verified_before_commit() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();

    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let seed_txn = client.begin().await.expect("should begin seed transaction");
    let author = insert_user(&seed_txn, "frank@example.com", "Frank")
        .await
        .expect("author insert should succeed");
    insert_post(&seed_txn, "Draft 1", false, author.id)
        .await
        .expect("first post insert should succeed");
    insert_post(&seed_txn, "Draft 2", false, author.id)
        .await
        .expect("second post insert should succeed");
    insert_post(&seed_txn, "Archived", true, author.id)
        .await
        .expect("third post insert should succeed");
    seed_txn.commit().await.expect("seed commit should succeed");

    let txn = client.begin().await.expect("should begin transaction");

    let updated = txn
        .update_many(
            crate::transaction::transaction_schema::update_many_with_variables::<
                PublishPostsByAuthorAndPublished,
            >(
                AuthorAndPublishedVariables {
                    author_id: author.id,
                    was_published: false,
                },
                PublishPostsData { published: true },
            ),
        )
        .await
        .expect("update should succeed");

    let deleted = txn
        .delete_many(
            crate::transaction::transaction_schema::delete_many_with_variables::<DeletePostsByTitle>(
                PostTitleVariables {
                    title: "Archived".to_owned(),
                },
            ),
        )
        .await
        .expect("delete should succeed");

    assert_eq!(updated, 2);
    assert_eq!(deleted, 1);

    let posts_in_txn = txn
        .find_many(crate::transaction::transaction_schema::query::<PostRecord>())
        .await
        .expect("transaction query should succeed");

    assert_eq!(posts_in_txn.len(), 2);
    assert!(posts_in_txn.iter().all(|post| post.published));
    assert!(posts_in_txn.iter().all(|post| post.author_id == author.id));
    let mut post_titles = posts_in_txn
        .iter()
        .map(|post| post.title.as_str())
        .collect::<Vec<_>>();
    post_titles.sort_unstable();
    assert_eq!(post_titles, vec!["Draft 1", "Draft 2"]);

    txn.commit().await.expect("commit should succeed");

    let posts_after_commit = client
        .find_many(crate::transaction::transaction_schema::query::<PostRecord>())
        .await
        .expect("post query should succeed");

    assert_eq!(posts_after_commit.len(), 2);
    assert!(posts_after_commit.iter().all(|post| post.published));

    let mut post_ids_in_txn = posts_in_txn.iter().map(|post| post.id).collect::<Vec<_>>();
    post_ids_in_txn.sort_unstable();

    let mut post_ids_after_commit = posts_after_commit
        .iter()
        .map(|post| post.id)
        .collect::<Vec<_>>();
    post_ids_after_commit.sort_unstable();

    assert_eq!(post_ids_after_commit, post_ids_in_txn);

    client.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn transaction_can_be_passed_through_helper_functions() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();

    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let txn = client.begin().await.expect("should begin transaction");

    let (user, post) =
        create_user_with_post(&txn, "grace@example.com", "Grace", "Created through helper")
            .await
            .expect("helper workflow should succeed");

    txn.commit().await.expect("commit should succeed");

    let persisted_user = client
        .find_first(
            crate::transaction::transaction_schema::query_with_variables::<UserByEmail>(
                UserEmailVariables {
                    email: "grace@example.com".to_owned(),
                },
            ),
        )
        .await
        .expect("persisted user should be queryable");

    let posts = client
        .find_many(crate::transaction::transaction_schema::query::<PostRecord>())
        .await
        .expect("post query should succeed");

    assert_eq!(persisted_user.id, user.id);
    assert_eq!(persisted_user.name, "Grace");
    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0].title, "Created through helper");
    assert_eq!(posts[0].author_id, user.id);
    assert_eq!(post.id, posts[0].id);

    client.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn transaction_begin_with_serializable_isolation_level_succeeds() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();

    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let txn = client
        .begin_with_options(TransactionOptions {
            isolation_level: Some(TransactionIsolationLevel::Serializable),
        })
        .await
        .expect("should begin transaction with explicit isolation level");

    let inserted = insert_user(&txn, "henry@example.com", "Henry")
        .await
        .expect("user insert should succeed");

    let queried = txn
        .find_first(
            crate::transaction::transaction_schema::query_with_variables::<UserByEmail>(
                UserEmailVariables {
                    email: "henry@example.com".to_owned(),
                },
            ),
        )
        .await
        .expect("transaction should query inserted user");

    assert_eq!(queried.id, inserted.id);
    assert_eq!(queried.email, "henry@example.com");
    assert_eq!(queried.name, "Henry");

    txn.commit().await.expect("commit should succeed");

    let persisted = client
        .find_first(
            crate::transaction::transaction_schema::query_with_variables::<UserByEmail>(
                UserEmailVariables {
                    email: "henry@example.com".to_owned(),
                },
            ),
        )
        .await
        .expect("committed user should be queryable");

    assert_eq!(persisted.id, inserted.id);
    assert_eq!(persisted.email, "henry@example.com");
    assert_eq!(persisted.name, "Henry");

    client.close().await;
    database.cleanup().await;
}
