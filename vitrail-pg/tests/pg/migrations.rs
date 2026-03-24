use crate::support::{TestDatabase, apply_sql_script};
use vitrail_pg::{PostgresSchema, schema};

const EMPTY_TO_BASE_SQL: &str = r#"-- CreateTable
CREATE TABLE "user" (
    "id" SERIAL NOT NULL,
    "email" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "user_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "post" (
    "id" SERIAL NOT NULL,
    "title" TEXT NOT NULL,
    "body" TEXT,
    "published" BOOLEAN NOT NULL,
    "author_id" INTEGER NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "post_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "user_email_key" ON "user"("email");

-- AddForeignKey
ALTER TABLE "post" ADD CONSTRAINT "post_author_id_fkey" FOREIGN KEY ("author_id") REFERENCES "user"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
"#;
const BASE_TO_EXPANDED_SQL: &str = r#"-- AlterTable
ALTER TABLE "post" ADD COLUMN     "updated_at" TIMESTAMP(3);

-- CreateTable
CREATE TABLE "comment" (
    "id" SERIAL NOT NULL,
    "body" TEXT NOT NULL,
    "post_id" INTEGER NOT NULL,

    CONSTRAINT "comment_pkey" PRIMARY KEY ("id")
);

-- AddForeignKey
ALTER TABLE "comment" ADD CONSTRAINT "comment_post_id_fkey" FOREIGN KEY ("post_id") REFERENCES "post"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
"#;
const EMPTY_TO_EXPANDED_SQL: &str = r#"-- CreateTable
CREATE TABLE "user" (
    "id" SERIAL NOT NULL,
    "email" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "user_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "post" (
    "id" SERIAL NOT NULL,
    "title" TEXT NOT NULL,
    "body" TEXT,
    "published" BOOLEAN NOT NULL,
    "author_id" INTEGER NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3),

    CONSTRAINT "post_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "comment" (
    "id" SERIAL NOT NULL,
    "body" TEXT NOT NULL,
    "post_id" INTEGER NOT NULL,

    CONSTRAINT "comment_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "user_email_key" ON "user"("email");

-- AddForeignKey
ALTER TABLE "post" ADD CONSTRAINT "post_author_id_fkey" FOREIGN KEY ("author_id") REFERENCES "user"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "comment" ADD CONSTRAINT "comment_post_id_fkey" FOREIGN KEY ("post_id") REFERENCES "post"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
"#;

schema! {
    name base_schema

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
    }
}

schema! {
    name expanded_schema

    model user {
        id         Int      @id @default(autoincrement())
        email      String   @unique
        name       String
        created_at DateTime @default(now())
        posts      post[]
    }

    model post {
        id         Int       @id @default(autoincrement())
        title      String
        body       String?
        published  Boolean
        author_id  Int
        created_at DateTime  @default(now())
        updated_at DateTime?
        author     user      @relation(fields: [author_id], references: [id])
        comments   comment[]
    }

    model comment {
        id      Int    @id @default(autoincrement())
        body    String
        post_id Int
        post    post   @relation(fields: [post_id], references: [id])
    }
}

fn empty_database_schema() -> PostgresSchema {
    PostgresSchema::empty()
}

fn base_database_schema() -> PostgresSchema {
    PostgresSchema::from_schema_access::<base_schema::Schema>()
}

fn expanded_database_schema() -> PostgresSchema {
    PostgresSchema::from_schema_access::<expanded_schema::Schema>()
}

#[test]
fn empty_to_base_direct_diff_matches_expected_sql() {
    let sql = base_database_schema()
        .migrate_from(&empty_database_schema())
        .to_sql();
    assert_eq!(normalize_sql(&sql), normalize_sql(EMPTY_TO_BASE_SQL));
}

#[test]
fn base_to_expanded_direct_diff_matches_expected_sql() {
    let sql = expanded_database_schema()
        .migrate_from(&base_database_schema())
        .to_sql();
    assert_eq!(normalize_sql(&sql), normalize_sql(BASE_TO_EXPANDED_SQL));
}

#[test]
fn empty_to_expanded_direct_diff_matches_expected_sql() {
    let sql = expanded_database_schema()
        .migrate_from(&empty_database_schema())
        .to_sql();
    assert_eq!(normalize_sql(&sql), normalize_sql(EMPTY_TO_EXPANDED_SQL));
}

#[tokio::test]
async fn generated_migration_brings_empty_database_to_base_schema() {
    assert_generated_migration_roundtrips(
        &empty_database_schema(),
        &base_database_schema(),
        EMPTY_TO_BASE_SQL,
    )
    .await;
}

#[tokio::test]
async fn generated_migration_brings_base_database_to_expanded_schema() {
    assert_generated_migration_roundtrips(
        &base_database_schema(),
        &expanded_database_schema(),
        BASE_TO_EXPANDED_SQL,
    )
    .await;
}

#[tokio::test]
async fn generated_migration_brings_empty_database_to_expanded_schema() {
    assert_generated_migration_roundtrips(
        &empty_database_schema(),
        &expanded_database_schema(),
        EMPTY_TO_EXPANDED_SQL,
    )
    .await;
}

async fn assert_generated_migration_roundtrips(
    start: &PostgresSchema,
    target: &PostgresSchema,
    expected_sql: &str,
) {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();

    if !start.migrate_from(&PostgresSchema::empty()).is_empty() {
        let bootstrap_sql = start.migrate_from(&PostgresSchema::empty()).to_sql();
        apply_sql_script(&database_url, &bootstrap_sql).await;
    }

    let current = PostgresSchema::introspect(&database_url)
        .await
        .expect("should introspect current postgres schema");
    let migration = target.migrate_from(&current);

    let migration_sql = migration.to_sql();

    assert_eq!(normalize_sql(&migration_sql), normalize_sql(expected_sql));

    apply_sql_script(&database_url, &migration_sql).await;

    let updated = PostgresSchema::introspect(&database_url)
        .await
        .expect("should introspect migrated postgres schema");
    let second_pass = target.migrate_from(&updated);

    assert!(
        second_pass.is_empty(),
        "migration should be empty after applying generated SQL, got:\n{}",
        second_pass.to_sql()
    );

    database.cleanup().await;
}

fn normalize_sql(sql: &str) -> &str {
    sql.trim_end()
}
