use crate::support::{TestDatabase, apply_sql_script};
use vitrail_pg::{PostgresSchema, schema};

const EMPTY_TO_BASE_SQL: &str = include_str!("../fixtures/pg_migrations/empty_to_base.sql");
const BASE_TO_EXPANDED_SQL: &str = include_str!("../fixtures/pg_migrations/base_to_expanded.sql");
const EMPTY_TO_EXPANDED_SQL: &str = include_str!("../fixtures/pg_migrations/empty_to_expanded.sql");

schema! {
    name base_schema

    model user {
        id          Int      @id @default(autoincrement())
        external_id String   @unique @db.Uuid
        email       String   @unique
        name        String
        created_at  DateTime @default(now())
        posts       post[]
    }

    model post {
        id         Int      @id @default(autoincrement())
        public_id  String   @unique @db.Uuid
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
        id          Int      @id @default(autoincrement())
        external_id String   @unique @db.Uuid
        email       String   @unique
        name        String
        created_at  DateTime @default(now())
        posts       post[]
    }

    model post {
        id         Int           @id @default(autoincrement())
        public_id  String        @unique @db.Uuid
        title      String
        body       String?
        published  Boolean
        author_id  Int           @index
        created_at DateTime      @default(now())
        score      Decimal
        updated_at DateTime?
        checksum   Bytes?
        author     user          @relation(fields: [author_id], references: [id])
        comments   comment[]
        locales    post_locale[]

        @@index([published, created_at])
    }

    model comment {
        id        Int    @id @default(autoincrement())
        public_id String @unique @db.Uuid
        body      String
        post_id   Int
        post      post   @relation(fields: [post_id], references: [id])
    }

    model post_locale {
        post_id Int
        locale  String
        title   String
        post    post               @relation(fields: [post_id], references: [id])
        notes   translation_note[]

        @@id([post_id, locale])
        @@unique([post_id, title])
        @@index([title, locale])
    }

    model translation_note {
        id          Int         @id @default(autoincrement())
        post_id     Int
        locale      String
        body        String
        translation post_locale @relation(fields: [post_id, locale], references: [post_id, locale])
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
