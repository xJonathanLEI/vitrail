use crate::support::{TestDatabase, apply_schema, apply_sql_script};
use sqlx::Connection as _;
use sqlx::sqlite::SqliteConnection;
use vitrail_sqlite::{ColumnDefault, ColumnType, SqliteSchema, schema};

const EMPTY_TO_BASE_SQL: &str = include_str!("../fixtures/sqlite_migrations/empty_to_base.sql");
const BASE_TO_EXPANDED_SQL: &str =
    include_str!("../fixtures/sqlite_migrations/base_to_expanded.sql");
const EMPTY_TO_EXPANDED_SQL: &str =
    include_str!("../fixtures/sqlite_migrations/empty_to_expanded.sql");
const EMPTY_TO_BIGINT_SQL: &str = include_str!("../fixtures/sqlite_migrations/empty_to_bigint.sql");
const EMPTY_TO_OPTIONAL_ONE_TO_ONE_SQL: &str =
    include_str!("../fixtures/sqlite_migrations/empty_to_optional_one_to_one.sql");
const EXTERNAL_ONLY_TO_BASE_SQL: &str =
    include_str!("../fixtures/sqlite_migrations/external_only_to_base.sql");

schema! {
    name base_schema

    tables {
        external: ["external_audit_log"]
    }

    model user {
        id          Int      @id @default(autoincrement())
        external_id String   @unique
        email       String   @unique
        name        String
        created_at  DateTime @default(now())
        posts       post[]
    }

    model post {
        id         Int      @id @default(autoincrement())
        public_id  String   @unique
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
        external_id String   @unique
        email       String   @unique
        name        String
        created_at  DateTime @default(now())
        posts       post[]
    }

    model post {
        id         Int      @id @default(autoincrement())
        public_id  String   @unique
        title      String
        body       String?
        published  Boolean
        author_id  Int      @index
        created_at DateTime @default(now())
        score      Float
        updated_at DateTime?
        checksum   Bytes?
        author     user     @relation(fields: [author_id], references: [id])
        comments   comment[]
        locales    post_locale[]

        @@index([published, created_at])
    }

    model comment {
        id        Int    @id @default(autoincrement())
        public_id String @unique
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

schema! {
    name migration_bigint_schema

    model account {
        id           BigInt    @id
        external_ref BigInt    @unique
        credit_limit BigInt
        invoices     invoice[]
    }

    model invoice {
        id           BigInt  @id
        account_id   BigInt  @index
        amount_cents BigInt
        settled_at   BigInt?
        account      account @relation(fields: [account_id], references: [id])

        @@unique([account_id, amount_cents])
    }
}

schema! {
    name optional_one_to_one_schema

    model user {
        id      Int      @id @default(autoincrement())
        profile profile?
    }

    model profile {
        id      Int   @id @default(autoincrement())
        user_id Int?  @unique
        user    user? @relation(fields: [user_id], references: [id])
    }
}

schema! {
    name all_scalar_introspection_schema

    model scalar_record {
        id           Int      @id @default(autoincrement())
        large_number BigInt
        name         String
        enabled      Boolean
        created_at   DateTime @default(now())
        score        Float
        payload      Bytes
        metadata     Json
    }
}

schema! {
    name integer_primary_key_introspection_schema

    model integer_primary_key_record {
        id   Int    @id
        name String
    }
}

schema! {
    name legacy_record_target_schema

    model legacy_record {
        id Int @id @default(autoincrement())
    }
}

schema! {
    name implicit_foreign_key_target_schema

    model parent {
        tenant_id Int
        id        Int
        children  child[]

        @@id([tenant_id, id])
    }

    model child {
        id               Int    @id
        parent_tenant_id Int
        parent_id        Int
        parent           parent @relation(fields: [parent_tenant_id, parent_id], references: [tenant_id, id])
    }
}

schema! {
    name canonical_relation_names_schema

    model audit_log {
        id      Int           @id @default(autoincrement())
        entries audit_entry[]
    }

    model audit_entry {
        id           Int      @id @default(autoincrement())
        audit_log_id Int
        audit_log    AuditLog @relation(fields: [audit_log_id], references: [id])
    }
}

schema! {
    name additive_start_schema

    model zebra_item {
        id    Int    @id @default(autoincrement())
        value String
    }

    model item {
        id    Int    @id @default(autoincrement())
        value String
    }
}

schema! {
    name additive_target_schema

    model zebra_item {
        id    Int     @id @default(autoincrement())
        value String
        note  String?
    }

    model item {
        id     Int      @id @default(autoincrement())
        value  String
        zebra  String?
        alpha  Int?
        middle DateTime?
    }
}

schema! {
    name index_start_schema

    model zebra {
        id    Int    @id @default(autoincrement())
        email String @unique
        name  String
    }

    model item {
        id          Int    @id @default(autoincrement())
        email       String @unique
        username    String @unique
        name        String @index
        title       String @index
        replacement String
    }
}

schema! {
    name index_target_schema

    model zebra {
        id    Int    @id @default(autoincrement())
        email String
        name  String @index
    }

    model item {
        id          Int    @id @default(autoincrement())
        email       String
        username    String
        name        String
        title       String
        replacement String @index
    }
}

schema! {
    name unsupported_index_target_schema

    model indexed_record {
        id    Int    @id
        name  String @index
        email String @index
        rank  String @index
    }
}

schema! {
    name drop_start_schema

    model zebra_obsolete {
        id Int @id @default(autoincrement())
    }

    model retained {
        id Int @id @default(autoincrement())
    }

    model alpha_obsolete {
        id Int @id @default(autoincrement())
    }
}

schema! {
    name drop_target_schema

    model retained {
        id Int @id @default(autoincrement())
    }
}

schema! {
    name ordered_metadata_schema

    model item {
        id Int @id @default(autoincrement())
        a  String
        b  String

        @@index([a])
        @@index([b])
    }
}

schema! {
    name reordered_metadata_schema

    model item {
        b  String
        id Int @id @default(autoincrement())
        a  String

        @@index([b])
        @@index([a])
    }
}

schema! {
    name multiple_redefinitions_start_schema

    model zebra {
        id    Int    @id @default(autoincrement())
        value String
    }

    model alpha {
        id    Int    @id @default(autoincrement())
        value String
    }
}

schema! {
    name multiple_redefinitions_target_schema

    model zebra {
        id       Int    @id @default(autoincrement())
        value    String
        required Int    @index
    }

    model alpha {
        id       Int    @id @default(autoincrement())
        value    String
        required Int    @index
    }
}

fn empty_database_schema() -> SqliteSchema {
    SqliteSchema::empty()
}

fn base_database_schema() -> SqliteSchema {
    SqliteSchema::from_schema_access::<base_schema::Schema>()
}

fn expanded_database_schema() -> SqliteSchema {
    SqliteSchema::from_schema_access::<expanded_schema::Schema>()
}

fn bigint_database_schema() -> SqliteSchema {
    SqliteSchema::from_schema_access::<migration_bigint_schema::Schema>()
}

fn optional_one_to_one_database_schema() -> SqliteSchema {
    SqliteSchema::from_schema_access::<optional_one_to_one_schema::Schema>()
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

#[test]
fn empty_to_bigint_direct_diff_matches_expected_sql() {
    let sql = bigint_database_schema()
        .migrate_from(&empty_database_schema())
        .to_sql();

    assert_eq!(normalize_sql(&sql), normalize_sql(EMPTY_TO_BIGINT_SQL));
}

#[test]
fn empty_to_optional_one_to_one_direct_diff_matches_expected_sql() {
    let sql = optional_one_to_one_database_schema()
        .migrate_from(&empty_database_schema())
        .to_sql();

    assert_eq!(
        normalize_sql(&sql),
        normalize_sql(EMPTY_TO_OPTIONAL_ONE_TO_ONE_SQL)
    );
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

#[tokio::test]
async fn generated_migration_brings_empty_database_to_bigint_schema() {
    assert_generated_migration_roundtrips(
        &empty_database_schema(),
        &bigint_database_schema(),
        EMPTY_TO_BIGINT_SQL,
    )
    .await;
}

#[tokio::test]
async fn generated_migration_brings_empty_database_to_optional_one_to_one_schema() {
    assert_generated_migration_roundtrips(
        &empty_database_schema(),
        &optional_one_to_one_database_schema(),
        EMPTY_TO_OPTIONAL_ONE_TO_ONE_SQL,
    )
    .await;
}

#[tokio::test]
async fn generated_index_only_migration_preserves_prisma_drop_order() {
    let start = SqliteSchema::from_schema_access::<index_start_schema::Schema>();
    let target = SqliteSchema::from_schema_access::<index_target_schema::Schema>();
    let expected_sql = target.migrate_from(&start).to_sql();

    assert_generated_migration_roundtrips(&start, &target, &expected_sql).await;
}

#[tokio::test]
async fn generated_migration_ignores_external_tables() {
    let database = TestDatabase::new();
    let database_url = database.url().to_owned();

    apply_sql_script(
        &database_url,
        r#"
CREATE TABLE "External_Audit_Log" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "payload" TEXT NOT NULL
);
"#,
    )
    .await;

    let current =
        SqliteSchema::introspect_ignoring_external_tables::<base_schema::Schema>(&database_url)
            .await
            .expect("should introspect SQLite schema while ignoring external tables");
    let target = base_database_schema();
    let migration_sql = target.migrate_from(&current).to_sql();

    assert_eq!(
        normalize_sql(&migration_sql),
        normalize_sql(EXTERNAL_ONLY_TO_BASE_SQL)
    );

    apply_sql_script(&database_url, &migration_sql).await;

    let updated =
        SqliteSchema::introspect_ignoring_external_tables::<base_schema::Schema>(&database_url)
            .await
            .expect("should introspect migrated SQLite schema while ignoring external tables");
    let second_pass = target.migrate_from(&updated);

    assert!(
        second_pass.is_empty(),
        "migration should be empty after applying generated SQL, got:\n{}",
        second_pass.to_sql()
    );

    let mut connection = SqliteConnection::connect(&database_url)
        .await
        .expect("should connect to SQLite to verify external table");
    let external_table_exists = sqlx::query_scalar::<_, i64>(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
    )
    .bind("External_Audit_Log")
    .fetch_one(&mut connection)
    .await
    .expect("should check whether the external SQLite table still exists");

    assert_eq!(
        external_table_exists, 1,
        "external table should still exist after migration"
    );

    drop(connection);
    database.cleanup();
}

#[tokio::test]
async fn introspection_preserves_unknown_types_and_can_remove_unsupported_columns() {
    let database = TestDatabase::new();
    let database_url = database.url().to_owned();

    apply_sql_script(
        &database_url,
        r#"
CREATE TABLE "_VITRAIL_MIGRATIONS" (
    "name" TEXT NOT NULL PRIMARY KEY,
    "applied_at" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE "legacy_record" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "payload" VECTOR(3) NOT NULL DEFAULT 'pending'
);
"#,
    )
    .await;

    let introspected = SqliteSchema::introspect(&database_url)
        .await
        .expect("should introspect unknown SQLite declared types");

    assert_eq!(introspected.tables().len(), 1);
    assert_eq!(introspected.tables()[0].name(), "legacy_record");

    let columns = introspected.tables()[0].columns();

    assert_eq!(columns[0].default(), Some(&ColumnDefault::Autoincrement));
    assert_eq!(
        columns[1].column_type(),
        ColumnType::Raw("VECTOR(3)".to_owned())
    );
    assert_eq!(
        columns[1].default(),
        Some(&ColumnDefault::Raw("'pending'".to_owned()))
    );

    let target = SqliteSchema::from_schema_access::<legacy_record_target_schema::Schema>();
    let migration_sql = target.migrate_from(&introspected).to_sql();

    assert!(
        migration_sql.contains("-- RedefineTables"),
        "removing an unsupported column should redefine its table"
    );

    apply_sql_script(&database_url, &migration_sql).await;

    let updated = SqliteSchema::introspect(&database_url)
        .await
        .expect("should introspect SQLite schema after removing the unsupported column");
    let second_pass = target.migrate_from(&updated);

    assert!(
        second_pass.is_empty(),
        "migration should be empty after removing the unsupported column, got:\n{}",
        second_pass.to_sql()
    );
    assert_eq!(updated.tables().len(), 1);
    assert_eq!(updated.tables()[0].columns().len(), 1);
    assert_eq!(updated.tables()[0].columns()[0].name(), "id");

    database.cleanup();
}

#[tokio::test]
async fn introspection_normalizes_every_supported_scalar_mapping() {
    let database = TestDatabase::new();
    let database_url = database.url().to_owned();
    let target = SqliteSchema::from_schema_access::<all_scalar_introspection_schema::Schema>();

    apply_schema(&database_url, &target).await;

    let introspected = SqliteSchema::introspect(&database_url)
        .await
        .expect("should introspect every supported SQLite scalar mapping");
    let columns = introspected.tables()[0].columns();

    assert_eq!(
        columns
            .iter()
            .map(|column| column.column_type())
            .collect::<Vec<_>>(),
        vec![
            ColumnType::Integer,
            ColumnType::BigInt,
            ColumnType::Text,
            ColumnType::Boolean,
            ColumnType::DateTime,
            ColumnType::Real,
            ColumnType::Blob,
            ColumnType::JsonB,
        ]
    );
    assert_eq!(columns[0].default(), Some(&ColumnDefault::Autoincrement));
    assert_eq!(columns[4].default(), Some(&ColumnDefault::CurrentTimestamp));

    let second_pass = target.migrate_from(&introspected);

    assert!(
        second_pass.is_empty(),
        "all supported scalar mappings should round-trip without a migration, got:\n{}",
        second_pass.to_sql()
    );

    database.cleanup();
}

#[tokio::test]
async fn introspection_treats_single_integer_primary_keys_as_required() {
    let database = TestDatabase::new();
    let database_url = database.url().to_owned();

    apply_sql_script(
        &database_url,
        r#"
CREATE TABLE "integer_primary_key_record" (
    "id" INTEGER PRIMARY KEY,
    "name" TEXT NOT NULL
);
"#,
    )
    .await;

    let introspected = SqliteSchema::introspect(&database_url)
        .await
        .expect("should introspect an implicit non-null SQLite integer primary key");
    let id_column = &introspected.tables()[0].columns()[0];

    assert!(
        !id_column.nullable(),
        "a sole INTEGER primary key is non-null even when NOT NULL is omitted"
    );

    let target =
        SqliteSchema::from_schema_access::<integer_primary_key_introspection_schema::Schema>();
    let migration = target.migrate_from(&introspected);

    assert!(
        migration.is_empty(),
        "implicit integer-primary-key nullability should not generate a migration:\n{}",
        migration.to_sql()
    );

    database.cleanup();
}

#[tokio::test]
async fn introspection_preserves_nullable_descending_integer_primary_keys() {
    let database = TestDatabase::new();
    let database_url = database.url().to_owned();

    apply_sql_script(
        &database_url,
        r#"
CREATE TABLE "descending_integer_primary_key_record" (
    "id" INTEGER PRIMARY KEY DESC,
    "name" TEXT NOT NULL
);
"#,
    )
    .await;

    let introspected = SqliteSchema::introspect(&database_url)
        .await
        .expect("should introspect a descending SQLite integer primary key");
    let id_column = &introspected.tables()[0].columns()[0];

    assert!(
        id_column.nullable(),
        "`INTEGER PRIMARY KEY DESC` uses a separate index and remains nullable"
    );

    database.cleanup();
}

#[tokio::test]
async fn introspection_does_not_treat_quoted_autoincrement_text_as_a_default() {
    let database = TestDatabase::new();
    let database_url = database.url().to_owned();

    apply_sql_script(
        &database_url,
        r#"
CREATE TABLE "keyword_record" (
    "id" INTEGER NOT NULL PRIMARY KEY,
    "note" TEXT NOT NULL DEFAULT 'AUTOINCREMENT'
);
"#,
    )
    .await;

    let introspected = SqliteSchema::introspect(&database_url)
        .await
        .expect("should distinguish the AUTOINCREMENT keyword from quoted default text");
    let columns = introspected.tables()[0].columns();

    assert_eq!(columns[0].default(), None);
    assert_eq!(
        columns[1].default(),
        Some(&ColumnDefault::Raw("'AUTOINCREMENT'".to_owned()))
    );

    database.cleanup();
}

#[tokio::test]
async fn introspection_only_includes_explicit_indexes_in_creation_order() {
    let database = TestDatabase::new();
    let database_url = database.url().to_owned();

    apply_sql_script(
        &database_url,
        r#"
CREATE TABLE "indexed_record" (
    "id" INTEGER NOT NULL PRIMARY KEY,
    "email" TEXT NOT NULL UNIQUE,
    "alpha" TEXT NOT NULL,
    "zebra" TEXT NOT NULL
);

CREATE INDEX "indexed_record_zebra_idx" ON "indexed_record"("zebra");
CREATE INDEX "indexed_record_alpha_idx" ON "indexed_record"("alpha");
"#,
    )
    .await;

    let introspected = SqliteSchema::introspect(&database_url)
        .await
        .expect("should introspect explicit SQLite indexes");
    let indexes = introspected.tables()[0].indexes();

    assert_eq!(
        indexes.iter().map(|index| index.name()).collect::<Vec<_>>(),
        vec!["indexed_record_zebra_idx", "indexed_record_alpha_idx",]
    );
    assert!(
        indexes.iter().all(|index| !index.unique()),
        "implicit unique indexes should not be included"
    );

    database.cleanup();
}

#[tokio::test]
async fn introspection_replaces_partial_expression_and_descending_indexes() {
    let database = TestDatabase::new();
    let database_url = database.url().to_owned();

    apply_sql_script(
        &database_url,
        r#"
CREATE TABLE "indexed_record" (
    "id" INTEGER NOT NULL PRIMARY KEY,
    "name" TEXT NOT NULL,
    "email" TEXT NOT NULL,
    "rank" TEXT NOT NULL
);

CREATE INDEX "indexed_record_name_idx" ON "indexed_record"("name")
WHERE "name" <> '';

CREATE INDEX "indexed_record_email_idx" ON "indexed_record"(lower("email"));

CREATE INDEX "indexed_record_rank_idx" ON "indexed_record"("rank" DESC);
"#,
    )
    .await;

    let current = SqliteSchema::introspect(&database_url)
        .await
        .expect("should introspect unsupported index definitions");
    let target = SqliteSchema::from_schema_access::<unsupported_index_target_schema::Schema>();
    let migration_sql = target.migrate_from(&current).to_sql();

    assert!(migration_sql.contains(r#"DROP INDEX "indexed_record_rank_idx";"#));
    assert!(migration_sql.contains(r#"DROP INDEX "indexed_record_email_idx";"#));
    assert!(migration_sql.contains(r#"DROP INDEX "indexed_record_name_idx";"#));
    assert!(
        migration_sql
            .contains(r#"CREATE INDEX "indexed_record_name_idx" ON "indexed_record"("name");"#)
    );
    assert!(
        migration_sql
            .contains(r#"CREATE INDEX "indexed_record_email_idx" ON "indexed_record"("email");"#)
    );
    assert!(
        migration_sql
            .contains(r#"CREATE INDEX "indexed_record_rank_idx" ON "indexed_record"("rank");"#)
    );

    apply_sql_script(&database_url, &migration_sql).await;

    let updated = SqliteSchema::introspect(&database_url)
        .await
        .expect("should introspect replaced indexes");
    let second_pass = target.migrate_from(&updated);

    assert!(
        second_pass.is_empty(),
        "supported replacement indexes should round-trip without another migration, got:\n{}",
        second_pass.to_sql()
    );

    database.cleanup();
}

#[tokio::test]
async fn introspection_resolves_implicit_primary_key_references() {
    let database = TestDatabase::new();
    let database_url = database.url().to_owned();

    apply_sql_script(
        &database_url,
        r#"
CREATE TABLE "parent" (
    "tenant_id" INTEGER NOT NULL,
    "id" INTEGER NOT NULL,
    PRIMARY KEY ("tenant_id", "id")
);

CREATE TABLE "child" (
    "id" INTEGER NOT NULL PRIMARY KEY,
    "parent_tenant_id" INTEGER NOT NULL,
    "parent_id" INTEGER NOT NULL,
    FOREIGN KEY ("parent_tenant_id", "parent_id") REFERENCES "parent" ON DELETE RESTRICT ON UPDATE CASCADE
);
"#,
    )
    .await;

    let introspected = SqliteSchema::introspect(&database_url)
        .await
        .expect("should resolve omitted foreign-key references to primary-key columns");
    let child = introspected
        .tables()
        .iter()
        .find(|table| table.name() == "child")
        .expect("child table should be introspected");
    let foreign_key = child
        .foreign_keys()
        .first()
        .expect("child table should have a foreign key");

    assert_eq!(
        foreign_key.referenced_columns(),
        &["tenant_id".to_owned(), "id".to_owned()]
    );

    let target = SqliteSchema::from_schema_access::<implicit_foreign_key_target_schema::Schema>();
    let migration = target.migrate_from(&introspected);

    assert!(
        migration.is_empty(),
        "implicit primary-key references should round-trip without a migration, got:\n{}",
        migration.to_sql()
    );

    database.cleanup();
}

#[test]
fn foreign_keys_use_canonical_relation_target_table_names() {
    let target = SqliteSchema::from_schema_access::<canonical_relation_names_schema::Schema>();

    let sql = target.migrate_from(&SqliteSchema::empty()).to_sql();

    assert!(sql.contains(r#"REFERENCES "audit_log" ("id")"#));
    assert!(!sql.contains(r#"REFERENCES "AuditLog" ("id")"#));
}

#[test]
fn nullable_columns_without_defaults_use_sorted_alter_table_statements() {
    let current = SqliteSchema::from_schema_access::<additive_start_schema::Schema>();
    let target = SqliteSchema::from_schema_access::<additive_target_schema::Schema>();

    let sql = target.migrate_from(&current).to_sql();

    assert_eq!(
        normalize_sql(&sql),
        normalize_sql(
            r#"-- AlterTable
ALTER TABLE "item" ADD COLUMN "alpha" INTEGER;
ALTER TABLE "item" ADD COLUMN "middle" DATETIME;
ALTER TABLE "item" ADD COLUMN "zebra" TEXT;

-- AlterTable
ALTER TABLE "zebra_item" ADD COLUMN "note" TEXT;
"#,
        )
    );
}

#[test]
fn index_only_changes_do_not_redefine_the_table() {
    let current = SqliteSchema::from_schema_access::<index_start_schema::Schema>();
    let target = SqliteSchema::from_schema_access::<index_target_schema::Schema>();

    let sql = target.migrate_from(&current).to_sql();

    assert_eq!(
        normalize_sql(&sql),
        normalize_sql(
            r#"-- DropIndex
DROP INDEX "item_title_idx";

-- DropIndex
DROP INDEX "item_name_idx";

-- DropIndex
DROP INDEX "item_username_key";

-- DropIndex
DROP INDEX "item_email_key";

-- DropIndex
DROP INDEX "zebra_email_key";

-- CreateIndex
CREATE INDEX "item_replacement_idx" ON "item"("replacement");

-- CreateIndex
CREATE INDEX "zebra_name_idx" ON "zebra"("name");
"#,
        )
    );
    assert!(!sql.contains("-- RedefineTables"));
}

#[test]
fn removed_tables_use_prisma_foreign_key_pragma_sequence() {
    let current = SqliteSchema::from_schema_access::<drop_start_schema::Schema>();
    let target = SqliteSchema::from_schema_access::<drop_target_schema::Schema>();

    let sql = target.migrate_from(&current).to_sql();

    assert_eq!(
        normalize_sql(&sql),
        normalize_sql(
            r#"-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "alpha_obsolete";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "zebra_obsolete";
PRAGMA foreign_keys=on;
"#,
        )
    );
}

#[test]
fn harmless_column_and_index_metadata_order_changes_are_ignored() {
    let current = SqliteSchema::from_schema_access::<ordered_metadata_schema::Schema>();
    let target = SqliteSchema::from_schema_access::<reordered_metadata_schema::Schema>();

    let migration = target.migrate_from(&current);

    assert!(
        migration.is_empty(),
        "metadata ordering alone should not generate SQL:\n{}",
        migration.to_sql()
    );
}

#[test]
fn multiple_table_redefinitions_share_one_block_in_prisma_table_order() {
    let current = SqliteSchema::from_schema_access::<multiple_redefinitions_start_schema::Schema>();
    let target = SqliteSchema::from_schema_access::<multiple_redefinitions_target_schema::Schema>();

    let sql = target.migrate_from(&current).to_sql();

    assert_eq!(sql.matches("-- RedefineTables").count(), 1);
    assert_eq!(sql.matches("PRAGMA defer_foreign_keys=ON;").count(), 1);
    assert_eq!(sql.matches("PRAGMA defer_foreign_keys=OFF;").count(), 1);

    let zebra_position = sql
        .find(r#"CREATE TABLE "new_zebra""#)
        .expect("zebra should be redefined");
    let alpha_position = sql
        .find(r#"CREATE TABLE "new_alpha""#)
        .expect("alpha should be redefined");

    assert!(
        alpha_position < zebra_position,
        "Prisma orders table redefinitions by table name"
    );
    assert!(sql.contains(r#"CREATE INDEX "zebra_required_idx""#));
    assert!(sql.contains(r#"CREATE INDEX "alpha_required_idx""#));
}

async fn assert_generated_migration_roundtrips(
    start: &SqliteSchema,
    target: &SqliteSchema,
    expected_sql: &str,
) {
    let database = TestDatabase::new();
    let database_url = database.url().to_owned();

    apply_schema(&database_url, start).await;

    let current = SqliteSchema::introspect(&database_url)
        .await
        .expect("should introspect current SQLite schema");
    let migration_sql = target.migrate_from(&current).to_sql();

    assert_eq!(normalize_sql(&migration_sql), normalize_sql(expected_sql));

    apply_sql_script(&database_url, &migration_sql).await;

    let updated = SqliteSchema::introspect(&database_url)
        .await
        .expect("should introspect migrated SQLite schema");
    let second_pass = target.migrate_from(&updated);

    assert!(
        second_pass.is_empty(),
        "migration should be empty after applying generated SQL, got:\n{}",
        second_pass.to_sql()
    );

    database.cleanup();
}

fn normalize_sql(sql: &str) -> &str {
    sql.trim_end()
}
