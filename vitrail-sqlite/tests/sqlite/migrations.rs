use vitrail_sqlite::{SqliteSchema, schema};

const EMPTY_TO_BASE_SQL: &str = include_str!("../fixtures/sqlite_migrations/empty_to_base.sql");
const BASE_TO_EXPANDED_SQL: &str =
    include_str!("../fixtures/sqlite_migrations/base_to_expanded.sql");
const EMPTY_TO_EXPANDED_SQL: &str =
    include_str!("../fixtures/sqlite_migrations/empty_to_expanded.sql");
const EMPTY_TO_BIGINT_SQL: &str = include_str!("../fixtures/sqlite_migrations/empty_to_bigint.sql");
const EMPTY_TO_OPTIONAL_ONE_TO_ONE_SQL: &str =
    include_str!("../fixtures/sqlite_migrations/empty_to_optional_one_to_one.sql");

schema! {
    name base_schema

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

fn normalize_sql(sql: &str) -> &str {
    sql.trim_end()
}
