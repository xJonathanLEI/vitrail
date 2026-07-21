#![cfg(not(target_arch = "wasm32"))]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use vitrail_d1::{D1MigrationGenerator, schema};

schema! {
    name initial_migration_schema

    model scalar_record {
        id         Int      @id @default(autoincrement())
        min_value  Int
        max_value  BigInt
        active     Boolean
        score      Float
        label      String   @unique
        payload    Bytes
        created_at DateTime
        metadata   Json
        note       String?
    }

    model author {
        id    Int    @id @default(autoincrement())
        name  String
        posts post[]
    }

    model post {
        id        Int    @id @default(autoincrement())
        title     String?
        author_id Int
        author    author @relation(fields: [author_id], references: [id])

        @@index([author_id])
    }
}

schema! {
    name final_migration_schema

    model scalar_record {
        id         Int      @id @default(autoincrement())
        min_value  Int
        max_value  BigInt
        active     Boolean
        score      Float
        label      String   @unique
        payload    Bytes
        created_at DateTime
        metadata   Json
        note       String?
    }

    model author {
        id    Int    @id @default(autoincrement())
        name  String
        posts post[]
    }

    model post {
        id        Int    @id @default(autoincrement())
        title     String
        author_id Int
        author    author @relation(fields: [author_id], references: [id])

        @@index([author_id])
    }
}

schema! {
    name wide_relation_migration_schema

    tables {
        external: ["scalar_record", "author", "post"]
    }

    model wide_parent {
        id       BigInt @id
        children wide_child[]
    }

    model wide_child {
        id        BigInt @id
        parent_id BigInt
        value_01  BigInt
        value_02  BigInt
        value_03  BigInt
        value_04  BigInt
        value_05  BigInt
        value_06  BigInt
        value_07  BigInt
        value_08  BigInt
        value_09  BigInt
        value_10  BigInt
        value_11  BigInt
        value_12  BigInt
        value_13  BigInt
        value_14  BigInt
        value_15  BigInt
        value_16  BigInt
        value_17  BigInt
        value_18  BigInt
        value_19  BigInt
        value_20  BigInt
        value_21  BigInt
        value_22  BigInt
        value_23  BigInt
        value_24  BigInt
        value_25  BigInt
        value_26  BigInt
        value_27  BigInt
        value_28  BigInt
        value_29  BigInt
        value_30  BigInt
        value_31  BigInt
        value_32  BigInt
        value_33  BigInt
        parent    wide_parent @relation(fields: [parent_id], references: [id])
    }
}

static NEXT_TEMPORARY_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TemporaryDirectory {
    path: PathBuf,
}

impl TemporaryDirectory {
    fn new(name: &str) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after the Unix epoch")
            .as_nanos();
        let sequence = NEXT_TEMPORARY_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "vitrail-d1-{name}-{}-{timestamp}-{sequence}",
            std::process::id(),
        ));

        if path.exists() {
            fs::remove_dir_all(&path)
                .expect("stale temporary migration directory should be removable");
        }

        fs::create_dir_all(&path).expect("temporary migration directory should be creatable");

        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        if self.path.exists() {
            fs::remove_dir_all(&self.path)
                .expect("temporary migration directory should be removable");
        }
    }
}

#[tokio::test]
async fn generates_nested_d1_migrations_and_replans_to_an_empty_diff() {
    let temporary_directory = TemporaryDirectory::new("migration-generation");
    let generator = D1MigrationGenerator::new(temporary_directory.path());

    let initial = generator
        .generate_migration::<initial_migration_schema::Schema>("Initial Schema!")
        .await
        .expect("initial D1 migration should be generated")
        .expect("initial schema should require a migration");

    assert!(
        initial.migration().name().ends_with("_initial_schema"),
        "migration name should use shared sanitization"
    );
    assert_eq!(
        initial.migration().directory().and_then(Path::parent),
        Some(temporary_directory.path()),
    );
    assert_eq!(
        initial
            .migration()
            .sql_path()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str()),
        Some("migration.sql"),
    );
    assert!(!initial.sql().contains("PRAGMA defer_foreign_keys"));
    assert!(!initial.sql().contains("PRAGMA foreign_keys"));

    for _ in 0..2 {
        let plan = generator
            .plan_migration::<initial_migration_schema::Schema>()
            .await
            .expect("generated initial migration should apply to the atomic shadow database");

        assert!(
            plan.is_empty(),
            "replanning the generated initial schema should produce an empty diff"
        );
        assert_eq!(plan.to_sql(), "");
    }

    let redefinition = generator
        .generate_migration::<final_migration_schema::Schema>("Require Post Title")
        .await
        .expect("D1 table-redefinition migration should be generated")
        .expect("the final schema should require a migration");

    assert!(
        redefinition
            .migration()
            .name()
            .ends_with("_require_post_title"),
        "migration name should use shared sanitization"
    );
    assert_eq!(
        redefinition
            .sql()
            .matches("PRAGMA defer_foreign_keys=ON;")
            .count(),
        1,
    );
    assert_eq!(
        redefinition
            .sql()
            .matches("PRAGMA defer_foreign_keys=OFF;")
            .count(),
        1,
    );
    assert!(
        !redefinition
            .sql()
            .to_ascii_lowercase()
            .contains("pragma foreign_keys"),
        "D1 migrations must not emit ineffective foreign-key toggles"
    );
    assert!(
        redefinition
            .sql()
            .contains("CONSTRAINT \"post_author_id_fkey\""),
        "table redefinition should preserve the foreign key"
    );
    assert!(
        redefinition.sql().contains("INSERT INTO \"new_post\""),
        "table redefinition should preserve existing rows"
    );

    for _ in 0..2 {
        let plan = generator
            .plan_migration::<final_migration_schema::Schema>()
            .await
            .expect("all generated D1 migrations should apply atomically to the shadow database");

        assert!(
            plan.is_empty(),
            "replanning the final schema should produce an empty diff"
        );
        assert_eq!(plan.to_sql(), "");
    }

    let wide_relation = generator
        .generate_migration::<wide_relation_migration_schema::Schema>("Wide Relation Fixture")
        .await
        .expect("wide-relation D1 migration should be generated")
        .expect("the wide-relation schema should require a migration");

    assert!(
        wide_relation
            .migration()
            .name()
            .ends_with("_wide_relation_fixture"),
        "migration name should use shared sanitization"
    );
    assert!(
        !wide_relation.sql().contains("PRAGMA defer_foreign_keys"),
        "adding the wide relation tables should not require destructive migration pragmas"
    );
    assert!(
        !wide_relation
            .sql()
            .to_ascii_lowercase()
            .contains("pragma foreign_keys"),
        "D1 migrations must not emit ineffective foreign-key toggles"
    );
    assert!(
        wide_relation.sql().contains("CREATE TABLE \"wide_parent\""),
        "wide-relation migration should create the parent table"
    );
    assert!(
        wide_relation.sql().contains("CREATE TABLE \"wide_child\""),
        "wide-relation migration should create the child table"
    );
    assert!(
        wide_relation.sql().contains("\"value_33\" BIGINT NOT NULL"),
        "wide-relation migration should include every nested result column"
    );
    assert!(
        wide_relation
            .sql()
            .contains("CONSTRAINT \"wide_child_parent_id_fkey\""),
        "wide-relation migration should include the parent foreign key"
    );

    for _ in 0..2 {
        let plan = generator
            .plan_migration::<wide_relation_migration_schema::Schema>()
            .await
            .expect("the wide-relation migration should apply atomically to the shadow database");

        assert!(
            plan.is_empty(),
            "replanning the wide-relation schema should produce an empty diff"
        );
        assert_eq!(plan.to_sql(), "");
    }

    let generated_scripts = read_nested_migrations(temporary_directory.path());
    let checked_scripts = read_nested_migrations(&checked_fixture_directory());

    assert_eq!(
        generated_scripts
            .iter()
            .map(|migration| migration.slug.as_str())
            .collect::<Vec<_>>(),
        [
            "initial_schema",
            "require_post_title",
            "wide_relation_fixture",
        ],
    );
    assert_eq!(
        checked_scripts
            .iter()
            .map(|migration| migration.slug.as_str())
            .collect::<Vec<_>>(),
        [
            "initial_schema",
            "require_post_title",
            "wide_relation_fixture",
        ],
    );
    assert_eq!(
        generated_scripts
            .iter()
            .map(|migration| migration.sql.as_str())
            .collect::<Vec<_>>(),
        checked_scripts
            .iter()
            .map(|migration| migration.sql.as_str())
            .collect::<Vec<_>>(),
        "checked Worker migration fixtures must match D1 generator output",
    );
}

struct NestedMigration {
    slug: String,
    sql: String,
}

fn read_nested_migrations(root: &Path) -> Vec<NestedMigration> {
    let mut directories = fs::read_dir(root)
        .expect("migration directory should be readable")
        .collect::<Result<Vec<_>, _>>()
        .expect("migration entries should be readable")
        .into_iter()
        .filter(|entry| {
            entry
                .file_type()
                .expect("migration entry type should be readable")
                .is_dir()
        })
        .collect::<Vec<_>>();

    directories.sort_by_key(|entry| entry.file_name());

    directories
        .into_iter()
        .map(|entry| {
            let name = entry.file_name().to_string_lossy().into_owned();
            let (_, slug) = name
                .split_once('_')
                .expect("nested migration name should contain a timestamp and slug");
            let sql_path = entry.path().join("migration.sql");

            assert!(
                sql_path.is_file(),
                "nested migration directory should contain migration.sql"
            );

            NestedMigration {
                slug: slug.to_owned(),
                sql: fs::read_to_string(sql_path)
                    .expect("nested migration script should be readable"),
            }
        })
        .collect()
}

fn checked_fixture_directory() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../examples/workspace/d1-worker/migrations")
}
