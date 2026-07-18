use crate::migrations::{
    EmbeddedMigrations, MIGRATION_SQL_FILE_NAME, MigrationDirectory, MigrationSource as _,
    MigrationSourceError,
};
use crate::schema::{
    Attribute, DefaultFunction, DialectMarker, DialectPolicy, Field, FieldType, Model,
    ModelAttribute, ModelIndexAttribute, ModelPrimaryKeyAttribute, ModelUniqueAttribute,
    NativeAttribute, RelationAttribute, RustTypeAttribute, ScalarType, Schema,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct TestDialectPolicy;

impl DialectPolicy for TestDialectPolicy {
    fn validate_scalar_type(_scalar: ScalarType) -> Result<(), String> {
        Ok(())
    }

    fn validate_native_attribute(
        _attribute: NativeAttribute,
        _field_type: &FieldType,
    ) -> Result<(), String> {
        Ok(())
    }

    fn validate_default(
        _field_type: &FieldType,
        _function: &DefaultFunction,
    ) -> Result<(), String> {
        Ok(())
    }

    fn normalize_external_table_name(table: &str) -> Result<String, String> {
        if table.is_empty() {
            Err("external table name must not be empty".to_owned())
        } else {
            Ok(table.to_owned())
        }
    }
}

type TestDialect = DialectMarker<TestDialectPolicy>;
type TestSchema = Schema<TestDialect>;
type TestModel = Model<TestDialect>;
type TestField = Field<TestDialect>;
type TestAttribute = Attribute<TestDialect>;
type TestModelAttribute = ModelAttribute<TestDialect>;
type TestModelIndexAttribute = ModelIndexAttribute<TestDialect>;
type TestModelUniqueAttribute = ModelUniqueAttribute<TestDialect>;
type TestRelationAttribute = RelationAttribute<TestDialect>;
type TestRustTypeAttribute = RustTypeAttribute<TestDialect>;
type TestEmbeddedMigrations = EmbeddedMigrations<TestDialect>;
type TestMigrationDirectory = MigrationDirectory<TestDialect>;

#[test]
fn embedded_migrations_are_sorted_by_name() {
    let migrations = TestEmbeddedMigrations::new([
        ("20240102000000_second", "SELECT 2;"),
        ("20240101000000_first", "SELECT 1;"),
    ]);

    let migrations = migrations
        .read_all()
        .expect("embedded migrations should be readable");

    assert_eq!(migrations[0].name(), "20240101000000_first");
    assert_eq!(migrations[0].sql(), "SELECT 1;");
    assert!(migrations[0].directory().is_none());
    assert!(migrations[0].sql_path().is_none());
    assert_eq!(migrations[1].name(), "20240102000000_second");
    assert_eq!(migrations[1].sql(), "SELECT 2;");
}

#[test]
fn migration_directories_are_sorted_and_preserve_file_metadata() {
    let root = temporary_path("sorted_migrations");

    for (name, sql) in [
        ("20240102000000_second", "SELECT 2;"),
        ("20240101000000_first", "SELECT 1;"),
    ] {
        let migration_directory = root.join(name);
        std::fs::create_dir_all(&migration_directory)
            .expect("migration directory should be creatable");
        std::fs::write(migration_directory.join(MIGRATION_SQL_FILE_NAME), sql)
            .expect("migration script should be writable");
    }

    let directory = TestMigrationDirectory::new(&root);
    let migrations = directory
        .read_all()
        .expect("migration directory should be readable");

    assert_eq!(migrations[0].name(), "20240101000000_first");
    assert_eq!(migrations[0].sql(), "SELECT 1;");
    assert_eq!(
        migrations[0].directory(),
        Some(root.join("20240101000000_first").as_path())
    );
    assert_eq!(
        migrations[0].sql_path(),
        Some(
            root.join("20240101000000_first")
                .join(MIGRATION_SQL_FILE_NAME)
                .as_path()
        )
    );
    assert_eq!(migrations[1].name(), "20240102000000_second");

    std::fs::remove_dir_all(root).expect("temporary migration directory should be removable");
}

#[test]
fn migration_directories_require_a_migration_script() {
    let root = temporary_path("missing_migration_script");
    let missing_directory = root.join("20240101000000_missing");
    std::fs::create_dir_all(&missing_directory).expect("migration directory should be creatable");

    let error = TestMigrationDirectory::new(&root)
        .read_all()
        .expect_err("missing migration script should be rejected");

    assert!(matches!(
        error,
        MigrationSourceError::MissingMigrationScript { directory }
            if directory == missing_directory
    ));

    std::fs::remove_dir_all(root).expect("temporary migration directory should be removable");
}

#[test]
fn creates_timestamped_migration_directories_with_slugified_names() {
    let root = temporary_path("create_migration");
    let directory = TestMigrationDirectory::new(&root);

    let migration = directory
        .create_migration("Add Users!", "SELECT 1;")
        .expect("migration should be created");

    let (timestamp, slug) = migration
        .name()
        .split_once('_')
        .expect("migration name should contain a timestamp and slug");
    assert_eq!(timestamp.len(), 14);
    assert!(
        timestamp
            .chars()
            .all(|character| character.is_ascii_digit())
    );
    assert_eq!(slug, "add_users");
    assert_eq!(migration.sql(), "SELECT 1;");

    let migration_directory = migration
        .directory()
        .expect("created migration should have a directory");
    let sql_path = migration_directory.join(MIGRATION_SQL_FILE_NAME);

    assert_eq!(migration_directory.parent(), Some(root.as_path()));
    assert_eq!(migration.sql_path(), Some(sql_path.as_path()));
    assert_eq!(
        std::fs::read_to_string(sql_path).expect("migration script should be readable"),
        "SELECT 1;"
    );

    std::fs::remove_dir_all(root).expect("temporary migration directory should be removable");
}

#[test]
fn invalid_migration_names_are_reported_by_shared_directory_logic() {
    let root = temporary_path("invalid_migration_name");
    let directory = TestMigrationDirectory::new(&root);

    let error = directory
        .create_migration("---", "SELECT 1;")
        .expect_err("migration name should be rejected");

    assert!(matches!(
        error,
        MigrationSourceError::InvalidMigrationName(name) if name == "---"
    ));

    if root.exists() {
        std::fs::remove_dir_all(root).expect("temporary migration directory should be removable");
    }
}

#[test]
fn validates_backend_neutral_schema_rules() {
    let schema = TestSchema::builder()
        .models(vec![
            TestModel::builder("user")
                .fields(vec![
                    TestField::builder("id", FieldType::int())
                        .attribute(TestAttribute::Id)
                        .build()
                        .expect("field should build"),
                    TestField::builder("posts", FieldType::relation_many("post"))
                        .build()
                        .expect("field should build"),
                ])
                .build()
                .expect("model should build"),
            TestModel::builder("post")
                .fields(vec![
                    TestField::builder("id", FieldType::int())
                        .attributes(vec![TestAttribute::Id])
                        .build()
                        .expect("field should build"),
                    TestField::builder("author_id", FieldType::int())
                        .build()
                        .expect("field should build"),
                    TestField::builder("author", FieldType::relation("user", false, false))
                        .attribute(TestAttribute::Relation(
                            TestRelationAttribute::builder()
                                .field("author_id")
                                .reference("id")
                                .build()
                                .expect("relation should build"),
                        ))
                        .build()
                        .expect("field should build"),
                ])
                .build()
                .expect("model should build"),
        ])
        .build();

    assert!(schema.is_ok());
}

#[test]
fn rejects_duplicate_fields() {
    let error = TestModel::builder("user")
        .fields(vec![
            TestField::builder("id", FieldType::int())
                .attribute(TestAttribute::Id)
                .build()
                .expect("field should build"),
            TestField::builder("id", FieldType::string())
                .build()
                .expect("field should build"),
        ])
        .build()
        .expect_err("duplicate field should be rejected");

    assert!(error.to_string().contains("duplicate field `id`"));
}

#[test]
fn rejects_unknown_relation_targets() {
    let schema = TestSchema::builder()
        .model(
            TestModel::builder("post")
                .fields(vec![
                    TestField::builder("id", FieldType::int())
                        .attribute(TestAttribute::Id)
                        .build()
                        .expect("field should build"),
                    TestField::builder("author", FieldType::relation("Missing", true, false))
                        .build()
                        .expect("field should build"),
                ])
                .build()
                .expect("model should build"),
        )
        .build()
        .expect_err("unknown relation target should be rejected");

    assert!(
        schema
            .to_string()
            .contains("unknown relation target model `Missing`")
    );
}

#[test]
fn accepts_compound_unique_constraints_and_indexes() {
    let model = TestModel::builder("post_locale")
        .fields(vec![
            TestField::builder("id", FieldType::int())
                .attribute(TestAttribute::Id)
                .build()
                .expect("field should build"),
            TestField::builder("post_id", FieldType::int())
                .build()
                .expect("field should build"),
            TestField::builder("locale", FieldType::string())
                .build()
                .expect("field should build"),
        ])
        .attributes(vec![
            TestModelAttribute::Unique(
                TestModelUniqueAttribute::builder()
                    .fields(vec!["post_id".into(), "locale".into()])
                    .build()
                    .expect("unique attribute should build"),
            ),
            TestModelAttribute::Index(
                TestModelIndexAttribute::builder()
                    .fields(vec!["locale".into(), "post_id".into()])
                    .build()
                    .expect("index attribute should build"),
            ),
        ])
        .build();

    assert!(model.is_ok());
}

#[test]
fn rejects_relation_fields_in_compound_constraints() {
    let unique_error = TestModel::builder("post_locale")
        .fields(vec![
            TestField::builder("id", FieldType::int())
                .attribute(TestAttribute::Id)
                .build()
                .expect("field should build"),
            TestField::builder("post", FieldType::relation("post", true, false))
                .build()
                .expect("field should build"),
        ])
        .attribute(TestModelAttribute::Unique(
            TestModelUniqueAttribute::builder()
                .field("post")
                .build()
                .expect("unique attribute should build"),
        ))
        .build()
        .expect_err("relation field should not be usable in a unique constraint");

    assert!(
        unique_error
            .to_string()
            .contains("unique field `post` must be scalar")
    );

    let index_error = TestModel::builder("post_locale")
        .fields(vec![
            TestField::builder("id", FieldType::int())
                .attribute(TestAttribute::Id)
                .build()
                .expect("field should build"),
            TestField::builder("post", FieldType::relation("post", true, false))
                .build()
                .expect("field should build"),
        ])
        .attribute(TestModelAttribute::Index(
            TestModelIndexAttribute::builder()
                .field("post")
                .build()
                .expect("index attribute should build"),
        ))
        .build()
        .expect_err("relation field should not be usable in an index");

    assert!(
        index_error
            .to_string()
            .contains("index field `post` must be scalar")
    );
}

#[test]
fn rejects_mixed_field_and_model_primary_keys() {
    let error = TestModel::builder("like")
        .fields(vec![
            TestField::builder("post_id", FieldType::int())
                .attribute(TestAttribute::Id)
                .build()
                .expect("field should build"),
            TestField::builder("user_id", FieldType::int())
                .build()
                .expect("field should build"),
        ])
        .attribute(TestModelAttribute::Id(
            ModelPrimaryKeyAttribute::<TestDialect>::builder()
                .fields(vec!["post_id".into(), "user_id".into()])
                .build()
                .expect("primary key should build"),
        ))
        .build()
        .expect_err("model should fail");

    assert!(
        error
            .to_string()
            .contains("cannot mix field-level `@id` with model-level `@@id`")
    );
}

#[test]
fn rust_type_overrides_remain_limited_to_strings() {
    let valid = TestField::builder("postal_code", FieldType::string())
        .attribute(TestAttribute::RustType(TestRustTypeAttribute::new(
            "PostalCode",
        )))
        .build();

    assert!(valid.is_ok());

    let error = TestField::builder("user_id", FieldType::int())
        .attribute(TestAttribute::RustType(TestRustTypeAttribute::new(
            "UserId",
        )))
        .build()
        .expect_err("non-string override should fail");

    assert!(
        error
            .to_string()
            .contains("only supported on `String` fields")
    );
}

fn temporary_path(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "vitrail_core_{label}_{}_{}",
        std::process::id(),
        unique_suffix()
    ))
}

fn unique_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos()
}
