use crate::{
    Attribute, ColumnDefault, ColumnType, DefaultAttribute, Field, FieldType, Model,
    ModelAttribute, ModelPrimaryKeyAttribute, RustTypeAttribute, ScalarType, Schema,
    ValidationError, ValidationErrors, ValidationLocation,
};

#[test]
fn accepts_supported_sqlite_scalar_types() {
    let schema = Schema::builder()
        .external_table("external_audit_log")
        .model(
            Model::builder("record")
                .fields(vec![
                    Field::builder("id", FieldType::int())
                        .attributes(vec![
                            Attribute::Id,
                            Attribute::Default(DefaultAttribute::autoincrement()),
                        ])
                        .build()
                        .expect("Int autoincrement primary key should build"),
                    Field::builder("large_number", FieldType::big_int())
                        .build()
                        .expect("BigInt field should build"),
                    Field::builder("name", FieldType::string())
                        .build()
                        .expect("String field should build"),
                    Field::builder("enabled", FieldType::scalar(ScalarType::Boolean, false))
                        .build()
                        .expect("Boolean field should build"),
                    Field::builder("created_at", FieldType::date_time())
                        .attribute(Attribute::Default(DefaultAttribute::now()))
                        .build()
                        .expect("DateTime field should build"),
                    Field::builder("score", FieldType::scalar(ScalarType::Float, false))
                        .build()
                        .expect("Float field should build"),
                    Field::builder("payload", FieldType::scalar(ScalarType::Bytes, false))
                        .build()
                        .expect("Bytes field should build"),
                    Field::builder("metadata", FieldType::scalar(ScalarType::Json, false))
                        .build()
                        .expect("Json field should build"),
                ])
                .build()
                .expect("model should build"),
        )
        .build()
        .expect("supported SQLite schema should build");

    assert_eq!(schema.models().len(), 1);
    assert_eq!(schema.external_tables(), &["external_audit_log"]);

    let sqlite_schema = schema.to_sqlite_schema();
    let columns = sqlite_schema.tables()[0].columns();

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
    assert!(
        columns
            .iter()
            .enumerate()
            .all(|(index, column)| matches!(index, 0 | 4) || column.default().is_none())
    );

    let sql = sqlite_schema
        .migrate_from(&crate::SqliteSchema::empty())
        .to_sql();

    assert!(sql.contains(r#""id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT"#));
    assert!(sql.contains(r#""metadata" JSONB NOT NULL"#));
}

#[test]
fn migration_sql_escapes_programmatic_schema_identifiers() {
    let schema = Schema::builder()
        .model(
            Model::builder("quoted\"table")
                .fields(vec![
                    Field::builder("id\"column", FieldType::int())
                        .attributes(vec![
                            Attribute::Id,
                            Attribute::Default(DefaultAttribute::autoincrement()),
                        ])
                        .build()
                        .expect("primary key field should build"),
                    Field::builder("value\"column", FieldType::string())
                        .attribute(Attribute::Unique)
                        .build()
                        .expect("unique field should build"),
                ])
                .build()
                .expect("model should build"),
        )
        .build()
        .expect("schema should build");

    let sql = schema
        .to_sqlite_schema()
        .migrate_from(&crate::SqliteSchema::empty())
        .to_sql();

    assert!(sql.contains(r#"CREATE TABLE "quoted""table" ("#));
    assert!(sql.contains(r#""id""column" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT"#));
    assert!(sql.contains(
        r#"CREATE UNIQUE INDEX "quoted""table_value""column_key" ON "quoted""table"("value""column");"#
    ));
}

#[test]
fn sqlite_index_debug_output_hides_internal_introspection_state() {
    let schema = Schema::builder()
        .model(
            Model::builder("user")
                .fields(vec![
                    Field::builder("id", FieldType::int())
                        .attribute(Attribute::Id)
                        .build()
                        .expect("primary key field should build"),
                    Field::builder("email", FieldType::string())
                        .attribute(Attribute::Unique)
                        .build()
                        .expect("unique field should build"),
                ])
                .build()
                .expect("model should build"),
        )
        .build()
        .expect("schema should build");
    let sqlite_schema = schema.to_sqlite_schema();
    let index = &sqlite_schema.tables()[0].indexes()[0];
    let debug = format!("{index:?}");

    assert_eq!(
        debug,
        "SqliteIndex { name: \"user_email_key\", columns: [\"email\"], unique: true }"
    );
    assert!(!debug.contains("definition_supported"));
}

#[test]
fn accepts_bigint_without_autoincrement() {
    let schema = Schema::builder()
        .model(
            Model::builder("event")
                .fields(vec![
                    Field::builder("id", FieldType::big_int())
                        .attribute(Attribute::Id)
                        .build()
                        .expect("BigInt primary key should build without autoincrement"),
                    Field::builder("description", FieldType::string())
                        .build()
                        .expect("description field should build"),
                ])
                .build()
                .expect("model should build"),
        )
        .build();

    assert!(schema.is_ok());
}

#[test]
fn rejects_decimal_fields() {
    let errors = Field::builder("amount", FieldType::scalar(ScalarType::Decimal, false))
        .build()
        .expect_err("Decimal should be rejected by the SQLite dialect");

    let error = only_error(&errors);
    assert!(matches!(
        &error.location,
        ValidationLocation::FieldType { field, ty, .. }
            if field == "amount" && ty == "Decimal"
    ));
    assert!(
        error
            .message
            .contains("not supported by the SQLite dialect")
    );
}

#[test]
fn rejects_postgres_native_uuid_attributes() {
    let errors = Field::builder("external_id", FieldType::string())
        .attribute(Attribute::DbUuid)
        .build()
        .expect_err("@db.Uuid should be rejected by the SQLite dialect");

    let error = only_error(&errors);
    assert!(matches!(
        &error.location,
        ValidationLocation::Attribute {
            field,
            attribute,
            ..
        } if field == "external_id" && attribute == "@db.Uuid"
    ));
    assert!(error.message.contains("PostgreSQL native attribute"));
    assert!(error.message.contains("SQLite dialect"));
}

#[test]
fn rejects_bigint_autoincrement_defaults() {
    let errors = Field::builder("id", FieldType::big_int())
        .attributes(vec![
            Attribute::Id,
            Attribute::Default(DefaultAttribute::autoincrement()),
        ])
        .build()
        .expect_err("BigInt autoincrement should be rejected by the SQLite dialect");

    let error = only_error(&errors);
    assert!(matches!(
        &error.location,
        ValidationLocation::Attribute {
            field,
            attribute,
            ..
        } if field == "id" && attribute == "@default"
    ));
    assert!(
        error
            .message
            .contains("only supported on `Int` fields in SQLite")
    );
}

#[test]
fn accepts_int_autoincrement_defaults() {
    let field = Field::builder("id", FieldType::int())
        .attributes(vec![
            Attribute::Id,
            Attribute::Default(DefaultAttribute::autoincrement()),
        ])
        .build();

    assert!(field.is_ok());
}

#[test]
fn rejects_int_autoincrement_on_non_primary_key() {
    let errors = Model::builder("event")
        .fields(vec![
            Field::builder("id", FieldType::int())
                .attribute(Attribute::Id)
                .build()
                .expect("primary key field should build"),
            Field::builder("sequence", FieldType::int())
                .attribute(Attribute::Default(DefaultAttribute::autoincrement()))
                .build()
                .expect("autoincrement field should pass field-level validation"),
        ])
        .build()
        .expect_err("SQLite autoincrement should require a primary key field");

    let error = only_error(&errors);
    assert!(matches!(
        &error.location,
        ValidationLocation::Attribute {
            model,
            field,
            attribute,
        } if model == "event" && field == "sequence" && attribute == "@default"
    ));
    assert!(error.message.contains("sole primary key column in SQLite"));
}

#[test]
fn rejects_int_autoincrement_on_compound_primary_key() {
    let primary_key = ModelPrimaryKeyAttribute::builder()
        .fields(vec!["id".to_owned(), "tenant_id".to_owned()])
        .build()
        .expect("compound primary key should build");

    let errors = Model::builder("event")
        .fields(vec![
            Field::builder("id", FieldType::int())
                .attribute(Attribute::Default(DefaultAttribute::autoincrement()))
                .build()
                .expect("autoincrement field should pass field-level validation"),
            Field::builder("tenant_id", FieldType::int())
                .build()
                .expect("tenant field should build"),
        ])
        .attribute(ModelAttribute::Id(primary_key))
        .build()
        .expect_err("SQLite autoincrement should reject compound primary keys");

    let error = only_error(&errors);
    assert!(matches!(
        &error.location,
        ValidationLocation::Attribute {
            model,
            field,
            attribute,
        } if model == "event" && field == "id" && attribute == "@default"
    ));
    assert!(error.message.contains("sole primary key column in SQLite"));
}

#[test]
fn accepts_int_autoincrement_on_single_model_primary_key() {
    let primary_key = ModelPrimaryKeyAttribute::builder()
        .field("id")
        .build()
        .expect("single-column primary key should build");

    let model = Model::builder("event")
        .field(
            Field::builder("id", FieldType::int())
                .attribute(Attribute::Default(DefaultAttribute::autoincrement()))
                .build()
                .expect("autoincrement field should pass field-level validation"),
        )
        .attribute(ModelAttribute::Id(primary_key))
        .build();

    assert!(model.is_ok());
}

#[test]
fn accepts_unqualified_external_table_names() {
    let schema = Schema::builder()
        .external_tables(vec![
            "external_audit_log".to_owned(),
            "legacy_events".to_owned(),
        ])
        .model(model_with_int_id("user"))
        .build();

    assert!(schema.is_ok());
}

#[test]
fn rejects_schema_qualified_external_table_names() {
    for qualified_name in ["main.external_audit_log", "public.external_audit_log"] {
        let errors = Schema::builder()
            .external_table(qualified_name)
            .model(model_with_int_id("user"))
            .build()
            .expect_err("qualified external table should be rejected");

        let error = only_error(&errors);
        assert!(matches!(
            &error.location,
            ValidationLocation::ExternalTable { table }
                if table == qualified_name
        ));
        assert!(
            error
                .message
                .contains("must use an unqualified table name in SQLite")
        );
    }
}

#[test]
fn keeps_rust_type_overrides_limited_to_strings() {
    let string_field = Field::builder("postal_code", FieldType::string())
        .attribute(Attribute::RustType(RustTypeAttribute::new("PostalCode")))
        .build();

    assert!(string_field.is_ok());

    let errors = Field::builder("user_id", FieldType::int())
        .attribute(Attribute::RustType(RustTypeAttribute::new("UserId")))
        .build()
        .expect_err("@rust_ty on a non-String field should be rejected");

    let error = only_error(&errors);
    assert!(matches!(
        &error.location,
        ValidationLocation::Attribute {
            field,
            attribute,
            ..
        } if field == "user_id" && attribute == "@rust_ty"
    ));
    assert!(error.message.contains("only supported on `String` fields"));
}

fn model_with_int_id(name: &str) -> Model {
    Model::builder(name)
        .field(
            Field::builder("id", FieldType::int())
                .attribute(Attribute::Id)
                .build()
                .expect("id field should build"),
        )
        .build()
        .expect("model should build")
}

fn only_error(errors: &ValidationErrors) -> &ValidationError {
    assert_eq!(errors.len(), 1, "expected exactly one validation error");
    errors
        .iter()
        .next()
        .expect("one validation error should be present")
}
