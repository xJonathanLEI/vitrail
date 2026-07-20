use crate::statement::CompiledStatement;
use crate::{
    Attribute, BindingValue, D1_MAX_BINDINGS, D1_MAX_COLUMNS, D1_MAX_SQL_BYTES, Field, FieldType,
    InsertValue, InsertValues, Model, OperationKind, QueryFilter, QueryFilterValue,
    QueryFilterValues, QueryOrder, QueryOrderDirection, QueryPagination, QueryRelationSelection,
    QuerySelection, QueryVariableValue, QueryVariables, RelationAttribute, ScalarType, Schema,
    SqliteFamilyFlavor, SqliteSchema, UpdateValue, UpdateValues, ValidationLocation,
    compile_delete_many_with_flavor, compile_insert_with_flavor, compile_query,
    compile_query_with_flavor, compile_update_many_with_flavor, validate_d1_schema,
};

fn integer_schema() -> Schema {
    Schema::builder()
        .model(
            Model::builder("number")
                .fields(vec![
                    Field::builder("id", FieldType::int())
                        .attribute(Attribute::Id)
                        .build()
                        .expect("integer id field should build"),
                    Field::builder("big", FieldType::big_int())
                        .build()
                        .expect("big integer field should build"),
                ])
                .build()
                .expect("integer model should build"),
        )
        .build()
        .expect("integer schema should build")
}

fn datetime_schema() -> Schema {
    Schema::builder()
        .model(
            Model::builder("event")
                .fields(vec![
                    Field::builder("id", FieldType::int())
                        .attribute(Attribute::Id)
                        .build()
                        .expect("event id field should build"),
                    Field::builder("happened_at", FieldType::date_time())
                        .build()
                        .expect("event datetime field should build"),
                ])
                .build()
                .expect("event model should build"),
        )
        .build()
        .expect("datetime schema should build")
}

fn integer_relation_schema() -> Schema {
    Schema::builder()
        .models(vec![
            Model::builder("parent")
                .fields(vec![
                    Field::builder("id", FieldType::int())
                        .attribute(Attribute::Id)
                        .build()
                        .expect("parent id field should build"),
                    Field::builder("children", FieldType::relation("child", false, true))
                        .build()
                        .expect("children relation should build"),
                ])
                .build()
                .expect("parent model should build"),
            Model::builder("child")
                .fields(vec![
                    Field::builder("id", FieldType::big_int())
                        .attribute(Attribute::Id)
                        .build()
                        .expect("child id field should build"),
                    Field::builder("parent_id", FieldType::int())
                        .build()
                        .expect("parent id field should build"),
                    Field::builder("parent", FieldType::relation("parent", false, false))
                        .attribute(Attribute::Relation(
                            RelationAttribute::builder()
                                .field("parent_id")
                                .reference("id")
                                .build()
                                .expect("parent relation metadata should build"),
                        ))
                        .build()
                        .expect("parent relation should build"),
                ])
                .build()
                .expect("child model should build"),
        ])
        .build()
        .expect("integer relation schema should build")
}

fn wide_relation_schema(width: usize) -> (Schema, Vec<&'static str>) {
    assert!(width >= 2);

    let mut selected_fields = vec!["id", "parent_id"];
    let mut child_fields = vec![
        Field::builder("id", FieldType::big_int())
            .attribute(Attribute::Id)
            .build()
            .expect("child id field should build"),
        Field::builder("parent_id", FieldType::int())
            .build()
            .expect("parent id field should build"),
    ];

    for index in 2..width {
        let field_name: &'static str = Box::leak(format!("field_{index}").into_boxed_str());
        selected_fields.push(field_name);
        child_fields.push(
            Field::builder(field_name, FieldType::int())
                .build()
                .expect("wide scalar field should build"),
        );
    }

    child_fields.push(
        Field::builder("parent", FieldType::relation("parent", false, false))
            .attribute(Attribute::Relation(
                RelationAttribute::builder()
                    .field("parent_id")
                    .reference("id")
                    .build()
                    .expect("wide relation metadata should build"),
            ))
            .build()
            .expect("wide parent relation should build"),
    );

    let schema = Schema::builder()
        .models(vec![
            Model::builder("parent")
                .fields(vec![
                    Field::builder("id", FieldType::int())
                        .attribute(Attribute::Id)
                        .build()
                        .expect("parent id field should build"),
                    Field::builder("children", FieldType::relation("child", false, true))
                        .build()
                        .expect("children relation should build"),
                ])
                .build()
                .expect("parent model should build"),
            Model::builder("child")
                .fields(child_fields)
                .build()
                .expect("wide child model should build"),
        ])
        .build()
        .expect("wide relation schema should build");

    (schema, selected_fields)
}

fn wide_relation_selection(selected_fields: Vec<&'static str>) -> QuerySelection {
    QuerySelection {
        model: "parent",
        scalar_fields: vec!["id"],
        relations: vec![QueryRelationSelection {
            field: "children",
            selection: QuerySelection {
                model: "child",
                scalar_fields: selected_fields,
                relations: Vec::new(),
                filter: None,
                order_by: Vec::new(),
                skip: None,
                limit: None,
            },
        }],
        filter: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    }
}

fn column_limit_models(column_count: usize) -> Vec<Model> {
    assert!(column_count >= 1);

    let mut parent_fields = vec![
        Field::builder("id", FieldType::int())
            .attribute(Attribute::Id)
            .build()
            .expect("parent id field should build"),
    ];

    for index in 1..column_count {
        parent_fields.push(
            Field::builder(format!("field_{index}"), FieldType::int())
                .build()
                .expect("column-limit field should build"),
        );
    }

    parent_fields.push(
        Field::builder("children", FieldType::relation("child", false, true))
            .build()
            .expect("children relation should build"),
    );

    vec![
        Model::builder("parent")
            .fields(parent_fields)
            .build()
            .expect("column-limit parent model should build"),
        Model::builder("child")
            .fields(vec![
                Field::builder("id", FieldType::int())
                    .attribute(Attribute::Id)
                    .build()
                    .expect("child id field should build"),
                Field::builder("parent_id", FieldType::int())
                    .build()
                    .expect("parent id field should build"),
                Field::builder("parent", FieldType::relation("parent", false, false))
                    .attribute(Attribute::Relation(
                        RelationAttribute::builder()
                            .field("parent_id")
                            .reference("id")
                            .build()
                            .expect("parent relation metadata should build"),
                    ))
                    .build()
                    .expect("parent relation should build"),
            ])
            .build()
            .expect("column-limit child model should build"),
    ]
}

fn migration_schema(name_optional: bool, include_legacy: bool) -> Schema {
    let mut models = vec![
        Model::builder("user")
            .fields(vec![
                Field::builder("id", FieldType::int())
                    .attribute(Attribute::Id)
                    .build()
                    .expect("user id field should build"),
                Field::builder("name", FieldType::scalar(ScalarType::String, name_optional))
                    .build()
                    .expect("user name field should build"),
            ])
            .build()
            .expect("user model should build"),
    ];

    if include_legacy {
        models.push(
            Model::builder("legacy")
                .fields(vec![
                    Field::builder("id", FieldType::int())
                        .attribute(Attribute::Id)
                        .build()
                        .expect("legacy id field should build"),
                ])
                .build()
                .expect("legacy model should build"),
        );
    }

    Schema::builder()
        .models(models)
        .build()
        .expect("migration schema should build")
}

#[test]
fn d1_queries_preserve_full_range_integers_and_numeric_semantics() {
    let schema = integer_schema();
    let selection = QuerySelection {
        model: "number",
        scalar_fields: vec!["id", "big"],
        relations: Vec::new(),
        filter: Some(QueryFilter::And(vec![
            QueryFilter::eq(
                "id",
                QueryFilterValue::Value(QueryVariableValue::Int(i64::MIN)),
            ),
            QueryFilter::r#in(
                "big",
                QueryFilterValues::values([
                    QueryVariableValue::Int(i64::MAX),
                    QueryVariableValue::Int(i64::MIN),
                ]),
            ),
        ])),
        order_by: vec![QueryOrder::scalar("big", QueryOrderDirection::Asc)],
        skip: Some(QueryPagination::value(7)),
        limit: Some(QueryPagination::value(i64::MAX)),
    };

    let statement = compile_query_with_flavor(
        &schema,
        &selection,
        &QueryVariables::new(),
        SqliteFamilyFlavor::D1,
    )
    .expect("D1 integer query should compile");

    assert_eq!(
        statement.sql(),
        r#"SELECT CAST("t0"."id" AS TEXT) AS "number__id", CAST("t0"."big" AS TEXT) AS "number__big" FROM "number" AS "t0" WHERE ("t0"."id" = CAST(?1 AS INTEGER) AND "t0"."big" IN (CAST(?2 AS INTEGER), CAST(?3 AS INTEGER))) ORDER BY "t0"."big" ASC LIMIT CAST(?4 AS INTEGER) OFFSET CAST(?5 AS INTEGER)"#
    );
    assert_eq!(
        statement.bindings(),
        &[
            BindingValue::Int(i64::MIN),
            BindingValue::Int(i64::MAX),
            BindingValue::Int(i64::MIN),
            BindingValue::Int(i64::MAX),
            BindingValue::Int(7),
        ]
    );

    assert!(!statement.sql().contains(r#"CAST("t0"."big" AS INTEGER)"#));
    assert!(!statement.sql().contains(r#"CAST("t0"."big" AS TEXT) ASC"#));
}

#[test]
fn d1_integer_writes_and_returning_rows_use_casts() {
    let schema = integer_schema();

    let insert = compile_insert_with_flavor(
        &schema,
        "number",
        &InsertValues::from_values(vec![
            ("id", InsertValue::Int(i64::MIN)),
            ("big", InsertValue::Int(i64::MAX)),
        ]),
        &["id", "big"],
        SqliteFamilyFlavor::D1,
    )
    .expect("D1 integer insert should compile");

    assert_eq!(
        insert.sql(),
        r#"INSERT INTO "number" ("id", "big") VALUES (CAST(?1 AS INTEGER), CAST(?2 AS INTEGER)) RETURNING CAST("number"."id" AS TEXT) AS "number__id", CAST("number"."big" AS TEXT) AS "number__big""#
    );
    assert_eq!(
        insert.bindings(),
        &[BindingValue::Int(i64::MIN), BindingValue::Int(i64::MAX),]
    );

    let filter = QueryFilter::eq(
        "id",
        QueryFilterValue::Value(QueryVariableValue::Int(i64::MIN)),
    );
    let update = compile_update_many_with_flavor(
        &schema,
        "number",
        &UpdateValues::from_values(vec![("big", UpdateValue::Int(i64::MAX))]),
        Some(&filter),
        &QueryVariables::new(),
        SqliteFamilyFlavor::D1,
    )
    .expect("D1 integer update should compile");

    assert_eq!(
        update.sql(),
        r#"UPDATE "number" AS "t0" SET "big" = CAST(?1 AS INTEGER) WHERE "t0"."id" = CAST(?2 AS INTEGER)"#
    );
    assert_eq!(
        update.bindings(),
        &[BindingValue::Int(i64::MAX), BindingValue::Int(i64::MIN),]
    );

    let delete = compile_delete_many_with_flavor(
        &schema,
        "number",
        Some(&filter),
        &QueryVariables::new(),
        SqliteFamilyFlavor::D1,
    )
    .expect("D1 integer delete should compile");

    assert_eq!(
        delete.sql(),
        r#"DELETE FROM "number" AS "t0" WHERE "t0"."id" = CAST(?1 AS INTEGER)"#
    );
    assert_eq!(delete.bindings(), &[BindingValue::Int(i64::MIN)]);
}

#[test]
fn datetime_writes_remain_raw_while_comparisons_are_normalized() {
    let schema = datetime_schema();
    let happened_at = chrono::DateTime::parse_from_rfc3339("2025-01-02T03:04:05.000000Z")
        .expect("test datetime should parse")
        .with_timezone(&chrono::Utc);
    let values = InsertValues::from_values(vec![
        ("id", InsertValue::Int(1)),
        ("happened_at", InsertValue::DateTime(happened_at)),
    ]);

    let native_insert = compile_insert_with_flavor(
        &schema,
        "event",
        &values,
        &["id", "happened_at"],
        SqliteFamilyFlavor::Native,
    )
    .expect("native datetime insert should compile");
    let d1_insert = compile_insert_with_flavor(
        &schema,
        "event",
        &values,
        &["id", "happened_at"],
        SqliteFamilyFlavor::D1,
    )
    .expect("D1 datetime insert should compile");

    assert_eq!(
        native_insert.sql(),
        r#"INSERT INTO "event" ("id", "happened_at") VALUES (?1, ?2) RETURNING "event"."id" AS "event__id", "event"."happened_at" AS "event__happened_at""#
    );
    assert_eq!(
        d1_insert.sql(),
        r#"INSERT INTO "event" ("id", "happened_at") VALUES (CAST(?1 AS INTEGER), ?2) RETURNING CAST("event"."id" AS TEXT) AS "event__id", "event"."happened_at" AS "event__happened_at""#
    );

    let filter = QueryFilter::eq(
        "happened_at",
        QueryFilterValue::Value(QueryVariableValue::DateTime(happened_at)),
    );
    let updates =
        UpdateValues::from_values(vec![("happened_at", UpdateValue::DateTime(happened_at))]);

    for flavor in [SqliteFamilyFlavor::Native, SqliteFamilyFlavor::D1] {
        let update = compile_update_many_with_flavor(
            &schema,
            "event",
            &updates,
            Some(&filter),
            &QueryVariables::new(),
            flavor,
        )
        .expect("datetime update should compile");

        assert_eq!(
            update.sql(),
            r#"UPDATE "event" AS "t0" SET "happened_at" = ?1 WHERE julianday("t0"."happened_at") = julianday(?2)"#
        );
    }
}

#[test]
fn d1_nested_integer_json_values_are_strings() {
    let schema = integer_relation_schema();
    let selection = QuerySelection {
        model: "parent",
        scalar_fields: vec!["id"],
        relations: vec![QueryRelationSelection {
            field: "children",
            selection: QuerySelection {
                model: "child",
                scalar_fields: vec!["id", "parent_id"],
                relations: Vec::new(),
                filter: None,
                order_by: Vec::new(),
                skip: None,
                limit: None,
            },
        }],
        filter: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let statement = compile_query_with_flavor(
        &schema,
        &selection,
        &QueryVariables::new(),
        SqliteFamilyFlavor::D1,
    )
    .expect("D1 nested integer query should compile");

    assert!(
        statement
            .sql()
            .contains(r#"json_array(CAST("t1"."id" AS TEXT), CAST("t1"."parent_id" AS TEXT))"#)
    );
    assert!(statement.sql().contains(r#""t1"."parent_id" = "t0"."id""#));
}

#[test]
fn d1_nested_json_compilation_respects_the_32_argument_floor() {
    for (width, expected_insertions) in [(32, 0), (33, 1), (100, 68)] {
        let (schema, fields) = wide_relation_schema(width);
        let selection = wide_relation_selection(fields);
        let statement = compile_query_with_flavor(
            &schema,
            &selection,
            &QueryVariables::new(),
            SqliteFamilyFlavor::D1,
        )
        .expect("wide D1 relation query should compile");

        assert_eq!(
            statement.sql().match_indices("json_insert(").count(),
            expected_insertions,
            "unexpected JSON chunking for {width} nested values",
        );

        if expected_insertions > 0 {
            assert!(statement.sql().contains("'$[#]'"));
        }
    }

    let (schema, fields) = wide_relation_schema(100);
    let native = compile_query(
        &schema,
        &wide_relation_selection(fields),
        &QueryVariables::new(),
    )
    .expect("wide native SQLite relation query should compile");

    assert!(!native.sql().contains("json_insert("));
}

#[test]
fn d1_accepts_100_bindings_and_rejects_101() {
    let schema = integer_schema();

    let selection_with_bindings = |count| QuerySelection {
        model: "number",
        scalar_fields: vec!["id"],
        relations: Vec::new(),
        filter: Some(QueryFilter::r#in(
            "id",
            QueryFilterValues::values(
                (0..count).map(|value| QueryVariableValue::Int(value as i64)),
            ),
        )),
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let accepted = compile_query_with_flavor(
        &schema,
        &selection_with_bindings(D1_MAX_BINDINGS),
        &QueryVariables::new(),
        SqliteFamilyFlavor::D1,
    )
    .expect("100 D1 bindings should be accepted");

    assert_eq!(accepted.bindings().len(), D1_MAX_BINDINGS);
    assert!(accepted.sql().contains("CAST(?100 AS INTEGER)"));

    let error = compile_query_with_flavor(
        &schema,
        &selection_with_bindings(D1_MAX_BINDINGS + 1),
        &QueryVariables::new(),
        SqliteFamilyFlavor::D1,
    )
    .expect_err("101 D1 bindings should be rejected");

    assert!(error.to_string().contains("query operation"));
    assert!(error.to_string().contains("101 bound parameters"));
    assert!(error.to_string().contains("allowed limit of 100"));
}

#[test]
fn d1_enforces_the_100_000_byte_sql_limit() {
    let accepted = CompiledStatement::new(
        SqliteFamilyFlavor::D1,
        "x".repeat(D1_MAX_SQL_BYTES),
        Vec::new(),
        Vec::new(),
        OperationKind::Query,
    )
    .expect("100,000 bytes of D1 SQL should be accepted");

    assert_eq!(accepted.sql().len(), D1_MAX_SQL_BYTES);

    let error = CompiledStatement::new(
        SqliteFamilyFlavor::D1,
        "x".repeat(D1_MAX_SQL_BYTES + 1),
        Vec::new(),
        Vec::new(),
        OperationKind::Query,
    )
    .expect_err("100,001 bytes of D1 SQL should be rejected");

    assert!(error.to_string().contains("100001 UTF-8 bytes"));
    assert!(error.to_string().contains("allowed limit of 100000 bytes"));

    CompiledStatement::new(
        SqliteFamilyFlavor::Native,
        "x".repeat(D1_MAX_SQL_BYTES + 1),
        Vec::new(),
        Vec::new(),
        OperationKind::Query,
    )
    .expect("native SQLite should not apply D1's SQL-size limit");
}

#[test]
fn d1_column_limit_counts_scalar_database_columns_only() {
    let accepted = Schema::builder()
        .models(column_limit_models(D1_MAX_COLUMNS))
        .build()
        .expect("100-column schema should build");

    validate_d1_schema(&accepted).expect("100 scalar columns plus a relation should be accepted");

    Schema::builder()
        .models(column_limit_models(D1_MAX_COLUMNS))
        .with_d1_platform_limits()
        .build()
        .expect("builder-level D1 validation should accept 100 columns");

    let rejected = Schema::builder()
        .models(column_limit_models(D1_MAX_COLUMNS + 1))
        .build()
        .expect("base SQLite schema should allow more than D1's limit");

    let errors =
        validate_d1_schema(&rejected).expect_err("101 scalar columns should be rejected for D1");
    let error = errors
        .iter()
        .next()
        .expect("column validation should return one error");

    assert_eq!(
        error.location,
        ValidationLocation::Model {
            model: "parent".to_owned(),
        }
    );
    assert!(error.message.contains("101 scalar database columns"));
    assert!(error.message.contains("limit of 100 columns per table"));

    Schema::builder()
        .models(column_limit_models(D1_MAX_COLUMNS + 1))
        .with_d1_platform_limits()
        .build()
        .expect_err("builder-level D1 validation should reject 101 columns");
}

#[test]
fn d1_migration_rendering_uses_script_level_deferred_foreign_keys() {
    let current = SqliteSchema::from_schema(&migration_schema(true, true));
    let target = SqliteSchema::from_schema(&migration_schema(false, false));
    let migration = target.migrate_from(&current);

    let native = migration.to_sql();
    assert!(native.contains(
        "-- DropTable\nPRAGMA foreign_keys=off;\nDROP TABLE \"legacy\";\nPRAGMA foreign_keys=on;"
    ));
    assert!(
        native
            .contains("-- RedefineTables\nPRAGMA defer_foreign_keys=ON;\nPRAGMA foreign_keys=OFF;")
    );

    let d1 = migration.to_d1_sql();
    assert!(d1.starts_with("PRAGMA defer_foreign_keys=ON;\n\n"));
    assert!(d1.ends_with("\n\nPRAGMA defer_foreign_keys=OFF;\n\n"));
    assert_eq!(d1.match_indices("PRAGMA defer_foreign_keys=ON;").count(), 1);
    assert_eq!(
        d1.match_indices("PRAGMA defer_foreign_keys=OFF;").count(),
        1
    );
    assert!(!d1.contains("PRAGMA foreign_keys"));
    assert!(d1.contains("-- DropTable\nDROP TABLE \"legacy\";"));
    assert!(d1.contains("-- RedefineTables\nCREATE TABLE \"new_user\""));
}

#[test]
fn d1_non_destructive_migrations_do_not_emit_deferred_foreign_keys() {
    let migration =
        SqliteSchema::from_schema(&integer_schema()).migrate_from(&SqliteSchema::empty());
    let d1 = migration.to_d1_sql();

    assert!(!d1.contains("PRAGMA defer_foreign_keys"));
    assert!(!d1.contains("PRAGMA foreign_keys"));
    assert!(d1.starts_with("-- CreateTable\n"));
}
