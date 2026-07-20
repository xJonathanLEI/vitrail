use crate::{
    Attribute, BindingValue, DefaultAttribute, Field, FieldType, InsertValue, InsertValues, Model,
    OperationKind, QueryFilter, QueryFilterValue, QueryRelationSelection, QuerySelection,
    QueryVariableValue, QueryVariables, RelationAttribute, ResultColumn, ScalarType, Schema,
    SqliteSchema, UpdateValue, UpdateValues, compile_delete_many, compile_insert, compile_query,
    compile_update_many,
};

fn test_schema() -> Schema {
    Schema::builder()
        .model(
            Model::builder("user")
                .fields(vec![
                    Field::builder("id", FieldType::int())
                        .attributes(vec![
                            Attribute::Id,
                            Attribute::Default(DefaultAttribute::autoincrement()),
                        ])
                        .build()
                        .expect("id field should build"),
                    Field::builder("email", FieldType::string())
                        .build()
                        .expect("email field should build"),
                    Field::builder("active", FieldType::scalar(ScalarType::Boolean, false))
                        .build()
                        .expect("active field should build"),
                ])
                .build()
                .expect("user model should build"),
        )
        .build()
        .expect("test schema should build")
}

fn relation_schema() -> Schema {
    Schema::builder()
        .models(vec![
            Model::builder("user")
                .fields(vec![
                    Field::builder("id", FieldType::int())
                        .attribute(Attribute::Id)
                        .build()
                        .expect("user id field should build"),
                    Field::builder("posts", FieldType::relation("post", false, true))
                        .build()
                        .expect("posts relation should build"),
                ])
                .build()
                .expect("user model should build"),
            Model::builder("post")
                .fields(vec![
                    Field::builder("id", FieldType::int())
                        .attribute(Attribute::Id)
                        .build()
                        .expect("post id field should build"),
                    Field::builder("author_id", FieldType::int())
                        .build()
                        .expect("author id field should build"),
                    Field::builder("author", FieldType::relation("user", false, false))
                        .attribute(Attribute::Relation(
                            RelationAttribute::builder()
                                .field("author_id")
                                .reference("id")
                                .build()
                                .expect("author relation metadata should build"),
                        ))
                        .build()
                        .expect("author relation should build"),
                ])
                .build()
                .expect("post model should build"),
        ])
        .build()
        .expect("relation schema should build")
}

#[test]
fn query_compilation_returns_normalized_bindings_and_result_metadata() {
    let schema = test_schema();
    let selection = QuerySelection {
        model: "user",
        scalar_fields: vec!["id", "email"],
        relations: Vec::new(),
        filter: Some(QueryFilter::eq("id", QueryFilterValue::variable("user_id"))),
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };
    let variables = QueryVariables::from_values(vec![("user_id", QueryVariableValue::Int(7))]);

    let statement = compile_query(&schema, &selection, &variables).expect("query should compile");

    assert_eq!(
        statement.sql(),
        r#"SELECT "t0"."id" AS "user__id", "t0"."email" AS "user__email" FROM "user" AS "t0" WHERE "t0"."id" = ?1"#
    );
    assert_eq!(statement.operation(), OperationKind::Query);
    assert_eq!(statement.bindings(), &[BindingValue::Int(7)]);
    assert_eq!(
        statement.result_columns(),
        &[
            ResultColumn::scalar("user__id", ScalarType::Int, false),
            ResultColumn::scalar("user__email", ScalarType::String, false),
        ]
    );
}

#[test]
fn query_compilation_describes_root_relation_result_columns() {
    let schema = relation_schema();
    let selection = QuerySelection {
        model: "user",
        scalar_fields: vec!["id"],
        relations: vec![QueryRelationSelection {
            field: "posts",
            selection: QuerySelection {
                model: "post",
                scalar_fields: vec!["id"],
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

    let statement = compile_query(&schema, &selection, &QueryVariables::new())
        .expect("relation query should compile");

    assert_eq!(
        statement.result_columns(),
        &[
            ResultColumn::scalar("user__id", ScalarType::Int, false),
            ResultColumn::relation("user__posts", true, false),
        ]
    );
}

#[test]
fn write_compilers_preserve_binding_order_and_operation_kinds() {
    let schema = test_schema();

    let insert = compile_insert(
        &schema,
        "user",
        &InsertValues::from_values(vec![
            ("email", InsertValue::from("alice@example.com")),
            ("active", InsertValue::from(true)),
        ]),
        &["id", "email"],
    )
    .expect("insert should compile");

    assert_eq!(
        insert.sql(),
        r#"INSERT INTO "user" ("email", "active") VALUES (?1, ?2) RETURNING "user"."id" AS "user__id", "user"."email" AS "user__email""#
    );
    assert_eq!(insert.operation(), OperationKind::Insert);
    assert_eq!(
        insert.bindings(),
        &[
            BindingValue::String("alice@example.com".to_owned()),
            BindingValue::Bool(true),
        ]
    );
    assert_eq!(
        insert.result_columns(),
        &[
            ResultColumn::scalar("user__id", ScalarType::Int, false),
            ResultColumn::scalar("user__email", ScalarType::String, false),
        ]
    );

    let variables = QueryVariables::from_values(vec![(
        "email",
        QueryVariableValue::String("alice@example.com".to_owned()),
    )]);
    let filter = QueryFilter::eq("email", QueryFilterValue::variable("email"));
    let update = compile_update_many(
        &schema,
        "user",
        &UpdateValues::from_values(vec![("active", UpdateValue::from(false))]),
        Some(&filter),
        &variables,
    )
    .expect("update should compile");

    assert_eq!(
        update.sql(),
        r#"UPDATE "user" AS "t0" SET "active" = ?1 WHERE "t0"."email" = ?2"#
    );
    assert_eq!(update.operation(), OperationKind::UpdateMany);
    assert_eq!(
        update.bindings(),
        &[
            BindingValue::Bool(false),
            BindingValue::String("alice@example.com".to_owned()),
        ]
    );
    assert!(update.result_columns().is_empty());

    let delete = compile_delete_many(&schema, "user", Some(&filter), &variables)
        .expect("delete should compile");

    assert_eq!(
        delete.sql(),
        r#"DELETE FROM "user" AS "t0" WHERE "t0"."email" = ?1"#
    );
    assert_eq!(delete.operation(), OperationKind::DeleteMany);
    assert_eq!(
        delete.bindings(),
        &[BindingValue::String("alice@example.com".to_owned())]
    );
    assert!(delete.result_columns().is_empty());
}

#[test]
fn collection_validation_uses_backend_neutral_compile_errors() {
    let mut variables = QueryVariables::new();
    variables
        .push("id", QueryVariableValue::Int(1))
        .expect("first variable should be accepted");

    let error = variables
        .push("id", QueryVariableValue::Int(2))
        .expect_err("duplicate variable should be rejected");

    assert_eq!(error.message(), "duplicate query variable `id`");
    assert_eq!(error.to_string(), "duplicate query variable `id`");
}

#[test]
fn native_migration_rendering_is_available_without_a_runtime() {
    let migration = SqliteSchema::from_schema(&test_schema())
        .migrate_from(&SqliteSchema::empty())
        .to_sql();

    assert_eq!(
        migration,
        concat!(
            "-- CreateTable\n",
            "CREATE TABLE \"user\" (\n",
            "    \"id\" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,\n",
            "    \"email\" TEXT NOT NULL,\n",
            "    \"active\" BOOLEAN NOT NULL\n",
            ");\n\n",
        )
    );
}
