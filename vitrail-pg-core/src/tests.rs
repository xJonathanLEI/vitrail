use crate::*;

#[test]
fn accepts_valid_schema_definition() {
    let schema = Schema::builder()
        .models(vec![
            Model::builder("user")
                .fields(vec![
                    Field::builder("id", FieldType::int())
                        .attributes(vec![
                            Attribute::Id,
                            Attribute::Default(DefaultAttribute::autoincrement()),
                        ])
                        .build()
                        .expect("field should build"),
                    Field::builder("uid", FieldType::string())
                        .attributes(vec![Attribute::Unique, Attribute::DbUuid])
                        .build()
                        .expect("field should build"),
                    Field::builder("post", FieldType::relation("post", true))
                        .build()
                        .expect("field should build"),
                    Field::builder("comment", FieldType::relation("comment", true))
                        .build()
                        .expect("field should build"),
                    Field::builder("status", FieldType::string())
                        .build()
                        .expect("field should build"),
                ])
                .build()
                .expect("model should build"),
            Model::builder("post")
                .fields(vec![
                    Field::builder("id", FieldType::int())
                        .attributes(vec![
                            Attribute::Id,
                            Attribute::Default(DefaultAttribute::autoincrement()),
                        ])
                        .build()
                        .expect("field should build"),
                    Field::builder("uid", FieldType::string())
                        .attributes(vec![Attribute::Unique, Attribute::DbUuid])
                        .build()
                        .expect("field should build"),
                    Field::builder("user_id", FieldType::int())
                        .attributes(vec![Attribute::Unique])
                        .build()
                        .expect("field should build"),
                    Field::builder("created_at", FieldType::date_time())
                        .attributes(vec![Attribute::Default(DefaultAttribute::now())])
                        .build()
                        .expect("field should build"),
                    Field::builder("user", FieldType::relation("User", false))
                        .attributes(vec![Attribute::Relation(
                            RelationAttribute::builder()
                                .fields(vec!["user_id".into()])
                                .references(vec!["id".into()])
                                .build()
                                .expect("relation should build"),
                        )])
                        .build()
                        .expect("field should build"),
                    Field::builder("comment", FieldType::relation("comment", true))
                        .build()
                        .expect("field should build"),
                ])
                .build()
                .expect("model should build"),
            Model::builder("comment")
                .fields(vec![
                    Field::builder("id", FieldType::int())
                        .attributes(vec![
                            Attribute::Id,
                            Attribute::Default(DefaultAttribute::autoincrement()),
                        ])
                        .build()
                        .expect("field should build"),
                    Field::builder("post_id", FieldType::int())
                        .attributes(vec![Attribute::Unique])
                        .build()
                        .expect("field should build"),
                    Field::builder("body", FieldType::string())
                        .build()
                        .expect("field should build"),
                    Field::builder("post", FieldType::relation("post", false))
                        .attributes(vec![Attribute::Relation(
                            RelationAttribute::builder()
                                .fields(vec!["post_id".into()])
                                .references(vec!["id".into()])
                                .build()
                                .expect("relation should build"),
                        )])
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
    let error = Model::builder("user")
        .fields(vec![
            Field::builder("id", FieldType::int())
                .attributes(vec![Attribute::Id])
                .build_for_model("user")
                .expect("field should build"),
            Field::builder("id", FieldType::int())
                .build_for_model("user")
                .expect("field should build"),
        ])
        .build()
        .expect_err("model should fail");

    assert!(error.to_string().contains("duplicate field"));
}

#[test]
fn rejects_unknown_relation_target() {
    let schema = Schema::builder()
        .model(
            Model::builder("post")
                .fields(vec![
                    Field::builder("id", FieldType::int())
                        .attributes(vec![Attribute::Id])
                        .build()
                        .expect("field should build"),
                    Field::builder("user_id", FieldType::int())
                        .build()
                        .expect("field should build"),
                    Field::builder("user", FieldType::relation("User", false))
                        .attributes(vec![Attribute::Relation(
                            RelationAttribute::builder()
                                .fields(vec!["user_id".into()])
                                .references(vec!["id".into()])
                                .build()
                                .expect("relation should build"),
                        )])
                        .build()
                        .expect("field should build"),
                ])
                .build()
                .expect("model should build"),
        )
        .build();

    let error = schema.expect_err("schema should fail");
    assert!(error.to_string().contains("unknown relation target model"));
}

#[test]
fn rejects_invalid_default_usage() {
    let error = Field::builder("id", FieldType::string())
        .attributes(vec![
            Attribute::Id,
            Attribute::Default(DefaultAttribute::autoincrement()),
        ])
        .build()
        .expect_err("field should fail");

    assert!(error.to_string().contains("only supported on `Int` fields"));
}

#[test]
fn allows_inferred_relation_fields() {
    let schema = Schema::builder()
        .models(vec![
            Model::builder("user")
                .fields(vec![
                    Field::builder("id", FieldType::int())
                        .attributes(vec![Attribute::Id])
                        .build()
                        .expect("field should build"),
                    Field::builder("post", FieldType::relation("Post", true))
                        .build()
                        .expect("field should build"),
                    Field::builder("comment", FieldType::relation("Comment", true))
                        .build()
                        .expect("field should build"),
                ])
                .build()
                .expect("model should build"),
            Model::builder("post")
                .fields(vec![
                    Field::builder("id", FieldType::int())
                        .attributes(vec![Attribute::Id])
                        .build()
                        .expect("field should build"),
                    Field::builder("user_id", FieldType::int())
                        .build()
                        .expect("field should build"),
                    Field::builder("user", FieldType::relation("user", false))
                        .attributes(vec![Attribute::Relation(
                            RelationAttribute::builder()
                                .fields(vec!["user_id".into()])
                                .references(vec!["id".into()])
                                .build()
                                .expect("relation should build"),
                        )])
                        .build()
                        .expect("field should build"),
                    Field::builder("comment", FieldType::relation("Comment", true))
                        .build()
                        .expect("field should build"),
                ])
                .build()
                .expect("model should build"),
            Model::builder("comment")
                .fields(vec![
                    Field::builder("id", FieldType::int())
                        .attributes(vec![Attribute::Id])
                        .build()
                        .expect("field should build"),
                    Field::builder("post_id", FieldType::int())
                        .build()
                        .expect("field should build"),
                    Field::builder("post", FieldType::relation("post", false))
                        .attributes(vec![Attribute::Relation(
                            RelationAttribute::builder()
                                .fields(vec!["post_id".into()])
                                .references(vec!["id".into()])
                                .build()
                                .expect("relation should build"),
                        )])
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
fn rejects_unknown_inferred_relation_target() {
    let schema = Schema::builder()
        .model(
            Model::builder("user")
                .fields(vec![
                    Field::builder("id", FieldType::int())
                        .attributes(vec![Attribute::Id])
                        .build()
                        .expect("field should build"),
                    Field::builder("comment", FieldType::relation("Missing", true))
                        .build()
                        .expect("field should build"),
                ])
                .build()
                .expect("model should build"),
        )
        .build();

    let error = schema.expect_err("schema should fail");
    assert!(error.to_string().contains("unknown relation target model"));
}
