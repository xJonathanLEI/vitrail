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
                    Field::builder("post", FieldType::relation("post", true, false))
                        .build()
                        .expect("field should build"),
                    Field::builder("comment", FieldType::relation("comment", true, false))
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
                    Field::builder("user", FieldType::relation("User", false, false))
                        .attributes(vec![Attribute::Relation(
                            RelationAttribute::builder()
                                .fields(vec!["user_id".into()])
                                .references(vec!["id".into()])
                                .build()
                                .expect("relation should build"),
                        )])
                        .build()
                        .expect("field should build"),
                    Field::builder("comment", FieldType::relation("comment", true, false))
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
                    Field::builder("post", FieldType::relation("post", false, false))
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
                    Field::builder("user", FieldType::relation("User", false, false))
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
                    Field::builder("post", FieldType::relation("Post", true, false))
                        .build()
                        .expect("field should build"),
                    Field::builder("comment", FieldType::relation("Comment", true, false))
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
                    Field::builder("user", FieldType::relation("user", false, false))
                        .attributes(vec![Attribute::Relation(
                            RelationAttribute::builder()
                                .fields(vec!["user_id".into()])
                                .references(vec!["id".into()])
                                .build()
                                .expect("relation should build"),
                        )])
                        .build()
                        .expect("field should build"),
                    Field::builder("comment", FieldType::relation("Comment", true, false))
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
                    Field::builder("post", FieldType::relation("post", false, false))
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
                    Field::builder("comment", FieldType::relation("Missing", true, false))
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
fn allows_inferred_one_to_many_relation_fields() {
    let schema = Schema::builder()
        .models(vec![
            Model::builder("user")
                .fields(vec![
                    Field::builder("id", FieldType::int())
                        .attributes(vec![Attribute::Id])
                        .build()
                        .expect("field should build"),
                    Field::builder("posts", FieldType::relation("Post", false, true))
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
                    Field::builder("author_id", FieldType::int())
                        .build()
                        .expect("field should build"),
                    Field::builder("author", FieldType::relation("user", false, false))
                        .attributes(vec![Attribute::Relation(
                            RelationAttribute::builder()
                                .fields(vec!["author_id".into()])
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
fn accepts_compound_primary_keys() {
    let schema = Schema::builder()
        .models(vec![
            Model::builder("user")
                .fields(vec![
                    Field::builder("id", FieldType::int())
                        .attributes(vec![Attribute::Id])
                        .build()
                        .expect("field should build"),
                    Field::builder("likes", FieldType::relation("Like", false, true))
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
                    Field::builder("likes", FieldType::relation("Like", false, true))
                        .build()
                        .expect("field should build"),
                ])
                .build()
                .expect("model should build"),
            Model::builder("like")
                .fields(vec![
                    Field::builder("post_id", FieldType::int())
                        .build()
                        .expect("field should build"),
                    Field::builder("user_id", FieldType::int())
                        .build()
                        .expect("field should build"),
                    Field::builder("post", FieldType::relation("post", false, false))
                        .attributes(vec![Attribute::Relation(
                            RelationAttribute::builder()
                                .fields(vec!["post_id".into()])
                                .references(vec!["id".into()])
                                .build()
                                .expect("relation should build"),
                        )])
                        .build()
                        .expect("field should build"),
                    Field::builder("user", FieldType::relation("user", false, false))
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
                .attributes(vec![ModelAttribute::Id(
                    ModelPrimaryKeyAttribute::builder()
                        .fields(vec!["post_id".into(), "user_id".into()])
                        .build()
                        .expect("primary key should build"),
                )])
                .build()
                .expect("model should build"),
        ])
        .build();

    assert!(schema.is_ok());
}

#[test]
fn rejects_mixing_field_and_model_primary_keys() {
    let error = Model::builder("like")
        .fields(vec![
            Field::builder("post_id", FieldType::int())
                .attributes(vec![Attribute::Id])
                .build()
                .expect("field should build"),
            Field::builder("user_id", FieldType::int())
                .build()
                .expect("field should build"),
        ])
        .attributes(vec![ModelAttribute::Id(
            ModelPrimaryKeyAttribute::builder()
                .fields(vec!["post_id".into(), "user_id".into()])
                .build()
                .expect("primary key should build"),
        )])
        .build()
        .expect_err("model should fail");

    assert!(
        error
            .to_string()
            .contains("cannot mix field-level `@id` with model-level `@@id`")
    );
}

#[test]
fn rejects_optional_fields_in_compound_primary_keys() {
    let error = Model::builder("like")
        .fields(vec![
            Field::builder("post_id", FieldType::scalar(ScalarType::Int, true))
                .build()
                .expect("field should build"),
            Field::builder("user_id", FieldType::int())
                .build()
                .expect("field should build"),
        ])
        .attributes(vec![ModelAttribute::Id(
            ModelPrimaryKeyAttribute::builder()
                .fields(vec!["post_id".into(), "user_id".into()])
                .build()
                .expect("primary key should build"),
        )])
        .build()
        .expect_err("model should fail");

    assert!(error.to_string().contains("must not be optional"));
}

#[test]
fn accepts_compound_unique_constraints() {
    let model = Model::builder("post_locale")
        .fields(vec![
            Field::builder("id", FieldType::int())
                .attributes(vec![Attribute::Id])
                .build()
                .expect("field should build"),
            Field::builder("post_id", FieldType::int())
                .build()
                .expect("field should build"),
            Field::builder("locale", FieldType::string())
                .build()
                .expect("field should build"),
            Field::builder("title", FieldType::string())
                .build()
                .expect("field should build"),
        ])
        .attributes(vec![ModelAttribute::Unique(
            ModelUniqueAttribute::builder()
                .fields(vec!["post_id".into(), "locale".into()])
                .build()
                .expect("unique attribute should build"),
        )])
        .build();

    assert!(model.is_ok());
}

#[test]
fn rejects_unknown_fields_in_compound_unique_constraints() {
    let error = Model::builder("post_locale")
        .fields(vec![
            Field::builder("post_id", FieldType::int())
                .build()
                .expect("field should build"),
            Field::builder("locale", FieldType::string())
                .build()
                .expect("field should build"),
        ])
        .attributes(vec![ModelAttribute::Unique(
            ModelUniqueAttribute::builder()
                .fields(vec!["post_id".into(), "missing".into()])
                .build()
                .expect("unique attribute should build"),
        )])
        .build()
        .expect_err("model should fail");

    assert!(error.to_string().contains("unknown unique field `missing`"));
}

#[test]
fn rejects_relation_fields_in_compound_unique_constraints() {
    let error = Model::builder("post_locale")
        .fields(vec![
            Field::builder("post_id", FieldType::int())
                .build()
                .expect("field should build"),
            Field::builder("post", FieldType::relation("post", false, false))
                .build()
                .expect("field should build"),
        ])
        .attributes(vec![ModelAttribute::Unique(
            ModelUniqueAttribute::builder()
                .fields(vec!["post_id".into(), "post".into()])
                .build()
                .expect("unique attribute should build"),
        )])
        .build()
        .expect_err("model should fail");

    assert!(
        error
            .to_string()
            .contains("unique field `post` must be scalar")
    );
}

#[test]
fn rejects_duplicate_fields_in_compound_unique_constraints() {
    let error = Model::builder("post_locale")
        .fields(vec![
            Field::builder("post_id", FieldType::int())
                .build()
                .expect("field should build"),
            Field::builder("locale", FieldType::string())
                .build()
                .expect("field should build"),
        ])
        .attributes(vec![ModelAttribute::Unique(
            ModelUniqueAttribute::builder()
                .fields(vec!["post_id".into(), "post_id".into()])
                .build()
                .expect("unique attribute should build"),
        )])
        .build()
        .expect_err("model should fail");

    assert!(
        error
            .to_string()
            .contains("duplicate unique field `post_id`")
    );
}

#[test]
fn accepts_compound_indexes() {
    let model = Model::builder("post_locale")
        .fields(vec![
            Field::builder("id", FieldType::int())
                .attributes(vec![Attribute::Id])
                .build()
                .expect("field should build"),
            Field::builder("post_id", FieldType::int())
                .build()
                .expect("field should build"),
            Field::builder("locale", FieldType::string())
                .build()
                .expect("field should build"),
            Field::builder("title", FieldType::string())
                .build()
                .expect("field should build"),
        ])
        .attributes(vec![ModelAttribute::Index(
            ModelIndexAttribute::builder()
                .fields(vec!["post_id".into(), "locale".into()])
                .build()
                .expect("index attribute should build"),
        )])
        .build();

    assert!(model.is_ok());
}

#[test]
fn rejects_unknown_fields_in_compound_indexes() {
    let error = Model::builder("post_locale")
        .fields(vec![
            Field::builder("post_id", FieldType::int())
                .build()
                .expect("field should build"),
            Field::builder("locale", FieldType::string())
                .build()
                .expect("field should build"),
        ])
        .attributes(vec![ModelAttribute::Index(
            ModelIndexAttribute::builder()
                .fields(vec!["post_id".into(), "missing".into()])
                .build()
                .expect("index attribute should build"),
        )])
        .build()
        .expect_err("model should fail");

    assert!(error.to_string().contains("unknown index field `missing`"));
}

#[test]
fn rejects_relation_fields_in_compound_indexes() {
    let error = Model::builder("post_locale")
        .fields(vec![
            Field::builder("post_id", FieldType::int())
                .build()
                .expect("field should build"),
            Field::builder("post", FieldType::relation("post", false, false))
                .build()
                .expect("field should build"),
        ])
        .attributes(vec![ModelAttribute::Index(
            ModelIndexAttribute::builder()
                .fields(vec!["post_id".into(), "post".into()])
                .build()
                .expect("index attribute should build"),
        )])
        .build()
        .expect_err("model should fail");

    assert!(
        error
            .to_string()
            .contains("index field `post` must be scalar")
    );
}

#[test]
fn rejects_duplicate_fields_in_compound_indexes() {
    let error = Model::builder("post_locale")
        .fields(vec![
            Field::builder("post_id", FieldType::int())
                .build()
                .expect("field should build"),
            Field::builder("locale", FieldType::string())
                .build()
                .expect("field should build"),
        ])
        .attributes(vec![ModelAttribute::Index(
            ModelIndexAttribute::builder()
                .fields(vec!["post_id".into(), "post_id".into()])
                .build()
                .expect("index attribute should build"),
        )])
        .build()
        .expect_err("model should fail");

    assert!(
        error
            .to_string()
            .contains("duplicate index field `post_id`")
    );
}

#[test]
fn allows_string_rust_type_override() {
    let field = Field::builder("postal_code", FieldType::string())
        .attributes(vec![Attribute::RustType(RustTypeAttribute::new(
            "PostalCode",
        ))])
        .build();

    assert!(field.is_ok());
}

#[test]
fn rejects_rust_type_override_on_non_string_field() {
    let error = Field::builder("user_id", FieldType::int())
        .attributes(vec![Attribute::RustType(RustTypeAttribute::new("UserId"))])
        .build()
        .expect_err("field should fail");

    assert!(
        error
            .to_string()
            .contains("only supported on `String` fields")
    );
}

#[test]
fn rejects_duplicate_rust_type_override() {
    let error = Field::builder("postal_code", FieldType::string())
        .attributes(vec![
            Attribute::RustType(RustTypeAttribute::new("PostalCode")),
            Attribute::RustType(RustTypeAttribute::new("PostalCode")),
        ])
        .build()
        .expect_err("field should fail");

    assert!(error.to_string().contains("duplicate `@rust_ty` attribute"));
}

#[test]
fn allows_db_uuid_on_string_field() {
    let field = Field::builder("uid", FieldType::string())
        .attributes(vec![Attribute::DbUuid])
        .build()
        .expect("field should build");

    assert!(field.has_db_uuid());
}

#[test]
fn rejects_duplicate_db_uuid_attribute() {
    let error = Field::builder("uid", FieldType::string())
        .attributes(vec![Attribute::DbUuid, Attribute::DbUuid])
        .build()
        .expect_err("field should fail");

    assert!(error.to_string().contains("duplicate `@db.Uuid` attribute"));
}

#[test]
fn rejects_db_uuid_on_non_string_field() {
    let error = Field::builder("uid", FieldType::int())
        .attributes(vec![Attribute::DbUuid])
        .build()
        .expect_err("field should fail");

    assert!(
        error
            .to_string()
            .contains("`@db.Uuid` is only supported on `String` fields")
    );
}

#[test]
fn rejects_combining_db_uuid_with_rust_type_override() {
    let error = Field::builder("uid", FieldType::string())
        .attributes(vec![
            Attribute::DbUuid,
            Attribute::RustType(RustTypeAttribute::new("UserId")),
        ])
        .build()
        .expect_err("field should fail");

    assert!(
        error
            .to_string()
            .contains("`@rust_ty` cannot be combined with `@db.Uuid`")
    );
}
