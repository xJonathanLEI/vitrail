use std::error::Error as _;
use std::fmt;
use std::sync::OnceLock;

use serde_json::json;

use crate::query::compile_query_statement;
use crate::row::D1RowMetadata;
use crate::{
    Attribute, D1Row, Error, Field, FieldType, Insert, InsertModel, InsertValue, InsertValues,
    Model, Query, QueryFilter, QueryModel, QuerySelection, QueryValue, QueryVariables, Schema,
    SchemaAccess, decode_error, json_as_bool, json_as_bytes, json_as_i64,
};

struct TestSchema;

impl SchemaAccess for TestSchema {
    fn schema() -> &'static Schema {
        static SCHEMA: OnceLock<Schema> = OnceLock::new();

        SCHEMA.get_or_init(|| {
            Schema::builder()
                .with_d1_platform_limits()
                .model(
                    Model::builder("number")
                        .fields(vec![
                            Field::builder("id", FieldType::int())
                                .attribute(Attribute::Id)
                                .build()
                                .expect("id field should build"),
                            Field::builder("big", FieldType::big_int())
                                .build()
                                .expect("big integer field should build"),
                        ])
                        .build()
                        .expect("number model should build"),
                )
                .build()
                .expect("test schema should build")
        })
    }
}

struct NumberRow;

impl QueryValue for NumberRow {
    fn from_json(_value: &serde_json::Value) -> Result<Self, Error> {
        unreachable!("JSON decoding is not used by these compilation tests")
    }
}

impl QueryModel for NumberRow {
    type Schema = TestSchema;
    type Variables = ();

    fn model_name() -> &'static str {
        "number"
    }

    fn selection() -> QuerySelection {
        QuerySelection {
            model: "number",
            scalar_fields: vec!["id", "big"],
            relations: Vec::new(),
            filter: Some(QueryFilter::eq("id", i64::MAX)),
            order_by: Vec::new(),
            skip: None,
            limit: None,
        }
    }

    fn from_row(_row: &D1Row, _prefix: &str) -> Result<Self, Error> {
        unreachable!("row decoding is not used by these compilation tests")
    }
}

struct InsertedNumber;

impl InsertModel for InsertedNumber {
    type Schema = TestSchema;
    type Values = InsertValues;

    fn model_name() -> &'static str {
        "number"
    }

    fn returning_fields() -> &'static [&'static str] {
        &["id", "big"]
    }

    fn from_row(_row: &D1Row, _prefix: &str) -> Result<Self, Error> {
        unreachable!("row decoding is not used by these compilation tests")
    }
}

#[test]
fn query_compilation_uses_d1_integer_transport_rules() {
    let query = Query::<TestSchema, NumberRow>::new();
    let sql = query.to_sql().expect("D1 query should compile");

    assert_eq!(
        sql,
        r#"SELECT CAST("t0"."id" AS TEXT) AS "number__id", CAST("t0"."big" AS TEXT) AS "number__big" FROM "number" AS "t0" WHERE "t0"."id" = CAST(?1 AS INTEGER)"#,
    );
}

#[test]
fn duplicate_result_aliases_are_rejected_before_row_decoding() {
    let selection = QuerySelection {
        model: "number",
        scalar_fields: vec!["id", "id"],
        relations: Vec::new(),
        filter: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };
    let statement =
        compile_query_statement(TestSchema::schema(), &selection, &QueryVariables::new())
            .expect("query with duplicate selections should compile");

    let error = D1RowMetadata::new(statement.result_columns())
        .expect_err("duplicate result aliases should be rejected");

    assert!(matches!(error, Error::Decode(_)));
    assert!(error.to_string().contains("duplicate alias `number__id`"));
}

#[test]
fn insert_compilation_casts_integer_bindings_and_returning_columns() {
    let insert =
        Insert::<TestSchema, InsertedNumber>::with_values(InsertValues::from_values(vec![
            ("id", InsertValue::Int(i64::MIN)),
            ("big", InsertValue::Int(i64::MAX)),
        ]));
    let sql = insert.to_sql().expect("D1 insert should compile");

    assert_eq!(
        sql,
        r#"INSERT INTO "number" ("id", "big") VALUES (CAST(?1 AS INTEGER), CAST(?2 AS INTEGER)) RETURNING CAST("number"."id" AS TEXT) AS "number__id", CAST("number"."big" AS TEXT) AS "number__big""#,
    );
}

#[test]
fn nested_integer_decoder_requires_exact_decimal_strings() {
    assert_eq!(
        json_as_i64(&json!("-9223372036854775808")).expect("i64::MIN text should decode"),
        i64::MIN,
    );
    assert_eq!(
        json_as_i64(&json!("9223372036854775807")).expect("i64::MAX text should decode"),
        i64::MAX,
    );

    let error = json_as_i64(&json!(9_007_199_254_740_991_i64))
        .expect_err("numeric JSON must not be accepted for exact i64 decoding");

    assert!(matches!(error, Error::Decode(_)));
    assert!(
        error
            .to_string()
            .contains("expected decimal integer string")
    );
}

#[test]
fn boolean_decoder_accepts_boolean_and_numeric_d1_transports() {
    assert!(json_as_bool(&json!(true)).expect("boolean true should decode"));
    assert!(!json_as_bool(&json!(false)).expect("boolean false should decode"));
    assert!(!json_as_bool(&json!(0)).expect("numeric zero should decode as false"));
    assert!(json_as_bool(&json!(1)).expect("numeric one should decode as true"));
    assert!(json_as_bool(&json!(-7)).expect("numeric nonzero should decode as true"));
}

#[test]
fn nested_blob_decoder_accepts_hex_and_byte_arrays() {
    assert_eq!(
        json_as_bytes(&json!("00017F80FEFF")).expect("hex BLOB should decode"),
        vec![0, 1, 127, 128, 254, 255],
    );
    assert_eq!(
        json_as_bytes(&json!([0, 1, 127, 128, 254, 255])).expect("byte-array BLOB should decode"),
        vec![0, 1, 127, 128, 254, 255],
    );

    let error = json_as_bytes(&json!([256])).expect_err("out-of-range byte should be rejected");

    assert!(matches!(error, Error::Decode(_)));
}

#[derive(Debug)]
struct CustomStringDecodeError;

impl fmt::Display for CustomStringDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid custom string")
    }
}

impl std::error::Error for CustomStringDecodeError {}

#[test]
fn custom_decode_error_preserves_its_source() {
    let error = decode_error(CustomStringDecodeError);

    assert!(matches!(error, Error::Decode(_)));
    assert_eq!(
        error.source().map(ToString::to_string).as_deref(),
        Some("invalid custom string"),
    );
}
