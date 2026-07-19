#[path = "sqlite/support.rs"]
mod support;

#[path = "sqlite/insert.rs"]
mod insert;
pub(crate) use insert::sqlite_insert_schema as insert_schema;

#[path = "sqlite/bytes.rs"]
mod bytes;
pub(crate) use bytes::sqlite_bytes_schema as bytes_schema;

#[path = "sqlite/bigint.rs"]
mod bigint;
pub(crate) use bigint::sqlite_bigint_schema as bigint_schema;

#[path = "sqlite/custom_types.rs"]
mod custom_types;
pub(crate) use custom_types::sqlite_custom_types_schema as custom_types_schema;

#[path = "sqlite/migrations.rs"]
mod migrations;

#[path = "sqlite/migrator.rs"]
mod migrator;

#[path = "sqlite/query.rs"]
mod query;
pub(crate) use query::sqlite_query_schema as query_schema;

#[path = "sqlite/statements.rs"]
mod statements;
pub(crate) use statements::sqlite_compound_statements_schema as compound_statements_schema;
pub(crate) use statements::sqlite_statements_schema as statements_schema;

#[path = "sqlite/update.rs"]
mod update;
pub(crate) use update::sqlite_update_schema as update_schema;

#[path = "sqlite/delete.rs"]
mod delete;
pub(crate) use delete::sqlite_delete_schema as delete_schema;

#[path = "sqlite/transaction.rs"]
mod transaction;

vitrail_sqlite::schema! {
    name sqlite_facade_schema

    tables {
        external: ["external_audit_log"]
    }

    model user {
        id          Int      @id @default(autoincrement())
        external_id BigInt   @unique
        email       String   @unique
        active      Boolean
        created_at  DateTime @default(now())
    }
}

#[test]
fn sqlite_facade_exposes_generated_schema() {
    use vitrail_sqlite::SchemaAccess as _;

    let schema: &'static vitrail_sqlite::Schema = sqlite_facade_schema::Schema::schema();

    assert_eq!(schema.models().len(), 1);
    assert_eq!(schema.models()[0].name(), "user");
    assert_eq!(schema.external_tables(), &["external_audit_log".to_owned()]);
}
