#[path = "sqlite/support.rs"]
mod support;

#[path = "sqlite/insert.rs"]
mod insert;
pub(crate) use insert::sqlite_insert_schema as insert_schema;

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
