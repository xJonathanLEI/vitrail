#[path = "sqlite/support.rs"]
mod support;

#[path = "sqlite/migrations.rs"]
mod migrations;

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
