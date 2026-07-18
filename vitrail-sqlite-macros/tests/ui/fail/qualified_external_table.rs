pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::schema;
extern crate self as vitrail_sqlite;

schema! {
    name qualified_external_table_schema

    tables {
        external: ["main.external_audit_log"]
    }

    model user {
        id Int @id @default(autoincrement())
    }
}

fn main() {}
