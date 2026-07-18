pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::schema;
extern crate self as vitrail_sqlite;

schema! {
    name external_tables_schema

    tables {
        external: ["external_audit_log", "legacy_events"]
    }

    model user {
        id    Int    @id @default(autoincrement())
        email String @unique
    }
}

fn main() {}
