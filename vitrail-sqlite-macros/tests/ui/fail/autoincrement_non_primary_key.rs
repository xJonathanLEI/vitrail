pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::schema;
extern crate self as vitrail_sqlite;

schema! {
    name autoincrement_non_primary_key_schema

    model event {
        id       Int @id
        sequence Int @default(autoincrement())
    }
}

fn main() {}
