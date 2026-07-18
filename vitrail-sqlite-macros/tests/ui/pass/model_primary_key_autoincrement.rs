pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::schema;
extern crate self as vitrail_sqlite;

schema! {
    name model_primary_key_autoincrement_schema

    model event {
        id Int @default(autoincrement())

        @@id([id])
    }
}

fn main() {}
