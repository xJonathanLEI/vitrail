pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::schema;
extern crate self as vitrail_sqlite;

schema! {
    name bigint_autoincrement_schema

    model event {
        id BigInt @id @default(autoincrement())
    }
}

fn main() {}
