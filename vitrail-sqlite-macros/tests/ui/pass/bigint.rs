pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::schema;
extern crate self as vitrail_sqlite;

schema! {
    name bigint_schema

    model event {
        id          BigInt @id
        description String
    }
}

fn main() {}
