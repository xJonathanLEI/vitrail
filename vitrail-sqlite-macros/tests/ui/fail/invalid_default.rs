pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::{schema};
extern crate self as vitrail_sqlite;

schema! {
    name my_schema

    model user {
        id String @id @default(autoincrement())
    }
}

fn main() {}
