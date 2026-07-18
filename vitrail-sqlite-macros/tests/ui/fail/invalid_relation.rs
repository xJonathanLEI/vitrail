pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::{schema};
extern crate self as vitrail_sqlite;

schema! {
    name my_schema

    model user {
        id Int @id
    }

    model post {
        id      Int  @id
        user_id Int
        user    user @relation(fields: [missing_field], references: [id])
    }
}

fn main() {}
