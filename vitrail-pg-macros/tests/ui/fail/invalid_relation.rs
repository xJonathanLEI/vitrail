pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{schema};
extern crate self as vitrail_pg;

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
