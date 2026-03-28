pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{schema};
extern crate self as vitrail_pg;

schema! {
    name my_schema

    model post {
        id      Int  @id
        user_id Int
        user    User @relation(fields: [user_id], references: [id])
    }
}

fn main() {}
