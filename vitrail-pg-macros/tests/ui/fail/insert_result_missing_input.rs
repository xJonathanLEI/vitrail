pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{InsertResult, schema};
extern crate self as vitrail_pg;

schema! {
    name insert_schema

    model user {
        id    Int    @id @default(autoincrement())
        email String
    }
}

#[derive(InsertResult)]
#[vitrail(schema = crate::insert_schema::Schema, model = user)]
struct User {
    id: i64,
    email: String,
}

fn main() {}
