pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::{InsertInput, schema};
extern crate self as vitrail_sqlite;

schema! {
    name insert_schema

    model user {
        id    Int    @id @default(autoincrement())
        email String
        name  String
    }
}

#[derive(InsertInput)]
#[vitrail(schema = crate::insert_schema::Schema, model = user)]
struct NewUser {
    name: String,
}

fn main() {}
