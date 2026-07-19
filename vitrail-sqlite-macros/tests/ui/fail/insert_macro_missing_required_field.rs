pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::{InsertInput, InsertResult, insert, schema};
extern crate self as vitrail_sqlite;

schema! {
    name insert_schema

    model user {
        id    Int    @id @default(autoincrement())
        email String
        name  String
    }
}

fn main() {
    let _ = insert! {
        crate::insert_schema,
        user {
            data: {
                name: "Alice".to_owned(),
            },
        }
    };
}
