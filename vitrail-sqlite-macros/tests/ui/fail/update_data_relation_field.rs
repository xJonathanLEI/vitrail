pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::{UpdateData, schema};
extern crate self as vitrail_sqlite;

schema! {
    name update_schema

    model user {
        id    Int    @id @default(autoincrement())
        email String
        posts post[]
    }

    model post {
        id        Int    @id @default(autoincrement())
        title     String
        author_id Int
        author    user   @relation(fields: [author_id], references: [id])
    }
}

#[derive(UpdateData)]
#[vitrail(schema = crate::update_schema::Schema, model = post)]
struct UpdatePostData {
    title: String,
    author: i64,
}

fn main() {}
