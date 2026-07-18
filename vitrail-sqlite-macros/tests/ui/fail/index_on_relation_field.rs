pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::schema;
extern crate self as vitrail_sqlite;

schema! {
    name index_on_relation_field_schema

    model user {
        id    Int    @id
        posts post[] @index
    }

    model post {
        id        Int  @id
        author_id Int
        author    user @relation(fields: [author_id], references: [id])
    }
}

fn main() {}
