pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::schema;
extern crate self as vitrail_sqlite;

schema! {
    name indexes_schema

    model user {
        id    Int    @id
        email String @unique
        name  String @index
        posts post[]
    }

    model post {
        id        Int     @id
        author_id Int     @index
        title     String
        author    user    @relation(fields: [author_id], references: [id])

        @@index([title, author_id])
    }
}

fn main() {}
