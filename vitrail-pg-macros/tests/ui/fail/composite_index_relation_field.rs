pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::schema;
extern crate self as vitrail_pg;

schema! {
    name composite_index_relation_field_schema

    model user {
        id    Int    @id
        posts post[]
    }

    model post {
        id        Int  @id
        author_id Int
        author    user @relation(fields: [author_id], references: [id])

        @@index([author_id, author])
    }
}

fn main() {}
