pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::schema;
extern crate self as vitrail_sqlite;

schema! {
    name compound_primary_key_schema

    model user {
        id Int @id
        likes Like[]
    }

    model post {
        id Int @id
        likes Like[]
    }

    model like {
        post_id Int
        user_id Int
        post post @relation(fields: [post_id], references: [id])
        user user @relation(fields: [user_id], references: [id])

        @@id([post_id, user_id])
    }
}

fn main() {}
