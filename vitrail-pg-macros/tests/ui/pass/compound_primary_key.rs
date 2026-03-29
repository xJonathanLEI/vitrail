pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::schema;
extern crate self as vitrail_pg;

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
