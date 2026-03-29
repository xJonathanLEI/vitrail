pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::schema;
extern crate self as vitrail_pg;

schema! {
    name composite_unique_schema

    model post {
        id      Int           @id
        locales post_locale[]
    }

    model post_locale {
        id      Int    @id
        post_id Int
        locale  String
        title   String
        post    post   @relation(fields: [post_id], references: [id])

        @@unique([post_id, locale])
    }
}

fn main() {}
