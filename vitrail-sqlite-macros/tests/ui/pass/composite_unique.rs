pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::schema;
extern crate self as vitrail_sqlite;

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
