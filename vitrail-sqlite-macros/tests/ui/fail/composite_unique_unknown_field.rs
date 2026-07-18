pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::schema;
extern crate self as vitrail_sqlite;

schema! {
    name composite_unique_unknown_field_schema

    model post_locale {
        id      Int    @id
        post_id Int
        locale  String

        @@unique([post_id, missing_field])
    }
}

fn main() {}
