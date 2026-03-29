pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::schema;
extern crate self as vitrail_pg;

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
