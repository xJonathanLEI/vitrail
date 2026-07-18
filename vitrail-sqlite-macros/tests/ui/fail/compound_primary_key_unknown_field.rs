pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::schema;
extern crate self as vitrail_sqlite;

schema! {
    name compound_primary_key_unknown_field

    model like {
        post_id Int
        user_id Int

        @@id([post_id, missing_field])
    }
}

fn main() {}
