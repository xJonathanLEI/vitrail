pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::schema;
extern crate self as vitrail_pg;

schema! {
    name compound_primary_key_unknown_field

    model like {
        post_id Int
        user_id Int

        @@id([post_id, missing_field])
    }
}

fn main() {}
