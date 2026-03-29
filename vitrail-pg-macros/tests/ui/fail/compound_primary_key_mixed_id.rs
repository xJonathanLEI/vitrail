pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::schema;
extern crate self as vitrail_pg;

schema! {
    name compound_primary_key_mixed_id

    model like {
        post_id Int @id
        user_id Int

        @@id([post_id, user_id])
    }
}

fn main() {}
