pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::schema;
extern crate self as vitrail_sqlite;

schema! {
    name compound_primary_key_mixed_id

    model like {
        post_id Int @id
        user_id Int

        @@id([post_id, user_id])
    }
}

fn main() {}
