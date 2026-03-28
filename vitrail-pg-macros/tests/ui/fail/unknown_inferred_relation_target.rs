pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{schema};
extern crate self as vitrail_pg;

schema! {
    name my_schema

    model user {
        id      Int      @id
        comment Missing?
    }
}

fn main() {}
