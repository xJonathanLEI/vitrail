pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{schema};
extern crate self as vitrail_pg;

schema! {
    model user {
        id Int @id
    }
}

fn main() {}
