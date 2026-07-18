pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::{schema};
extern crate self as vitrail_sqlite;

schema! {
    model user {
        id Int @id
    }
}

fn main() {}
