pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::schema;
extern crate self as vitrail_sqlite;

schema! {
    name decimal_schema

    model account {
        id      Int     @id @default(autoincrement())
        balance Decimal
    }
}

fn main() {}
