pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::schema;
extern crate self as vitrail_sqlite;

schema! {
    name my_schema

    model user {
        id      Int     @id
        profile profile
    }

    model profile {
        id      Int  @id
        user_id Int  @unique
        user    user @relation(fields: [user_id], references: [id])
    }
}

fn main() {}
