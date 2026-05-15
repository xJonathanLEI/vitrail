pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::schema;
extern crate self as vitrail_pg;

schema! {
    name my_schema

    model user {
        id       Int       @id
        profiles profile[]
    }

    model profile {
        id      Int  @id
        user_id Int  @unique
        user    user @relation(fields: [user_id], references: [id])
    }
}

fn main() {}
