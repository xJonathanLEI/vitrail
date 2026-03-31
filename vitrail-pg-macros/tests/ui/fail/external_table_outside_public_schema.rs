pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::schema;
extern crate self as vitrail_pg;

schema! {
    name my_schema

    tables {
        external: ["auth.users"]
    }

    model user {
        id    Int    @id @default(autoincrement())
        email String @unique
    }
}

fn main() {}
