pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::schema;
extern crate self as vitrail_pg;


schema! {
    name rust_ty_relation_field_schema

    model user {
        id      Int     @id @default(autoincrement())
        address address @rust_ty(Address)
    }

    model address {
        id      Int @id @default(autoincrement())
        user_id Int
        user    user @relation(fields: [user_id], references: [id])
    }
}

fn main() {}
