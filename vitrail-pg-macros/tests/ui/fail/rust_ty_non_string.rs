pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::schema;
extern crate self as vitrail_pg;


schema! {
    name rust_ty_non_string_schema

    model user {
        id      Int @id @default(autoincrement())
        user_id Int @rust_ty(UserId)
    }
}

fn main() {}
