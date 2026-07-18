pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::schema;
extern crate self as vitrail_sqlite;


schema! {
    name rust_ty_non_string_schema

    model user {
        id      Int @id @default(autoincrement())
        user_id Int @rust_ty(UserId)
    }
}

fn main() {}
