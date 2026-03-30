pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::schema;
extern crate self as vitrail_pg;

schema! {
    name db_uuid_with_rust_ty_schema

    model user {
        id  Int    @id @default(autoincrement())
        uid String @db.Uuid @rust_ty(UserId)
    }
}

fn main() {}
