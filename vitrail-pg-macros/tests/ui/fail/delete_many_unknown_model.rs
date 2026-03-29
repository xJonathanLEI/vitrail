pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{DeleteMany, schema};
extern crate self as vitrail_pg;

schema! {
    name delete_schema

    model user {
        id    Int    @id @default(autoincrement())
        email String @unique
    }
}

#[derive(DeleteMany)]
#[vitrail(schema = crate::delete_schema::Schema, model = missing_model)]
struct DeleteMissingModel;

fn main() {}
