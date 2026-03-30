pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::schema;
extern crate self as vitrail_pg;

schema! {
    name db_uuid_non_string_schema

    model user {
        id  Int @id @default(autoincrement())
        uid Int @db.Uuid
    }
}

fn main() {}
