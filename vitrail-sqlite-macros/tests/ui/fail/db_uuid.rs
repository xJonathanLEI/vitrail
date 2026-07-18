pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::schema;
extern crate self as vitrail_sqlite;

schema! {
    name db_uuid_schema

    model organization {
        id          Int    @id @default(autoincrement())
        external_id String @unique @db.Uuid
    }
}

fn main() {}
