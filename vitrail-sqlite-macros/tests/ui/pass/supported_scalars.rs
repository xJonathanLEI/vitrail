pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::schema;
extern crate self as vitrail_sqlite;

schema! {
    name supported_scalars_schema

    model record {
        id           Int      @id @default(autoincrement())
        large_number BigInt
        name         String
        enabled      Boolean
        created_at   DateTime @default(now())
        score        Float
        payload      Bytes
        metadata     Json
    }
}

fn main() {}
