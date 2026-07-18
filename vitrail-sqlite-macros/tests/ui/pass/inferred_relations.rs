pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::schema;
extern crate self as vitrail_sqlite;

schema! {
    name inferred_relations_schema

    model user {
        id     Int    @id @default(autoincrement())
        post   Post?
        status String
    }

    model post {
        id         Int      @id @default(autoincrement())
        user_id    Int
        created_at DateTime @default(now())
        user       user     @relation(fields: [user_id], references: [id])
    }
}

fn main() {}
