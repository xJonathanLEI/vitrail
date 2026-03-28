pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{InsertInput, InsertResult, insert, schema};
extern crate self as vitrail_pg;

schema! {
    name insert_schema

    model user {
        id         Int      @id @default(autoincrement())
        email      String   @unique
        name       String
        created_at DateTime @default(now())
        posts      post[]
    }

    model post {
        id        Int    @id @default(autoincrement())
        title     String
        author_id Int
        author    user   @relation(fields: [author_id], references: [id])
    }
}

#[derive(InsertInput)]
#[vitrail(schema = crate::insert_schema::Schema, model = user)]
struct NewUser {
    email: String,
    name: String,
}

#[derive(InsertResult)]
#[vitrail(schema = crate::insert_schema::Schema, model = user, input = NewUser)]
struct User {
    id: i64,
    email: String,
    name: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

fn main() {
    let _ = crate::insert_schema::insert::<User>(NewUser {
        email: "alice@example.com".to_owned(),
        name: "Alice".to_owned(),
    });

    let _ = insert! {
        crate::insert_schema,
        user {
            data: {
                email: "alice@example.com".to_owned(),
                name: "Alice".to_owned(),
            },
        }
    };
}
