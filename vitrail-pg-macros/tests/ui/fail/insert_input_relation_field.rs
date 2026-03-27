use vitrail_pg::{InsertInput, schema};

schema! {
    name insert_schema

    model user {
        id    Int    @id @default(autoincrement())
        email String
        posts post[]
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
    posts: i64,
}

fn main() {}
