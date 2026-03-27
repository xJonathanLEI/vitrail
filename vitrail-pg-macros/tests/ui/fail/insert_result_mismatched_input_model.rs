use vitrail_pg::{InsertInput, InsertResult, schema};

schema! {
    name insert_schema

    model user {
        id    Int    @id @default(autoincrement())
        email String
    }

    model post {
        id    Int    @id @default(autoincrement())
        title String
    }
}

#[derive(InsertInput)]
#[vitrail(schema = crate::insert_schema::Schema, model = post)]
struct NewPost {
    title: String,
}

#[derive(InsertResult)]
#[vitrail(schema = crate::insert_schema::Schema, model = user, input = NewPost)]
struct User {
    id: i64,
    email: String,
}

fn main() {}
