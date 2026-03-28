pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{QueryVariables, UpdateData, UpdateMany, schema};
extern crate self as vitrail_pg;

schema! {
    name update_schema

    model user {
        id    Int    @id @default(autoincrement())
        email String
        age   Int
        posts post[]
    }

    model post {
        id        Int    @id @default(autoincrement())
        title     String
        author_id Int
        author    user   @relation(fields: [author_id], references: [id])
    }
}

#[derive(UpdateData)]
#[vitrail(schema = crate::update_schema::Schema, model = post)]
struct UpdatePostData {
    title: String,
}

#[derive(QueryVariables)]
struct UpdatePostVariables {
    author_email: String,
}

#[derive(UpdateMany)]
#[vitrail(
    schema = crate::update_schema::Schema,
    model = post,
    data = UpdatePostData,
    variables = UpdatePostVariables,
    where(author.email.domain = eq(author_email))
)]
struct UpdatePostsByInvalidWherePath;

fn main() {}
