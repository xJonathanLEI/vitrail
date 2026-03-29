pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{DeleteMany, QueryVariables, schema};
extern crate self as vitrail_pg;

schema! {
    name delete_schema

    model user {
        id    Int    @id @default(autoincrement())
        email String @unique
        age   Int
        posts post[]
    }

    model post {
        id        Int     @id @default(autoincrement())
        title     String
        published Boolean
        author_id Int
        author    user    @relation(fields: [author_id], references: [id])
    }
}

#[derive(QueryVariables)]
struct AuthorAgeVariables {
    author_age: i64,
}

#[derive(DeleteMany)]
#[vitrail(
    schema = crate::delete_schema::Schema,
    model = post,
    variables = AuthorAgeVariables,
    where(author.age = eq(author_age))
)]
struct DeletePostsByAuthorAge;

fn main() {
    let _ = crate::delete_schema::delete_many_with_variables::<DeletePostsByAuthorAge>(
        AuthorAgeVariables { author_age: 30 },
    );
}
