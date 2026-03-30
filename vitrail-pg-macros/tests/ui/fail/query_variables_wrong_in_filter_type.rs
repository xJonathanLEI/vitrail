pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{QueryResult, QueryVariables, schema};
extern crate self as vitrail_pg;

schema! {
    name query_variables_wrong_in_filter_type_schema

    model user {
        id    Int    @id @default(autoincrement())
        email String @unique
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
struct PostByIdsVariables {
    post_ids: Vec<String>,
}

#[derive(QueryResult)]
#[vitrail(
    schema = crate::query_variables_wrong_in_filter_type_schema::Schema,
    model = post,
    variables = PostByIdsVariables,
    where(id = in(post_ids))
)]
struct PostByIds {
    id: i64,
    title: String,
}

fn main() {}
