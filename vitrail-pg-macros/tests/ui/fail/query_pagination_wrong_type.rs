pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{QueryResult, QueryVariables, schema};
extern crate self as vitrail_pg;

schema! {
    name query_schema

    model post {
        id    Int    @id @default(autoincrement())
        title String
    }
}

#[derive(QueryVariables)]
struct PaginationVariables {
    skip: String,
    limit: i64,
}

#[derive(QueryResult)]
#[vitrail(
    schema = crate::query_schema::Schema,
    model = post,
    variables = PaginationVariables,
    skip = skip,
    limit = limit
)]
struct PaginatedPost {
    id: i64,
    title: String,
}

fn main() {
    let _ = crate::query_schema::query_with_variables::<PaginatedPost>(PaginationVariables {
        skip: "1".to_owned(),
        limit: 10,
    });
}
