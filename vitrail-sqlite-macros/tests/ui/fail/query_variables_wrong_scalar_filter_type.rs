pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::{QueryResult, QueryVariables, schema};
extern crate self as vitrail_sqlite;

schema! {
    name query_variables_wrong_scalar_filter_type_schema

    model post {
        id        Int     @id @default(autoincrement())
        title     String
        published Boolean
    }
}

#[derive(QueryVariables)]
struct PostByIdVariables {
    post_id: String,
}

#[derive(QueryResult)]
#[vitrail(
    schema = crate::query_variables_wrong_scalar_filter_type_schema::Schema,
    model = post,
    variables = PostByIdVariables,
    where(id = eq(post_id))
)]
struct PostById {
    id: i64,
    title: String,
}

fn main() {}
