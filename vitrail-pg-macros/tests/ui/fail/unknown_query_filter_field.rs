pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{QueryResult, QueryVariables, schema};
extern crate self as vitrail_pg;

schema! {
    name query_schema

    model user {
        id Int @id @default(autoincrement())
        email String
    }
}

#[derive(QueryVariables)]
struct UserByIdVariables {
    user_id: i64,
}

#[derive(QueryResult)]
#[vitrail(
    schema = crate::query_schema::Schema,
    model = user,
    variables = UserByIdVariables,
    where(bid = eq(user_id))
)]
struct UserById {
    id: i64,
    email: String,
}

fn main() {}
