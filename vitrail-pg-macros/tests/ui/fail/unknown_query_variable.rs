use vitrail_pg::{schema, QueryResult, QueryVariables};

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
    where(id = eq(user_ids))
)]
struct UserById {
    id: i64,
    email: String,
}

fn main() {}
