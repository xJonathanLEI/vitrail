pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::{QueryResult, schema};
extern crate self as vitrail_sqlite;

schema! {
    name query_schema

    model user {
        id Int @id @default(autoincrement())
        email String
    }
}

#[derive(QueryResult)]
#[vitrail(
    schema = crate::query_schema::Schema,
    model = user,
    where(email = not(excluded_email))
)]
struct UserByExcludedEmail {
    id: i64,
    email: String,
}

fn main() {
    let _ = crate::query_schema::query::<UserByExcludedEmail>();
}
