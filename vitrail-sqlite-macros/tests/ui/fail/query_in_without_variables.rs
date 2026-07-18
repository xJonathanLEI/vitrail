pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::{QueryResult, schema};
extern crate self as vitrail_sqlite;

schema! {
    name query_schema

    model post {
        id    Int    @id @default(autoincrement())
        title String
    }
}

#[derive(QueryResult)]
#[vitrail(
    schema = crate::query_schema::Schema,
    model = post,
    where(id = in(post_ids))
)]
struct PostByIds {
    id: i64,
    title: String,
}

fn main() {
    let _ = crate::query_schema::query::<PostByIds>();
}
