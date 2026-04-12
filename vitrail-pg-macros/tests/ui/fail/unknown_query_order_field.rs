pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{QueryResult, schema};
extern crate self as vitrail_pg;

schema! {
    name query_order_schema

    model post {
        id    Int    @id @default(autoincrement())
        title String
    }
}

#[derive(QueryResult)]
#[vitrail(schema = crate::query_order_schema::Schema, model = post, order_by(slug = asc))]
struct InvalidPostOrder {
    id: i64,
    title: String,
}

fn main() {}
