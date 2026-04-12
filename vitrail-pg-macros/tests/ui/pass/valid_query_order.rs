pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{QueryResult, query, schema};
extern crate self as vitrail_pg;

schema! {
    name query_order_schema

    model user {
        id    Int    @id @default(autoincrement())
        email String @unique
        posts post[]
    }

    model post {
        id        Int    @id @default(autoincrement())
        title     String
        author_id Int
        author    user   @relation(fields: [author_id], references: [id])
    }
}

#[derive(QueryResult)]
#[vitrail(schema = crate::query_order_schema::Schema, model = post, order_by(author.email = asc, title = desc))]
struct PostOrderedByAuthorEmail {
    id: i64,
    title: String,
}

fn main() {
    let _ = crate::query_order_schema::query::<PostOrderedByAuthorEmail>();

    let _ = query! {
        crate::query_order_schema,
        post {
            select: {
                id: true,
                title: true,
            },
            order_by: [
                { author: { email: asc } },
                { title: desc },
            ],
        }
    };
}
