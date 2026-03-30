pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{QueryResult, query, schema};
extern crate self as vitrail_pg;

schema! {
    name query_helper_wrong_filter_type_schema

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

fn main() {
    let _ = query! {
        crate::query_helper_wrong_filter_type_schema,
        post {
            select: {
                id: true,
                title: true,
            },
            where: {
                id: {
                    in: vec!["asdf".to_owned()]
                },
            },
        }
    };
}
