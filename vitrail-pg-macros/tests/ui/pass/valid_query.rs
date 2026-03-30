pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{QueryResult, QueryVariables, query, schema};
extern crate self as vitrail_pg;

schema! {
    name query_schema

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
struct ExcludedTitleVariables {
    excluded_title: String,
}

#[derive(QueryResult)]
#[vitrail(
    schema = crate::query_schema::Schema,
    model = post,
    variables = ExcludedTitleVariables,
    where(title = not(excluded_title))
)]
struct PostWithDifferentTitle {
    id: i64,
    title: String,
}

fn main() {
    let _ = crate::query_schema::query_with_variables::<PostWithDifferentTitle>(
        ExcludedTitleVariables {
            excluded_title: "Draft".to_owned(),
        },
    );

    let _ = query! {
        crate::query_schema,
        post {
            select: {
                id: true,
                title: true,
            },
            where: {
                title: {
                    not: "Draft".to_owned()
                },
            },
        }
    };
}
