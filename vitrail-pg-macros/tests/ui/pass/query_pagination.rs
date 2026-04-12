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
struct PaginationVariables {
    skip: i64,
    limit: i64,
}

#[derive(QueryResult)]
#[vitrail(
    schema = crate::query_schema::Schema,
    model = post,
    variables = PaginationVariables,
    order_by(title = desc),
    skip = skip,
    limit = limit
)]
struct PaginatedPost {
    id: i64,
    title: String,
}

#[derive(QueryResult)]
#[vitrail(
    schema = crate::query_schema::Schema,
    model = post,
    order_by(title = desc),
    skip = 1,
    limit = 2
)]
struct StaticallyPaginatedPost {
    id: i64,
    title: String,
}

#[derive(QueryResult)]
#[vitrail(schema = crate::query_schema::Schema, model = user)]
struct UserWithPaginatedPosts {
    id: i64,
    email: String,
    #[vitrail(include)]
    posts: Vec<StaticallyPaginatedPost>,
}

fn main() {
    let _ = crate::query_schema::query_with_variables::<PaginatedPost>(PaginationVariables {
        skip: 1,
        limit: 10,
    });

    let _ = crate::query_schema::query::<UserWithPaginatedPosts>();

    let _ = query! {
        crate::query_schema,
        post {
            select: {
                id: true,
                title: true,
            },
            order_by: [
                { title: desc },
            ],
            skip: 1_i64,
            limit: 10_i64,
        }
    };

    let skip = 2_i64;
    let limit = 5_i64;

    let _ = query! {
        crate::query_schema,
        user {
            select: {
                id: true,
                email: true,
            },
            include: {
                posts: {
                    select: {
                        id: true,
                        title: true,
                    },
                    order_by: [
                        { title: desc },
                    ],
                    skip: skip,
                    limit: limit,
                },
            },
        }
    };
}
