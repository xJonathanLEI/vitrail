pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{QueryResult, QueryVariables, schema};
extern crate self as vitrail_pg;

schema! {
    name query_schema

    model user {
        id Int @id @default(autoincrement())
        email String
        posts post[]
    }

    model post {
        id Int @id @default(autoincrement())
        title String
        author_id Int
        author user @relation(fields: [author_id], references: [id])
    }
}

#[derive(QueryVariables)]
struct PostByIdVariables {
    post_id: i64,
}

#[derive(QueryResult)]
#[vitrail(
    schema = crate::query_schema::Schema,
    model = post,
    variables = PostByIdVariables,
    where(id = eq(post_id))
)]
struct FilteredPost {
    id: i64,
    title: String,
}

#[derive(QueryResult)]
#[vitrail(schema = crate::query_schema::Schema, model = user)]
struct UserWithFilteredPosts {
    id: i64,
    email: String,
    #[vitrail(include)]
    posts: Vec<FilteredPost>,
}

fn main() {
    let _ = crate::query_schema::query::<UserWithFilteredPosts>();
}
