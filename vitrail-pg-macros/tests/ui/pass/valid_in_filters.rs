pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{
    DeleteMany, QueryResult, QueryVariables, UpdateData, UpdateMany, delete, query, schema,
    update,
};
extern crate self as vitrail_pg;

schema! {
    name in_filter_schema

    model user {
        id       Int      @id @default(autoincrement())
        email    String   @unique
        posts    post[]
        comments comment[]
    }

    model post {
        id        Int       @id @default(autoincrement())
        title     String
        published Boolean
        author_id Int
        author    user      @relation(fields: [author_id], references: [id])
        comments  comment[]
    }

    model comment {
        id        Int    @id @default(autoincrement())
        body      String
        post_id   Int
        author_id Int
        post      post   @relation(fields: [post_id], references: [id])
        author    user   @relation(fields: [author_id], references: [id])
    }
}

#[derive(QueryVariables)]
struct PostIdsVariables {
    post_ids: Vec<i64>,
}

#[derive(QueryVariables)]
struct CommentIdsVariables {
    comment_ids: Vec<i64>,
}

#[derive(QueryResult)]
#[vitrail(
    schema = crate::in_filter_schema::Schema,
    model = post,
    variables = PostIdsVariables,
    where(id = in(post_ids))
)]
struct PostByIds {
    id: i64,
    title: String,
}

#[derive(UpdateData)]
#[vitrail(schema = crate::in_filter_schema::Schema, model = post)]
struct PublishPostsData {
    published: bool,
}

#[derive(UpdateMany)]
#[vitrail(
    schema = crate::in_filter_schema::Schema,
    model = post,
    data = PublishPostsData,
    variables = PostIdsVariables,
    where(id = in(post_ids))
)]
struct PublishPostsByIds;

#[derive(DeleteMany)]
#[vitrail(
    schema = crate::in_filter_schema::Schema,
    model = comment,
    variables = CommentIdsVariables,
    where(id = in(comment_ids))
)]
struct DeleteCommentsByIds;

fn main() {
    let _ = crate::in_filter_schema::query_with_variables::<PostByIds>(PostIdsVariables {
        post_ids: vec![1, 2, 3],
    });

    let _ = crate::in_filter_schema::update_many_with_variables::<PublishPostsByIds>(
        PostIdsVariables {
            post_ids: vec![1, 2, 3],
        },
        PublishPostsData { published: true },
    );

    let _ = crate::in_filter_schema::delete_many_with_variables::<DeleteCommentsByIds>(
        CommentIdsVariables {
            comment_ids: vec![4, 5, 6],
        },
    );

    let _ = query! {
        crate::in_filter_schema,
        post {
            select: {
                id: true,
                title: true,
            },
            where: {
                id: {
                    in: vec![1_i64, 2_i64, 3_i64]
                },
            },
        }
    };

    let _ = update! {
        crate::in_filter_schema,
        post {
            data: {
                published: true,
            },
            where: {
                id: {
                    in: vec![1_i64, 2_i64, 3_i64]
                },
            },
        }
    };

    let _ = delete! {
        crate::in_filter_schema,
        comment {
            where: {
                id: {
                    in: vec![4_i64, 5_i64, 6_i64]
                },
            },
        }
    };
}
