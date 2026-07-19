use vitrail_sqlite::{
    InsertInput, InsertResult, QueryResult, QueryVariables, insert, query, schema,
};

schema! {
    name statements_schema

    model user {
        id         Int      @id @default(autoincrement())
        email      String   @unique
        name       String
        created_at DateTime @default(now())
        posts      post[]
    }

    model post {
        id         Int      @id @default(autoincrement())
        title      String
        body       String?
        published  Boolean
        author_id  Int
        created_at DateTime @default(now())
        author     user     @relation(fields: [author_id], references: [id])
        comments   comment[]
    }

    model comment {
        id      Int    @id @default(autoincrement())
        body    String
        post_id Int
        post    post   @relation(fields: [post_id], references: [id])
    }
}

pub(crate) use self::statements_schema as sqlite_statements_schema;

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(schema = crate::statements_schema::Schema, model = user)]
struct UserSummary {
    id: i64,
    email: String,
    name: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(schema = crate::statements_schema::Schema, model = comment)]
struct CommentSummary {
    id: i64,
    body: String,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(schema = crate::statements_schema::Schema, model = post)]
struct PostWithAuthor {
    id: i64,
    title: String,
    #[vitrail(include)]
    author: UserSummary,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(schema = crate::statements_schema::Schema, model = post)]
struct PostWithComments {
    id: i64,
    title: String,
    #[vitrail(include)]
    comments: Vec<CommentSummary>,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(schema = crate::statements_schema::Schema, model = user)]
struct UserWithPostsAndComments {
    id: i64,
    email: String,
    #[vitrail(include)]
    posts: Vec<PostWithComments>,
}

#[derive(QueryVariables)]
struct UserByIdVariables {
    user_id: i64,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(
    schema = crate::statements_schema::Schema,
    model = user,
    variables = UserByIdVariables,
    where(id = eq(user_id))
)]
struct UserById {
    id: i64,
    email: String,
}

#[derive(QueryVariables)]
struct UserWithFilteredPostsVariables {
    user_id: i64,
    post_id: i64,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(
    schema = crate::statements_schema::Schema,
    model = post,
    variables = UserWithFilteredPostsVariables,
    where(id = eq(post_id))
)]
struct FilteredPost {
    id: i64,
    title: String,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(
    schema = crate::statements_schema::Schema,
    model = user,
    variables = UserWithFilteredPostsVariables,
    where(id = eq(user_id))
)]
struct UserWithFilteredPosts {
    id: i64,
    email: String,
    #[vitrail(include)]
    posts: Vec<FilteredPost>,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(schema = crate::statements_schema::Schema, model = post, where(body = null))]
struct PostWithNullBody {
    id: i64,
    title: String,
    body: Option<String>,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(
    schema = crate::statements_schema::Schema,
    model = post,
    where(body = not(null))
)]
struct PostWithNonNullBody {
    id: i64,
    title: String,
    body: Option<String>,
}

#[derive(QueryVariables)]
struct PostByExcludedTitleVariables {
    excluded_title: String,
}

#[derive(QueryVariables)]
struct PostsByIdsVariables {
    post_ids: Vec<i64>,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(
    schema = crate::statements_schema::Schema,
    model = post,
    variables = PostByExcludedTitleVariables,
    where(title = not(excluded_title))
)]
struct PostWithDifferentTitle {
    id: i64,
    title: String,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(
    schema = crate::statements_schema::Schema,
    model = post,
    variables = PostsByIdsVariables,
    where(id = in(post_ids))
)]
struct PostByIds {
    id: i64,
    title: String,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(schema = crate::statements_schema::Schema, model = post, order_by(title = desc))]
struct PostOrderedByTitleDesc {
    id: i64,
    title: String,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(schema = crate::statements_schema::Schema, model = comment, order_by(body = desc))]
struct CommentOrderedByBodyDesc {
    id: i64,
    body: String,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(schema = crate::statements_schema::Schema, model = user)]
struct UserWithPostsOrderedByTitleDesc {
    id: i64,
    email: String,
    #[vitrail(include)]
    posts: Vec<PostOrderedByTitleDesc>,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(schema = crate::statements_schema::Schema, model = post, order_by(title = desc))]
struct PostWithCommentsOrderedDesc {
    id: i64,
    title: String,
    #[vitrail(include)]
    comments: Vec<CommentOrderedByBodyDesc>,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(schema = crate::statements_schema::Schema, model = user)]
struct UserWithPostsAndCommentsOrderedDesc {
    id: i64,
    email: String,
    #[vitrail(include)]
    posts: Vec<PostWithCommentsOrderedDesc>,
}

#[derive(QueryVariables)]
struct PaginationVariables {
    skip: i64,
    limit: i64,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(
    schema = crate::statements_schema::Schema,
    model = post,
    order_by(title = desc),
    skip = 1,
    limit = 1
)]
struct PostPageStatic {
    id: i64,
    title: String,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(
    schema = crate::statements_schema::Schema,
    model = post,
    variables = PaginationVariables,
    order_by(title = desc),
    skip = skip,
    limit = limit
)]
struct PostPageWithVariables {
    id: i64,
    title: String,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(schema = crate::statements_schema::Schema, model = user)]
struct UserWithPaginatedPosts {
    id: i64,
    email: String,
    #[vitrail(include)]
    posts: Vec<PostPageStatic>,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(
    schema = crate::statements_schema::Schema,
    model = user,
    variables = PaginationVariables
)]
struct UserWithPaginatedPostsVariables {
    id: i64,
    email: String,
    #[vitrail(include)]
    posts: Vec<PostPageWithVariables>,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(
    schema = crate::statements_schema::Schema,
    model = post,
    skip = 1,
    limit = 1
)]
struct PostPageStaticWithoutOrder {
    id: i64,
    title: String,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(schema = crate::statements_schema::Schema, model = user)]
struct UserWithPaginatedPostsWithoutOrder {
    id: i64,
    email: String,
    #[vitrail(include)]
    posts: Vec<PostPageStaticWithoutOrder>,
}

#[test]
fn ad_hoc_skip_and_limit_generates_expected_sql() {
    let sql = query! {
        crate::statements_schema,
        post {
            select: {
                id: true,
                title: true,
            },
            order_by: [
                { title: desc },
            ],
            skip: 1_i64,
            limit: 1_i64,
        }
    }
    .to_sql()
    .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "post__id","#,
            r#""t0"."title" AS "post__title""#,
            r#"FROM "post" AS "t0""#,
            r#"ORDER BY "t0"."title" DESC"#,
            r#"LIMIT ?1 OFFSET ?2"#,
        ]
        .join(" ")
    );
}

#[test]
fn nested_ad_hoc_skip_and_limit_generates_expected_sql() {
    let sql = query! {
        crate::statements_schema,
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
                    skip: 1_i64,
                    limit: 1_i64,
                },
            },
        }
    }
    .to_sql()
    .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "user__id","#,
            r#""t0"."email" AS "user__email","#,
            r#"(SELECT COALESCE(json_group_array(json("__vitrail_nested_rows"."data")), json('[]')) AS "data" FROM (SELECT json_array("t1"."id", "t1"."title") AS "data" FROM "post" AS "t1" WHERE "t1"."author_id" = "t0"."id" ORDER BY "t1"."title" DESC LIMIT ?1 OFFSET ?2) AS "__vitrail_nested_rows") AS "user__posts""#,
            r#"FROM "user" AS "t0""#,
        ]
        .join(" ")
    );
}

#[test]
fn model_first_skip_and_limit_generates_expected_sql() {
    let sql = crate::statements_schema::query::<PostPageStatic>()
        .to_sql()
        .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "post__id","#,
            r#""t0"."title" AS "post__title""#,
            r#"FROM "post" AS "t0""#,
            r#"ORDER BY "t0"."title" DESC"#,
            r#"LIMIT ?1 OFFSET ?2"#,
        ]
        .join(" ")
    );
}

#[test]
fn model_first_skip_and_limit_with_variables_generates_expected_sql() {
    let sql = crate::statements_schema::query_with_variables::<PostPageWithVariables>(
        PaginationVariables { skip: 1, limit: 1 },
    )
    .to_sql()
    .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "post__id","#,
            r#""t0"."title" AS "post__title""#,
            r#"FROM "post" AS "t0""#,
            r#"ORDER BY "t0"."title" DESC"#,
            r#"LIMIT ?1 OFFSET ?2"#,
        ]
        .join(" ")
    );
}

#[test]
fn nested_model_first_skip_and_limit_generates_expected_sql() {
    let sql = crate::statements_schema::query::<UserWithPaginatedPosts>()
        .to_sql()
        .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "user__id","#,
            r#""t0"."email" AS "user__email","#,
            r#"(SELECT COALESCE(json_group_array(json("__vitrail_nested_rows"."data")), json('[]')) AS "data" FROM (SELECT json_array("t1"."id", "t1"."title") AS "data" FROM "post" AS "t1" WHERE "t1"."author_id" = "t0"."id" ORDER BY "t1"."title" DESC LIMIT ?1 OFFSET ?2) AS "__vitrail_nested_rows") AS "user__posts""#,
            r#"FROM "user" AS "t0""#,
        ]
        .join(" ")
    );
}

#[test]
fn nested_model_first_skip_and_limit_with_variables_generates_expected_sql() {
    let sql = crate::statements_schema::query_with_variables::<UserWithPaginatedPostsVariables>(
        PaginationVariables { skip: 1, limit: 1 },
    )
    .to_sql()
    .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "user__id","#,
            r#""t0"."email" AS "user__email","#,
            r#"(SELECT COALESCE(json_group_array(json("__vitrail_nested_rows"."data")), json('[]')) AS "data" FROM (SELECT json_array("t1"."id", "t1"."title") AS "data" FROM "post" AS "t1" WHERE "t1"."author_id" = "t0"."id" ORDER BY "t1"."title" DESC LIMIT ?1 OFFSET ?2) AS "__vitrail_nested_rows") AS "user__posts""#,
            r#"FROM "user" AS "t0""#,
        ]
        .join(" ")
    );
}

#[test]
fn nested_model_first_skip_and_limit_without_explicit_order_generates_expected_sql() {
    let sql = crate::statements_schema::query::<UserWithPaginatedPostsWithoutOrder>()
        .to_sql()
        .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "user__id","#,
            r#""t0"."email" AS "user__email","#,
            r#"(SELECT COALESCE(json_group_array(json("__vitrail_nested_rows"."data")), json('[]')) AS "data" FROM (SELECT json_array("t1"."id", "t1"."title") AS "data" FROM "post" AS "t1" WHERE "t1"."author_id" = "t0"."id" ORDER BY "t1"."id" LIMIT ?1 OFFSET ?2) AS "__vitrail_nested_rows") AS "user__posts""#,
            r#"FROM "user" AS "t0""#,
        ]
        .join(" ")
    );
}

#[allow(dead_code)]
#[derive(InsertInput)]
#[vitrail(schema = crate::statements_schema::Schema, model = user)]
struct NewUser {
    email: String,
    name: String,
}

#[allow(dead_code)]
#[derive(InsertResult)]
#[vitrail(schema = crate::statements_schema::Schema, model = user, input = NewUser)]
struct InsertedUser {
    id: i64,
    email: String,
    name: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[allow(dead_code)]
#[derive(InsertInput)]
#[vitrail(schema = crate::statements_schema::Schema, model = post)]
struct NewPost {
    title: String,
    body: Option<String>,
    published: bool,
    author_id: i64,
}

#[allow(dead_code)]
#[derive(InsertResult)]
#[vitrail(schema = crate::statements_schema::Schema, model = post, input = NewPost)]
struct InsertedPostSummary {
    id: i64,
    title: String,
    body: Option<String>,
}

#[test]
fn scalar_only_query_generates_expected_sql() {
    let sql = query! {
        crate::statements_schema,
        user {
            select: {
                id: true,
                email: true,
                name: true,
                created_at: true,
            },
        }
    }
    .to_sql()
    .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "user__id","#,
            r#""t0"."email" AS "user__email","#,
            r#""t0"."name" AS "user__name","#,
            r#""t0"."created_at" AS "user__created_at""#,
            r#"FROM "user" AS "t0""#,
        ]
        .join(" ")
    );
}

#[test]
fn nested_query_generates_expected_sql() {
    let sql = query! {
        crate::statements_schema,
        user {
            select: {
                id: true,
                email: true,
            },
            include: {
                posts: true,
            },
        }
    }
    .to_sql()
    .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "user__id","#,
            r#""t0"."email" AS "user__email","#,
            r#"(SELECT COALESCE(json_group_array(json("__vitrail_nested_rows"."data")), json('[]')) AS "data" FROM (SELECT json_array("t1"."id", "t1"."title", "t1"."body", json(CASE WHEN "t1"."published" IS NULL THEN NULL WHEN "t1"."published" THEN 'true' ELSE 'false' END), "t1"."author_id", "t1"."created_at") AS "data" FROM "post" AS "t1" WHERE "t1"."author_id" = "t0"."id" ORDER BY "t1"."id") AS "__vitrail_nested_rows") AS "user__posts""#,
            r#"FROM "user" AS "t0""#,
        ]
        .join(" ")
    );
}

#[test]
fn explicit_nested_query_selection_generates_expected_sql() {
    let sql = query! {
        crate::statements_schema,
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
                },
            },
        }
    }
    .to_sql()
    .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "user__id","#,
            r#""t0"."email" AS "user__email","#,
            r#"(SELECT COALESCE(json_group_array(json("__vitrail_nested_rows"."data")), json('[]')) AS "data" FROM (SELECT json_array("t1"."id", "t1"."title") AS "data" FROM "post" AS "t1" WHERE "t1"."author_id" = "t0"."id" ORDER BY "t1"."id") AS "__vitrail_nested_rows") AS "user__posts""#,
            r#"FROM "user" AS "t0""#,
        ]
        .join(" ")
    );
}

#[test]
fn to_one_include_generates_expected_sql() {
    let sql = crate::statements_schema::query::<PostWithAuthor>()
        .to_sql()
        .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "post__id","#,
            r#""t0"."title" AS "post__title","#,
            r#"(SELECT json_array("t1"."id", "t1"."email", "t1"."name", "t1"."created_at") AS "data" FROM "user" AS "t1" WHERE "t1"."id" = "t0"."author_id" LIMIT 1) AS "post__author""#,
            r#"FROM "post" AS "t0""#,
        ]
        .join(" ")
    );
}

#[test]
fn nested_ad_hoc_order_by_generates_expected_sql() {
    let sql = query! {
        crate::statements_schema,
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
                    include: {
                        comments: {
                            select: {
                                id: true,
                                body: true,
                            },
                            order_by: [
                                { body: desc },
                            ],
                        },
                    },
                    order_by: [
                        { title: desc },
                    ],
                },
            },
        }
    }
    .to_sql()
    .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "user__id","#,
            r#""t0"."email" AS "user__email","#,
            r#"(SELECT COALESCE(json_group_array(json("__vitrail_nested_rows"."data")), json('[]')) AS "data" FROM (SELECT json_array("t1"."id", "t1"."title", json((SELECT COALESCE(json_group_array(json("__vitrail_nested_rows"."data")), json('[]')) AS "data" FROM (SELECT json_array("t2"."id", "t2"."body") AS "data" FROM "comment" AS "t2" WHERE "t2"."post_id" = "t1"."id" ORDER BY "t2"."body" DESC) AS "__vitrail_nested_rows"))) AS "data" FROM "post" AS "t1" WHERE "t1"."author_id" = "t0"."id" ORDER BY "t1"."title" DESC) AS "__vitrail_nested_rows") AS "user__posts""#,
            r#"FROM "user" AS "t0""#,
        ]
        .join(" ")
    );
}

#[test]
fn nested_model_first_order_by_generates_expected_sql() {
    let sql = crate::statements_schema::query::<UserWithPostsAndCommentsOrderedDesc>()
        .to_sql()
        .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "user__id","#,
            r#""t0"."email" AS "user__email","#,
            r#"(SELECT COALESCE(json_group_array(json("__vitrail_nested_rows"."data")), json('[]')) AS "data" FROM (SELECT json_array("t1"."id", "t1"."title", json((SELECT COALESCE(json_group_array(json("__vitrail_nested_rows"."data")), json('[]')) AS "data" FROM (SELECT json_array("t2"."id", "t2"."body") AS "data" FROM "comment" AS "t2" WHERE "t2"."post_id" = "t1"."id" ORDER BY "t2"."body" DESC) AS "__vitrail_nested_rows"))) AS "data" FROM "post" AS "t1" WHERE "t1"."author_id" = "t0"."id" ORDER BY "t1"."title" DESC) AS "__vitrail_nested_rows") AS "user__posts""#,
            r#"FROM "user" AS "t0""#,
        ]
        .join(" ")
    );
}

#[test]
fn ad_hoc_where_generates_expected_sql() {
    let sql = query! {
        crate::statements_schema,
        user {
            select: {
                id: true,
                email: true,
            },
            where: {
                id: {
                    eq: 7_i64
                },
            },
        }
    }
    .to_sql()
    .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "user__id","#,
            r#""t0"."email" AS "user__email""#,
            r#"FROM "user" AS "t0""#,
            r#"WHERE "t0"."id" = ?1"#,
        ]
        .join(" ")
    );
}

#[test]
fn ad_hoc_null_where_generates_expected_sql() {
    let sql = query! {
        crate::statements_schema,
        post {
            select: {
                id: true,
                title: true,
                body: true,
            },
            where: {
                body: null,
            },
        }
    }
    .to_sql()
    .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "post__id","#,
            r#""t0"."title" AS "post__title","#,
            r#""t0"."body" AS "post__body""#,
            r#"FROM "post" AS "t0""#,
            r#"WHERE "t0"."body" IS NULL"#,
        ]
        .join(" ")
    );
}

#[test]
fn ad_hoc_not_null_where_generates_expected_sql() {
    let sql = query! {
        crate::statements_schema,
        post {
            select: {
                id: true,
                title: true,
                body: true,
            },
            where: {
                body: {
                    not: null
                },
            },
        }
    }
    .to_sql()
    .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "post__id","#,
            r#""t0"."title" AS "post__title","#,
            r#""t0"."body" AS "post__body""#,
            r#"FROM "post" AS "t0""#,
            r#"WHERE "t0"."body" IS NOT NULL"#,
        ]
        .join(" ")
    );
}

#[test]
fn ad_hoc_not_equal_where_generates_expected_sql() {
    let sql = query! {
        crate::statements_schema,
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
    }
    .to_sql()
    .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "post__id","#,
            r#""t0"."title" AS "post__title""#,
            r#"FROM "post" AS "t0""#,
            r#"WHERE "t0"."title" <> ?1"#,
        ]
        .join(" ")
    );
}

#[test]
fn ad_hoc_in_where_generates_expected_sql() {
    let sql = query! {
        crate::statements_schema,
        post {
            select: {
                id: true,
                title: true,
            },
            where: {
                id: {
                    in: vec![7_i64, 11_i64]
                },
            },
        }
    }
    .to_sql()
    .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "post__id","#,
            r#""t0"."title" AS "post__title""#,
            r#"FROM "post" AS "t0""#,
            r#"WHERE "t0"."id" IN (?1, ?2)"#,
        ]
        .join(" ")
    );
}

#[test]
fn ad_hoc_order_by_generates_expected_sql() {
    let sql = query! {
        crate::statements_schema,
        post {
            select: {
                id: true,
                title: true,
            },
            order_by: [
                { title: desc },
            ],
        }
    }
    .to_sql()
    .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "post__id","#,
            r#""t0"."title" AS "post__title""#,
            r#"FROM "post" AS "t0""#,
            r#"ORDER BY "t0"."title" DESC"#,
        ]
        .join(" ")
    );
}

#[test]
fn ad_hoc_relation_order_by_generates_expected_sql() {
    let sql = query! {
        crate::statements_schema,
        post {
            select: {
                id: true,
                title: true,
            },
            order_by: [
                { author: { email: asc } },
                { created_at: desc },
            ],
        }
    }
    .to_sql()
    .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "post__id","#,
            r#""t0"."title" AS "post__title""#,
            r#"FROM "post" AS "t0""#,
            r#"LEFT JOIN "user" AS "t1" ON "t1"."id" = "t0"."author_id""#,
            r#"ORDER BY "t1"."email" ASC, julianday("t0"."created_at") DESC"#,
        ]
        .join(" ")
    );
}

#[test]
fn repeated_relation_order_by_reuses_single_join() {
    let sql = query! {
        crate::statements_schema,
        post {
            select: {
                id: true,
                title: true,
            },
            order_by: [
                { author: { email: asc } },
                { author: { id: desc } },
            ],
        }
    }
    .to_sql()
    .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "post__id","#,
            r#""t0"."title" AS "post__title""#,
            r#"FROM "post" AS "t0""#,
            r#"LEFT JOIN "user" AS "t1" ON "t1"."id" = "t0"."author_id""#,
            r#"ORDER BY "t1"."email" ASC, "t1"."id" DESC"#,
        ]
        .join(" ")
    );
}

#[test]
fn model_first_order_by_generates_expected_sql() {
    let sql = crate::statements_schema::query::<PostOrderedByTitleDesc>()
        .to_sql()
        .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "post__id","#,
            r#""t0"."title" AS "post__title""#,
            r#"FROM "post" AS "t0""#,
            r#"ORDER BY "t0"."title" DESC"#,
        ]
        .join(" ")
    );
}

#[test]
fn model_first_where_generates_expected_sql() {
    let sql = crate::statements_schema::query_with_variables::<UserById>(UserByIdVariables {
        user_id: 7,
    })
    .to_sql()
    .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "user__id","#,
            r#""t0"."email" AS "user__email""#,
            r#"FROM "user" AS "t0""#,
            r#"WHERE "t0"."id" = ?1"#,
        ]
        .join(" ")
    );
}

#[test]
fn model_first_null_where_generates_expected_sql() {
    let sql = crate::statements_schema::query::<PostWithNullBody>()
        .to_sql()
        .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "post__id","#,
            r#""t0"."title" AS "post__title","#,
            r#""t0"."body" AS "post__body""#,
            r#"FROM "post" AS "t0""#,
            r#"WHERE "t0"."body" IS NULL"#,
        ]
        .join(" ")
    );
}

#[test]
fn model_first_not_null_where_generates_expected_sql() {
    let sql = crate::statements_schema::query::<PostWithNonNullBody>()
        .to_sql()
        .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "post__id","#,
            r#""t0"."title" AS "post__title","#,
            r#""t0"."body" AS "post__body""#,
            r#"FROM "post" AS "t0""#,
            r#"WHERE "t0"."body" IS NOT NULL"#,
        ]
        .join(" ")
    );
}

#[test]
fn model_first_not_equal_where_generates_expected_sql() {
    let sql = crate::statements_schema::query_with_variables::<PostWithDifferentTitle>(
        PostByExcludedTitleVariables {
            excluded_title: "Draft".to_owned(),
        },
    )
    .to_sql()
    .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "post__id","#,
            r#""t0"."title" AS "post__title""#,
            r#"FROM "post" AS "t0""#,
            r#"WHERE "t0"."title" <> ?1"#,
        ]
        .join(" ")
    );
}

#[test]
fn model_first_in_where_generates_expected_sql() {
    let sql = crate::statements_schema::query_with_variables::<PostByIds>(PostsByIdsVariables {
        post_ids: vec![7, 11],
    })
    .to_sql()
    .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "post__id","#,
            r#""t0"."title" AS "post__title""#,
            r#"FROM "post" AS "t0""#,
            r#"WHERE "t0"."id" IN (?1, ?2)"#,
        ]
        .join(" ")
    );
}

#[test]
fn nested_model_first_where_generates_expected_sql() {
    let sql = crate::statements_schema::query_with_variables::<UserWithFilteredPosts>(
        UserWithFilteredPostsVariables {
            user_id: 7,
            post_id: 11,
        },
    )
    .to_sql()
    .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "user__id","#,
            r#""t0"."email" AS "user__email","#,
            r#"(SELECT COALESCE(json_group_array(json("__vitrail_nested_rows"."data")), json('[]')) AS "data" FROM (SELECT json_array("t1"."id", "t1"."title") AS "data" FROM "post" AS "t1" WHERE "t1"."author_id" = "t0"."id" AND "t1"."id" = ?1 ORDER BY "t1"."id") AS "__vitrail_nested_rows") AS "user__posts""#,
            r#"FROM "user" AS "t0""#,
            r#"WHERE "t0"."id" = ?2"#,
        ]
        .join(" ")
    );
}

#[test]
fn nested_query_recursively_lateralizes_nested_includes() {
    let sql = crate::statements_schema::query::<UserWithPostsAndComments>()
        .to_sql()
        .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "user__id","#,
            r#""t0"."email" AS "user__email","#,
            r#"(SELECT COALESCE(json_group_array(json("__vitrail_nested_rows"."data")), json('[]')) AS "data" FROM (SELECT json_array("t1"."id", "t1"."title", json((SELECT COALESCE(json_group_array(json("__vitrail_nested_rows"."data")), json('[]')) AS "data" FROM (SELECT json_array("t2"."id", "t2"."body") AS "data" FROM "comment" AS "t2" WHERE "t2"."post_id" = "t1"."id" ORDER BY "t2"."id") AS "__vitrail_nested_rows"))) AS "data" FROM "post" AS "t1" WHERE "t1"."author_id" = "t0"."id" ORDER BY "t1"."id") AS "__vitrail_nested_rows") AS "user__posts""#,
            r#"FROM "user" AS "t0""#,
        ]
        .join(" ")
    );
}

#[test]
fn model_first_insert_generates_expected_sql() {
    let sql = crate::statements_schema::insert::<InsertedUser>(NewUser {
        email: "alice@example.com".to_owned(),
        name: "Alice".to_owned(),
    })
    .to_sql()
    .unwrap();

    assert_eq!(
        sql,
        [
            r#"INSERT INTO "user" ("email", "name")"#,
            r#"VALUES (?1, ?2)"#,
            r#"RETURNING"#,
            r#""user"."id" AS "user__id","#,
            r#""user"."email" AS "user__email","#,
            r#""user"."name" AS "user__name","#,
            r#""user"."created_at" AS "user__created_at""#,
        ]
        .join(" ")
    );
}

#[test]
fn helper_insert_defaults_to_all_scalar_fields_when_select_is_omitted() {
    let sql = insert! {
        crate::statements_schema,
        user {
            data: {
                email: "alice@example.com".to_owned(),
                name: "Alice".to_owned(),
            },
        }
    }
    .to_sql()
    .unwrap();

    assert_eq!(
        sql,
        [
            r#"INSERT INTO "user" ("email", "name")"#,
            r#"VALUES (?1, ?2)"#,
            r#"RETURNING"#,
            r#""user"."id" AS "user__id","#,
            r#""user"."email" AS "user__email","#,
            r#""user"."name" AS "user__name","#,
            r#""user"."created_at" AS "user__created_at""#,
        ]
        .join(" ")
    );
}

#[test]
fn helper_insert_with_nullable_field_and_subset_select_generates_expected_sql() {
    let sql = insert! {
        crate::statements_schema,
        post {
            data: {
                title: "Hello from Vitrail".to_owned(),
                body: None,
                published: true,
                author_id: 7_i64,
            },
            select: {
                id: true,
                title: true,
                body: true,
            },
        }
    }
    .to_sql()
    .unwrap();

    assert_eq!(
        sql,
        [
            r#"INSERT INTO "post" ("title", "body", "published", "author_id")"#,
            r#"VALUES (?1, ?2, ?3, ?4)"#,
            r#"RETURNING"#,
            r#""post"."id" AS "post__id","#,
            r#""post"."title" AS "post__title","#,
            r#""post"."body" AS "post__body""#,
        ]
        .join(" ")
    );
}

schema! {
    name compound_statements_schema

    model post_locale {
        post_id Int
        locale String
        title String
        notes translation_note[]

        @@id([post_id, locale])
    }

    model translation_note {
        id Int @id @default(autoincrement())
        post_id Int
        locale String
        body String
        translation post_locale @relation(fields: [post_id, locale], references: [post_id, locale])
    }
}

pub(crate) use self::compound_statements_schema as sqlite_compound_statements_schema;

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(schema = crate::compound_statements_schema::Schema, model = translation_note)]
struct TranslationNoteWithTranslation {
    id: i64,
    body: String,
    #[vitrail(include)]
    translation: PostLocaleSummary,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(schema = crate::compound_statements_schema::Schema, model = post_locale)]
struct PostLocaleSummary {
    post_id: i64,
    locale: String,
    title: String,
}

#[test]
fn compound_to_one_include_generates_expected_sql() {
    let sql = crate::compound_statements_schema::query::<TranslationNoteWithTranslation>()
        .to_sql()
        .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#""t0"."id" AS "translation_note__id","#,
            r#""t0"."body" AS "translation_note__body","#,
            r#"(SELECT json_array("t1"."post_id", "t1"."locale", "t1"."title") AS "data" FROM "post_locale" AS "t1" WHERE "t1"."post_id" = "t0"."post_id" AND "t1"."locale" = "t0"."locale" LIMIT 1) AS "translation_note__translation""#,
            r#"FROM "translation_note" AS "t0""#,
        ]
        .join(" ")
    );
}
