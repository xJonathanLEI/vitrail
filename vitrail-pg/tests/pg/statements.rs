use vitrail_pg::{QueryResult, QueryVariables, query, schema};

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

pub(crate) use self::statements_schema as pg_statements_schema;

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
            r#"("t0"."id")::bigint AS "user__id","#,
            r#""t0"."email" AS "user__email","#,
            r#""t0"."name" AS "user__name","#,
            r#"("t0"."created_at" AT TIME ZONE 'UTC') AS "user__created_at""#,
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
            r#"("t0"."id")::bigint AS "user__id","#,
            r#""t0"."email" AS "user__email","#,
            r#""t1"."data" AS "user__posts""#,
            r#"FROM "user" AS "t0""#,
            r#"LEFT JOIN LATERAL (SELECT COALESCE(json_agg(json_build_array(("t2"."id")::bigint, "t2"."title", "t2"."body", "t2"."published", ("t2"."author_id")::bigint, ("t2"."created_at" AT TIME ZONE 'UTC')) ORDER BY "t2"."id"), '[]'::json) AS "data" FROM "post" AS "t2" WHERE "t2"."author_id" = "t0"."id") AS "t1" ON TRUE"#,
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
            r#"("t0"."id")::bigint AS "user__id","#,
            r#""t0"."email" AS "user__email","#,
            r#""t1"."data" AS "user__posts""#,
            r#"FROM "user" AS "t0""#,
            r#"LEFT JOIN LATERAL (SELECT COALESCE(json_agg(json_build_array(("t2"."id")::bigint, "t2"."title") ORDER BY "t2"."id"), '[]'::json) AS "data" FROM "post" AS "t2" WHERE "t2"."author_id" = "t0"."id") AS "t1" ON TRUE"#,
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
            r#"("t0"."id")::bigint AS "post__id","#,
            r#""t0"."title" AS "post__title","#,
            r#""t1"."data" AS "post__author""#,
            r#"FROM "post" AS "t0""#,
            r#"LEFT JOIN LATERAL (SELECT json_build_array(("t2"."id")::bigint, "t2"."email", "t2"."name", ("t2"."created_at" AT TIME ZONE 'UTC')) AS "data" FROM "user" AS "t2" WHERE "t2"."id" = "t0"."author_id" LIMIT 1) AS "t1" ON TRUE"#,
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
            r#"("t0"."id")::bigint AS "user__id","#,
            r#""t0"."email" AS "user__email""#,
            r#"FROM "user" AS "t0""#,
            r#"WHERE ("t0"."id")::bigint = $1"#,
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
            r#"("t0"."id")::bigint AS "user__id","#,
            r#""t0"."email" AS "user__email""#,
            r#"FROM "user" AS "t0""#,
            r#"WHERE ("t0"."id")::bigint = $1"#,
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
            r#"("t0"."id")::bigint AS "user__id","#,
            r#""t0"."email" AS "user__email","#,
            r#""t1"."data" AS "user__posts""#,
            r#"FROM "user" AS "t0""#,
            r#"LEFT JOIN LATERAL (SELECT COALESCE(json_agg(json_build_array(("t2"."id")::bigint, "t2"."title") ORDER BY "t2"."id"), '[]'::json) AS "data" FROM "post" AS "t2" WHERE "t2"."author_id" = "t0"."id" AND ("t2"."id")::bigint = $1) AS "t1" ON TRUE"#,
            r#"WHERE ("t0"."id")::bigint = $2"#,
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
            r#"("t0"."id")::bigint AS "user__id","#,
            r#""t0"."email" AS "user__email","#,
            r#""t1"."data" AS "user__posts""#,
            r#"FROM "user" AS "t0""#,
            r#"LEFT JOIN LATERAL (SELECT COALESCE(json_agg(json_build_array(("t2"."id")::bigint, "t2"."title", "t3"."data") ORDER BY "t2"."id"), '[]'::json) AS "data" FROM "post" AS "t2" LEFT JOIN LATERAL (SELECT COALESCE(json_agg(json_build_array(("t4"."id")::bigint, "t4"."body") ORDER BY "t4"."id"), '[]'::json) AS "data" FROM "comment" AS "t4" WHERE "t4"."post_id" = "t2"."id") AS "t3" ON TRUE WHERE "t2"."author_id" = "t0"."id") AS "t1" ON TRUE"#,
        ]
        .join(" ")
    );
}
