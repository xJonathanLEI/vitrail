use vitrail_pg::{InsertInput, InsertResult, QueryResult, QueryVariables, insert, query, schema};

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
            r#"("t0"."id")::bigint AS "post__id","#,
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
            r#"("t0"."id")::bigint AS "post__id","#,
            r#""t0"."title" AS "post__title","#,
            r#""t0"."body" AS "post__body""#,
            r#"FROM "post" AS "t0""#,
            r#"WHERE NOT ("t0"."body" IS NULL)"#,
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
fn model_first_null_where_generates_expected_sql() {
    let sql = crate::statements_schema::query::<PostWithNullBody>()
        .to_sql()
        .unwrap();

    assert_eq!(
        sql,
        [
            r#"SELECT"#,
            r#"("t0"."id")::bigint AS "post__id","#,
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
            r#"("t0"."id")::bigint AS "post__id","#,
            r#""t0"."title" AS "post__title","#,
            r#""t0"."body" AS "post__body""#,
            r#"FROM "post" AS "t0""#,
            r#"WHERE NOT ("t0"."body" IS NULL)"#,
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
            r#"VALUES ($1, $2)"#,
            r#"RETURNING"#,
            r#"("user"."id")::bigint AS "user__id","#,
            r#""user"."email" AS "user__email","#,
            r#""user"."name" AS "user__name","#,
            r#"("user"."created_at" AT TIME ZONE 'UTC') AS "user__created_at""#,
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
            r#"VALUES ($1, $2)"#,
            r#"RETURNING"#,
            r#"("user"."id")::bigint AS "user__id","#,
            r#""user"."email" AS "user__email","#,
            r#""user"."name" AS "user__name","#,
            r#"("user"."created_at" AT TIME ZONE 'UTC') AS "user__created_at""#,
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
            r#"VALUES ($1, $2, $3, $4)"#,
            r#"RETURNING"#,
            r#"("post"."id")::bigint AS "post__id","#,
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

pub(crate) use self::compound_statements_schema as pg_compound_statements_schema;

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
            r#"("t0"."id")::bigint AS "translation_note__id","#,
            r#""t0"."body" AS "translation_note__body","#,
            r#""t1"."data" AS "translation_note__translation""#,
            r#"FROM "translation_note" AS "t0""#,
            r#"LEFT JOIN LATERAL (SELECT json_build_array(("t2"."post_id")::bigint, "t2"."locale", "t2"."title") AS "data" FROM "post_locale" AS "t2" WHERE "t2"."post_id" = "t0"."post_id" AND "t2"."locale" = "t0"."locale" LIMIT 1) AS "t1" ON TRUE"#,
        ]
        .join(" ")
    );
}
