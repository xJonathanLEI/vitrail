use vitrail_pg::{query, schema};

schema! {
    name my_schema

    model user {
        id         Int      @id @default(autoincrement())
        email      String   @unique
        name       String
        created_at DateTime @default(now())
    }

    model post {
        id         Int      @id @default(autoincrement())
        title      String
        body       String?
        published  Boolean
        author_id  Int
        created_at DateTime @default(now())
        author     user     @relation(fields: [author_id], references: [id])
    }
}

#[test]
fn scalar_only_query_generates_expected_sql() {
    let sql = query! {
        crate::my_schema,
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
            r#""t0"."created_at" AS "user__created_at""#,
            r#"FROM "user" AS "t0""#,
        ]
        .join(" ")
    );
}

#[test]
fn nested_query_generates_expected_sql() {
    let sql = query! {
        crate::my_schema,
        post {
            select: {
                id: true,
                title: true,
            },
            include: {
                author: true,
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
            r#"("t1"."id")::bigint AS "post__author__id","#,
            r#""t1"."email" AS "post__author__email","#,
            r#""t1"."name" AS "post__author__name","#,
            r#""t1"."created_at" AS "post__author__created_at""#,
            r#"FROM "post" AS "t0""#,
            r#"INNER JOIN "user" AS "t1" ON "t0"."author_id" = "t1"."id""#,
        ]
        .join(" ")
    );
}
