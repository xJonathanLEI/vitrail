use vitrail_pg::{VitrailClient, delete, insert, query, schema, update, uuid::Uuid};

schema! {
    name my_schema

    model user {
        id          Int      @id @default(autoincrement())
        external_id String   @unique @db.Uuid
        email       String   @unique
        name        String
        created_at  DateTime @default(now())
        posts       post[]
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

#[tokio::main]
async fn main() {
    let client = VitrailClient::new("postgres://postgres:postgres@127.0.0.1:5432/vitrail")
        .await
        .unwrap();
    let external_id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap();

    let user = client
        .insert(insert! {
            crate::my_schema,
            user {
                data: {
                    external_id: external_id,
                    email: "alice@example.com".to_owned(),
                    name: "Alice".to_owned(),
                },
            }
        })
        .await
        .unwrap();

    client
        .insert(insert! {
            crate::my_schema,
            post {
                data: {
                    title: "Hello Vitrail".to_owned(),
                    body: Some("Draft body".to_owned()),
                    published: false,
                    author_id: user.id,
                },
            }
        })
        .await
        .unwrap();

    client
        .insert(insert! {
            crate::my_schema,
            post {
                data: {
                    title: "Untitled draft".to_owned(),
                    body: None,
                    published: false,
                    author_id: user.id,
                },
            }
        })
        .await
        .unwrap();

    let updated_posts = client
        .update_many(update! {
            crate::my_schema,
            post {
                data: {
                    published: true,
                },
                where: {
                    author: {
                        email: {
                            eq: "alice@example.com".to_owned()
                        }
                    },
                },
            }
        })
        .await
        .unwrap();

    let deleted_posts = client
        .delete_many(delete! {
            crate::my_schema,
            post {
                where: {
                    body: null,
                },
            }
        })
        .await
        .unwrap();

    let users = client
        .find_many(query! {
            crate::my_schema,
            user {
                select: {
                    id: true,
                    external_id: true,
                    email: true,
                    name: true,
                },
                include: {
                    posts: {
                        select: {
                            id: true,
                            title: true,
                        },
                    },
                },
                where: {
                    external_id: {
                        eq: external_id
                    },
                },
            }
        })
        .await
        .unwrap();

    println!("inserted user {} ({})", user.email, user.external_id);
    println!("updated {} posts", updated_posts);
    println!("deleted {} posts", deleted_posts);
    println!("fetched {} users", users.len());
}
