use vitrail_pg::{VitrailClient, query, schema};

schema! {
    name my_schema

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
    }
}

#[tokio::main]
async fn main() {
    let client = VitrailClient::new("postgres://postgres:postgres@127.0.0.1:5432/vitrail")
        .await
        .unwrap();

    let user_id = 1_i64;

    let users = client
        .find_many(query! {
            crate::my_schema,
            user {
                select: {
                    id: true,
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
                    id: {
                        eq: user_id
                    },
                },
            }
        })
        .await
        .unwrap();

    println!("fetched {} users", users.len());
}
