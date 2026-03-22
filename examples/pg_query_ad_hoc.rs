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

#[tokio::main]
async fn main() {
    let client = my_schema::VitrailClient::new("postgres://127.0.0.1:5432/vitrail")
        .await
        .unwrap();

    let posts = client
        .find_many(query! {
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
        })
        .await
        .unwrap();
    let post = &posts[0];

    println!(
        "Post #{}: {} (#{} {} [{}]; joined {})",
        post.id,
        post.title,
        post.author.id,
        post.author.name,
        post.author.email,
        post.author.created_at
    );
}
