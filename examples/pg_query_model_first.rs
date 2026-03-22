use vitrail_pg::{QueryResult, schema};

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

#[derive(QueryResult)]
#[vitrail(schema = crate::my_schema::Schema, model = user)]
struct UserSummary {
    id: i64,
    email: String,
    name: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(QueryResult)]
#[vitrail(schema = crate::my_schema::Schema, model = post)]
struct PostWithAuthor {
    id: i64,
    title: String,
    #[vitrail(include)]
    author: UserSummary,
}

#[tokio::main]
async fn main() {
    let client = my_schema::VitrailClient::new("postgres://127.0.0.1:5432/vitrail")
        .await
        .unwrap();

    let posts = client
        .find_many(my_schema::query::<PostWithAuthor>())
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
