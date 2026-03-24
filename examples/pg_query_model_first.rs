use vitrail_pg::{QueryResult, schema};

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

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(schema = crate::my_schema::Schema, model = post)]
struct PostSummary {
    id: i64,
    title: String,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(schema = crate::my_schema::Schema, model = user)]
struct UserWithPosts {
    id: i64,
    email: String,
    name: String,
    #[vitrail(include)]
    posts: Vec<PostSummary>,
}

#[tokio::main]
async fn main() {
    let client =
        my_schema::VitrailClient::new("postgres://postgres:postgres@127.0.0.1:5432/vitrail")
            .await
            .unwrap();

    let users = client
        .find_many(my_schema::query::<UserWithPosts>())
        .await
        .unwrap();

    println!("fetched {} users", users.len());
}
