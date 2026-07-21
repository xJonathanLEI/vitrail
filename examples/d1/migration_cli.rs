#[cfg(not(target_arch = "wasm32"))]
use vitrail_d1::{run_cli, schema};

#[cfg(not(target_arch = "wasm32"))]
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
        author_id  Int
        created_at DateTime @default(now())
        author     user     @relation(fields: [author_id], references: [id])

        @@index([author_id, created_at])
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_cli::<my_schema::Schema>().await
}

#[cfg(target_arch = "wasm32")]
fn main() {}
