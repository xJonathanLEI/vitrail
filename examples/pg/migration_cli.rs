use vitrail_pg::{run_cli, schema};

schema! {
    name my_schema

    model user {
        id         Int      @id @default(autoincrement())
        email      String   @unique
        name       String   @index
        created_at DateTime @default(now())
    }

    model post {
        id         Int      @id @default(autoincrement())
        title      String
        body       String?
        published  Boolean
        author_id  Int      @index
        created_at DateTime @default(now())
        author     user     @relation(fields: [author_id], references: [id])

        @@index([published, created_at])
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // This wires the generated schema into Vitrail's migration CLI.
    //
    // Example usage:
    //   cargo run --example pg_migration_cli -- \
    //     migrate dev \
    //     --name init \
    //     --database-url postgres://postgres:postgres@127.0.0.1:5432/vitrail
    //
    //   cargo run --example pg_migration_cli -- \
    //     migrate deploy \
    //     --database-url postgres://postgres:postgres@127.0.0.1:5432/vitrail
    //
    //   cargo run --example pg_migration_cli -- \
    //     migrate status \
    //     --database-url postgres://postgres:postgres@127.0.0.1:5432/vitrail
    //
    // You can also set `VITRAIL_DATABASE_URL` instead of passing `--database-url`.
    run_cli::<my_schema::Schema>().await
}
