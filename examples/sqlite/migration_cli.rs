use vitrail_sqlite::{run_cli, schema};

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
    // This wires the generated schema into Vitrail's SQLite migration CLI.
    //
    // Example usage:
    //   cargo run --example sqlite_migration_cli -- \
    //     migrate dev \
    //     --name init \
    //     --database-url sqlite://dev.db
    //
    //   cargo run --example sqlite_migration_cli -- \
    //     migrate deploy \
    //     --database-url sqlite://dev.db
    //
    //   cargo run --example sqlite_migration_cli -- \
    //     migrate status \
    //     --database-url sqlite://dev.db
    //
    // You can also set `VITRAIL_DATABASE_URL` instead of passing `--database-url`.
    run_cli::<my_schema::Schema>().await
}
