use vitrail_pg_macros::schema;

schema! {
    model user {
        id      Int     @id @default(autoincrement())
        uid     String  @unique @db.Uuid
        status  String
        post    post?
        comment comment?
    }

    model post {
        id         Int      @id @default(autoincrement())
        uid        String   @unique @db.Uuid
        user_id    Int      @unique
        created_at DateTime @default(now())
        user       user     @relation(fields: [user_id], references: [id])
        comment    comment?
    }

    model comment {
        id      Int    @id @default(autoincrement())
        post_id Int    @unique
        body    String
        post    post   @relation(fields: [post_id], references: [id])
    }
}

fn main() {}
