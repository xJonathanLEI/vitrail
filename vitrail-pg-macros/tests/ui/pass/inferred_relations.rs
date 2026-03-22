use vitrail_pg_macros::schema;

schema! {
    name inferred_relations_schema

    model user {
        id     Int     @id @default(autoincrement())
        uid    String  @unique @db.Uuid
        post   Post?
        status String
    }

    model post {
        id         Int      @id @default(autoincrement())
        uid        String   @unique @db.Uuid
        user_id    Int
        created_at DateTime @default(now())
        user       user     @relation(fields: [user_id], references: [id])
    }
}

fn main() {}
