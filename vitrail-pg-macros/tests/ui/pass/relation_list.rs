use vitrail_pg_macros::schema;

schema! {
    name relation_list_schema

    model user {
        id    Int    @id @default(autoincrement())
        posts post[]
    }

    model post {
        id        Int    @id @default(autoincrement())
        author_id Int
        author    user   @relation(fields: [author_id], references: [id])
    }
}

fn main() {}
