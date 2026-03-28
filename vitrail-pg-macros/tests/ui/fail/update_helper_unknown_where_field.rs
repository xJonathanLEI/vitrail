use vitrail_pg::{schema, update};

schema! {
    name update_helper_schema

    model user {
        id    Int    @id @default(autoincrement())
        email String @unique
        age   Int
        posts post[]
    }

    model post {
        id        Int     @id @default(autoincrement())
        title     String
        published Boolean
        author_id Int
        author    user    @relation(fields: [author_id], references: [id])
    }
}

fn main() {
    let _ = update! {
        crate::update_helper_schema,
        post {
            data: {
                published: true,
            },
            where: {
                author: {
                    unknown: {
                        eq: 30
                    }
                },
            },
        }
    };
}
