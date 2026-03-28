pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{UpdateData, schema, update};
extern crate self as vitrail_pg;

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
                published: {
                    gt: false
                },
            },
        }
    };
}
