use vitrail_pg_macros::schema;

schema! {
    model user {
        id String @id @default(autoincrement())
    }
}

fn main() {}
