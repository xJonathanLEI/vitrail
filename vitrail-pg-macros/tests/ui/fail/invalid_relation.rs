use vitrail_pg_macros::schema;

schema! {
    model user {
        id Int @id
    }

    model post {
        id      Int  @id
        user_id Int
        user    user @relation(fields: [missing_field], references: [id])
    }
}

fn main() {}
