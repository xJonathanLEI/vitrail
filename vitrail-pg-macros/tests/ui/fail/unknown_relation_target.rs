use vitrail_pg_macros::schema;

schema! {
    name my_schema

    model post {
        id      Int  @id
        user_id Int
        user    User @relation(fields: [user_id], references: [id])
    }
}

fn main() {}
