use vitrail_pg_macros::schema;

schema! {
    name my_schema

    model user {
        id Int @id
        id Int
    }
}

fn main() {}
