use vitrail_pg::{insert, schema};

schema! {
    name insert_schema

    model user {
        id    Int    @id @default(autoincrement())
        email String
        name  String
    }
}

fn main() {
    let _ = insert! {
        crate::insert_schema,
        user {
            data: {
                name: "Alice".to_owned(),
            },
        }
    };
}
