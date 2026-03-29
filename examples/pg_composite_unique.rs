use vitrail_pg::{PostgresSchema, schema};

schema! {
    name my_schema

    model post {
        id      Int           @id @default(autoincrement())
        title   String
        locales post_locale[]
    }

    model post_locale {
        id      Int    @id @default(autoincrement())
        post_id Int
        locale  String
        title   String
        post    post   @relation(fields: [post_id], references: [id])

        // Each post can have at most one row for a given locale.
        @@unique([post_id, locale])
    }
}

fn main() {
    let sql = PostgresSchema::from_schema_access::<my_schema::Schema>()
        .migrate_from(&PostgresSchema::empty())
        .to_sql();

    println!("Generated migration SQL:");
    println!("{sql}");
}
