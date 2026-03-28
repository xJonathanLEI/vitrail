use vitrail_pg::{QueryVariables, UpdateData, UpdateMany, schema};

schema! {
    name update_schema

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

#[derive(UpdateData)]
#[vitrail(schema = crate::update_schema::Schema, model = post)]
struct PublishPostsData {
    published: bool,
}

#[derive(QueryVariables)]
struct AuthorAgeVariables {
    author_age: i64,
}

#[derive(UpdateMany)]
#[vitrail(
    schema = crate::update_schema::Schema,
    model = post,
    data = PublishPostsData,
    variables = AuthorAgeVariables,
    where(author.age = eq(author_age))
)]
struct PublishPostsByAuthorAge;

fn main() {
    let _ = crate::update_schema::update_many_with_variables::<PublishPostsByAuthorAge>(
        AuthorAgeVariables { author_age: 30 },
        PublishPostsData { published: true },
    );
}
