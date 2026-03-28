use vitrail_pg::{UpdateData, UpdateMany, schema};

schema! {
    name update_schema

    model user {
        id    Int    @id @default(autoincrement())
        email String
    }

    model post {
        id    Int    @id @default(autoincrement())
        title String
    }
}

#[derive(UpdateData)]
#[vitrail(schema = crate::update_schema::Schema, model = user)]
struct UpdateUserData {
    email: String,
}

#[derive(UpdateMany)]
#[vitrail(
    schema = crate::update_schema::Schema,
    model = post,
    data = UpdateUserData
)]
struct UpdatePosts;

fn main() {}
