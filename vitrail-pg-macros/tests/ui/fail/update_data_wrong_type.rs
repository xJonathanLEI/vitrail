pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{UpdateData, schema};
extern crate self as vitrail_pg;

schema! {
    name update_schema

    model post {
        id        Int     @id @default(autoincrement())
        title     String
        published Boolean
    }
}

#[derive(UpdateData)]
#[vitrail(schema = crate::update_schema::Schema, model = post)]
struct UpdatePostData {
    published: i64,
}

fn main() {}
