pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{UpdateData, UpdateMany, schema};
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
struct PublishPostsData {
    published: bool,
}

#[derive(UpdateMany)]
#[vitrail(
    schema = crate::update_schema::Schema,
    model = post,
    data = PublishPostsData,
    where(id = in(post_ids))
)]
struct PublishPostsByIds;

fn main() {
    let _ = crate::update_schema::update_many::<PublishPostsByIds>(
        PublishPostsData { published: true },
    );
}
