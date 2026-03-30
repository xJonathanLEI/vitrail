pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{QueryVariables, UpdateData, UpdateMany, schema};
extern crate self as vitrail_pg;

schema! {
    name update_not_schema

    model post {
        id        Int     @id @default(autoincrement())
        title     String
        published Boolean
    }
}

#[derive(UpdateData)]
#[vitrail(schema = crate::update_not_schema::Schema, model = post)]
struct PublishPostsData {
    published: bool,
}

#[derive(QueryVariables)]
struct ExcludedTitleVariables {
    excluded_title: String,
}

#[derive(UpdateMany)]
#[vitrail(
    schema = crate::update_not_schema::Schema,
    model = post,
    data = PublishPostsData,
    variables = ExcludedTitleVariables,
    where(title = not(excluded_title))
)]
struct PublishPostsByExcludedTitle;

fn main() {
    let _ = crate::update_not_schema::update_many_with_variables::<PublishPostsByExcludedTitle>(
        ExcludedTitleVariables {
            excluded_title: "Draft".to_owned(),
        },
        PublishPostsData { published: true },
    );
}
