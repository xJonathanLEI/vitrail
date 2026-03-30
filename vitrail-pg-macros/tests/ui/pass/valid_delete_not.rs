pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{DeleteMany, QueryVariables, schema};
extern crate self as vitrail_pg;

schema! {
    name delete_not_schema

    model post {
        id    Int    @id @default(autoincrement())
        title String
    }
}

#[derive(QueryVariables)]
struct ExcludedTitleVariables {
    excluded_title: String,
}

#[derive(DeleteMany)]
#[vitrail(
    schema = crate::delete_not_schema::Schema,
    model = post,
    variables = ExcludedTitleVariables,
    where(title = not(excluded_title))
)]
struct DeletePostsByExcludedTitle;

fn main() {
    let _ = crate::delete_not_schema::delete_many_with_variables::<DeletePostsByExcludedTitle>(
        ExcludedTitleVariables {
            excluded_title: "Draft".to_owned(),
        },
    );
}
