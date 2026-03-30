pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{DeleteMany, schema};
extern crate self as vitrail_pg;

schema! {
    name delete_schema

    model post {
        id    Int    @id @default(autoincrement())
        title String
    }
}

#[derive(DeleteMany)]
#[vitrail(
    schema = crate::delete_schema::Schema,
    model = post,
    where(title = not(excluded_title))
)]
struct DeletePostsByExcludedTitle;

fn main() {
    let _ = crate::delete_schema::delete_many::<DeletePostsByExcludedTitle>();
}
