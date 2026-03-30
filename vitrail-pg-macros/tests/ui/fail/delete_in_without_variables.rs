pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{DeleteMany, schema};
extern crate self as vitrail_pg;

schema! {
    name delete_schema

    model comment {
        id   Int    @id @default(autoincrement())
        body String
    }
}

#[derive(DeleteMany)]
#[vitrail(
    schema = crate::delete_schema::Schema,
    model = comment,
    where(id = in(comment_ids))
)]
struct DeleteCommentsByIds;

fn main() {
    let _ = crate::delete_schema::delete_many::<DeleteCommentsByIds>();
}
