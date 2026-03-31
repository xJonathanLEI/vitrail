pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{delete, schema};
extern crate self as vitrail_pg;

schema! {
    name delete_helper_local_variable_capture_schema

    model post {
        id    Int    @id @default(autoincrement())
        title String
    }
}

fn main() {
    let excluded_title = "Draft".to_owned();

    // Helper macros should accept function-local values in filters.
    let _ = delete! {
        crate::delete_helper_local_variable_capture_schema,
        post {
            where: {
                title: {
                    not: excluded_title
                },
            },
        }
    };
}
