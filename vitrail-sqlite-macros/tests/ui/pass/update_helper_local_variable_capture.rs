pub use vitrail_sqlite_core::*;
pub use vitrail_sqlite_macros::{UpdateData, UpdateMany, schema, update};
extern crate self as vitrail_sqlite;

schema! {
    name update_helper_local_variable_capture_schema

    model post {
        id        Int     @id @default(autoincrement())
        title     String
        published Boolean
    }
}

fn main() {
    let excluded_title = "Draft".to_owned();

    // Helper macros should accept function-local values in filters.
    let _ = update! {
        crate::update_helper_local_variable_capture_schema,
        post {
            data: {
                published: true,
            },
            where: {
                title: {
                    not: excluded_title
                },
            },
        }
    };
}
