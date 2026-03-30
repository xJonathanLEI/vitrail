pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{delete, schema};
extern crate self as vitrail_pg;

schema! {
    name delete_helper_not_schema

    model post {
        id    Int    @id @default(autoincrement())
        title String
    }
}

fn main() {
    let _ = delete! {
        crate::delete_helper_not_schema,
        post {
            where: {
                title: {
                    not: "Draft".to_owned()
                },
            },
        }
    };
}
