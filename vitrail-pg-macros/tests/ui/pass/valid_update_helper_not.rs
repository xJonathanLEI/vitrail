pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{UpdateData, schema, update};
extern crate self as vitrail_pg;

schema! {
    name update_helper_not_schema

    model post {
        id        Int     @id @default(autoincrement())
        title     String
        published Boolean
    }
}

fn main() {
    let _ = update! {
        crate::update_helper_not_schema,
        post {
            data: {
                published: true,
            },
            where: {
                title: {
                    not: "Draft".to_owned()
                },
            },
        }
    };
}
