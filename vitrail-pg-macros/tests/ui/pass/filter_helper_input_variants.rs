pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{QueryResult, query, schema};
extern crate self as vitrail_pg;

schema! {
    name filter_helper_input_variants_schema

    model post {
        id        Int     @id @default(autoincrement())
        title     String
        published Boolean
    }
}

fn main() {
    let _ = query! {
        crate::filter_helper_input_variants_schema,
        post {
            select: {
                id: true,
                title: true,
            },
            where: {
                title: {
                    eq: "Hello Vitrail"
                },
            },
        }
    };

    let _ = query! {
        crate::filter_helper_input_variants_schema,
        post {
            select: {
                id: true,
                title: true,
            },
            where: {
                title: {
                    not: "Draft"
                },
            },
        }
    };

    let _ = query! {
        crate::filter_helper_input_variants_schema,
        post {
            select: {
                id: true,
                title: true,
            },
            where: {
                title: {
                    in: ["Hello Vitrail", "Second post"]
                },
            },
        }
    };

    let _ = query! {
        crate::filter_helper_input_variants_schema,
        post {
            select: {
                id: true,
                title: true,
            },
            where: {
                id: {
                    in: [1_i64, 2_i64, 3_i64]
                },
            },
        }
    };

}
