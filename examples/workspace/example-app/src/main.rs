use example_db::{PostalCode, app_schema};
use vitrail_pg::{insert, query};

fn main() {
    let postal_code = PostalCode::parse("75001").unwrap();

    let insert_sql = insert! {
        example_db::app_schema,
        address {
            data: {
                postal_code: postal_code.clone(),
                user_id: 1_i64,
            },
            select: {
                id: true,
                postal_code: true,
            },
        }
    }
    .to_sql()
    .unwrap();

    let query_sql = query! {
        example_db::app_schema,
        user {
            select: {
                id: true,
                email: true,
                name: true,
            },
            include: {
                addresses: {
                    select: {
                        id: true,
                        postal_code: true,
                    },
                },
            },
            where: {
                email: {
                    eq: "alice@example.com"
                },
            },
        }
    }
    .to_sql()
    .unwrap();

    println!("schema available via {:?}", app_schema::Schema);
    println!("postal code from example-db: {}", postal_code.as_str());
    println!("insert SQL:\n{insert_sql}");
    println!("query SQL:\n{query_sql}");
}
