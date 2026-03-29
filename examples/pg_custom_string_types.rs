#![allow(clippy::crate_in_macro_def)]
use sqlx::postgres::PgPoolOptions;
use vitrail_pg::{
    QueryResult, QueryVariables, StringValueType, VitrailClient, insert, query, schema,
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct PostalCode(String);

impl PostalCode {
    fn parse(value: impl Into<String>) -> Result<Self, PostalCodeError> {
        let value = value.into();
        let is_valid = value.len() == 5 && value.chars().all(|ch| ch.is_ascii_digit());

        if is_valid {
            Ok(Self(value))
        } else {
            Err(PostalCodeError(value))
        }
    }
}

#[derive(Debug)]
struct PostalCodeError(String);

impl std::fmt::Display for PostalCodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid postal code `{}`", self.0)
    }
}

impl std::error::Error for PostalCodeError {}

impl StringValueType for PostalCode {
    fn from_db_string(value: String) -> Result<Self, sqlx::Error> {
        PostalCode::parse(value).map_err(|error| sqlx::Error::Decode(Box::new(error)))
    }

    fn into_db_string(self) -> String {
        self.0
    }
}

schema! {
    name my_schema

    model country {
        id        Int     @id @default(autoincrement())
        name      String
        addresses address[]
    }

    model address {
        id          Int     @id @default(autoincrement())
        postal_code String  @rust_ty(crate::PostalCode)
        country_id   Int
        country      country @relation(fields: [country_id], references: [id])
    }
}

#[derive(QueryVariables)]
struct AddressByPostalCodeVariables {
    postal_code: PostalCode,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(
    schema = crate::my_schema::Schema,
    model = address,
    variables = AddressByPostalCodeVariables,
    where(postal_code = eq(postal_code))
)]
struct AddressSummary {
    id: i64,
    postal_code: PostalCode,
}

#[tokio::main]
async fn main() {
    let database_url = "postgres://postgres:postgres@127.0.0.1:5432/vitrail";
    let client = VitrailClient::new(database_url).await.unwrap();
    let pool = PgPoolOptions::new().connect(database_url).await.unwrap();

    let country_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO "country" ("name")
        VALUES ('France')
        RETURNING "id"::bigint
        "#,
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let inserted = client
        .insert(insert! {
            crate::my_schema,
            address {
                data: {
                    postal_code: PostalCode::parse("75001").unwrap(),
                    country_id: country_id,
                },
                select: {
                    id: true,
                    postal_code: true,
                },
            }
        })
        .await
        .unwrap();

    let matches = client
        .find_many(crate::my_schema::query_with_variables::<AddressSummary>(
            AddressByPostalCodeVariables {
                postal_code: PostalCode::parse("75001").unwrap(),
            },
        ))
        .await
        .unwrap();

    let countries = client
        .find_many(query! {
            crate::my_schema,
            country {
                select: {
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
            }
        })
        .await
        .unwrap();

    println!(
        "inserted address {} with {}",
        inserted.id, inserted.postal_code.0
    );
    println!("matched {} address rows", matches.len());
    println!("loaded {} countries", countries.len());

    pool.close().await;
    client.close().await;
}
