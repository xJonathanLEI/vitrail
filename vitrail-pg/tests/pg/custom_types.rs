#![allow(clippy::crate_in_macro_def)]
use crate::support::{TestDatabase, apply_schema};
use sqlx::postgres::PgPoolOptions;
use vitrail_pg::{
    PostgresSchema, QueryResult, QueryVariables, StringValueType, UpdateData, UpdateMany,
    VitrailClient, insert, query, schema,
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct PostalCode {
    inner: String,
}

impl PostalCode {
    fn parse(value: impl Into<String>) -> Result<Self, PostalCodeError> {
        let value = value.into();
        let is_valid = value.len() == 5 && value.chars().all(|ch| ch.is_ascii_digit());

        if is_valid {
            Ok(Self { inner: value })
        } else {
            Err(PostalCodeError(value))
        }
    }

    fn as_str(&self) -> &str {
        &self.inner
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
        self.inner
    }
}

schema! {
    name custom_types_schema

    model country {
        id        Int     @id @default(autoincrement())
        name      String
        addresses address[]
    }

    model address {
        id          Int     @id @default(autoincrement())
        postal_code String  @rust_ty(crate::custom_types::PostalCode)
        country_id   Int
        country      country @relation(fields: [country_id], references: [id])
    }
}

pub(crate) use self::custom_types_schema as pg_custom_types_schema;

#[derive(QueryVariables)]
struct AddressByPostalCodeVariables {
    postal_code: PostalCode,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(
    schema = crate::custom_types_schema::Schema,
    model = address,
    variables = AddressByPostalCodeVariables,
    where(postal_code = eq(postal_code))
)]
struct AddressSummary {
    id: i64,
    postal_code: PostalCode,
}

#[derive(UpdateData)]
#[vitrail(schema = crate::custom_types_schema::Schema, model = address)]
struct UpdatePostalCodeData {
    postal_code: PostalCode,
}

#[derive(UpdateMany)]
#[vitrail(
    schema = crate::custom_types_schema::Schema,
    model = address,
    data = UpdatePostalCodeData,
    variables = AddressByPostalCodeVariables,
    where(postal_code = eq(postal_code))
)]
struct UpdatePostalCodeByValue;

async fn setup_database(database_url: &str) -> i64 {
    apply_schema(
        database_url,
        &PostgresSchema::from_schema_access::<crate::custom_types_schema::Schema>(),
    )
    .await;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
        .expect("should connect to postgres");

    let country_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO "country" ("name")
        VALUES ('France')
        RETURNING "id"::bigint
        "#,
    )
    .fetch_one(&pool)
    .await
    .expect("should insert country");

    pool.close().await;
    country_id
}

#[tokio::test]
async fn string_backed_custom_types_work_across_insert_query_update_and_include() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();
    let country_id = setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let inserted_address = client
        .insert(insert! {
            crate::custom_types_schema,
            address {
                data: {
                    postal_code: PostalCode::parse("75001").expect("postal code should parse"),
                    country_id: country_id,
                },
                select: {
                    id: true,
                    postal_code: true,
                },
            }
        })
        .await
        .expect("insert should succeed");

    assert!(inserted_address.id > 0);
    assert_eq!(inserted_address.postal_code.as_str(), "75001");

    let addresses = client
        .find_many(crate::custom_types_schema::query_with_variables::<
            AddressSummary,
        >(AddressByPostalCodeVariables {
            postal_code: PostalCode::parse("75001").expect("postal code should parse"),
        }))
        .await
        .expect("query should succeed");

    assert_eq!(addresses.len(), 1);
    assert_eq!(addresses[0].postal_code.as_str(), "75001");

    let updated_count = client
        .update_many(crate::custom_types_schema::update_many_with_variables::<
            UpdatePostalCodeByValue,
        >(
            AddressByPostalCodeVariables {
                postal_code: PostalCode::parse("75001").expect("postal code should parse"),
            },
            UpdatePostalCodeData {
                postal_code: PostalCode::parse("94130").expect("postal code should parse"),
            },
        ))
        .await
        .expect("update should succeed");

    assert_eq!(updated_count, 1);

    let countries = client
        .find_many(query! {
            crate::custom_types_schema,
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
        .expect("query with include should succeed");

    assert_eq!(countries.len(), 1);
    assert_eq!(countries[0].name, "France");
    assert_eq!(countries[0].addresses.len(), 1);
    assert_eq!(countries[0].addresses[0].postal_code.as_str(), "94130");

    client.close().await;
    database.cleanup().await;
}
