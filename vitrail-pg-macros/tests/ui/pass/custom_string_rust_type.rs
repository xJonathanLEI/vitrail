pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{
    InsertInput, InsertResult, QueryResult, QueryVariables, UpdateData, schema,
};
extern crate self as vitrail_pg;

#[derive(Clone, Debug, Eq, PartialEq)]
struct PostalCode(String);

impl StringValueType for PostalCode {
    fn from_db_string(value: String) -> Result<Self, sqlx::Error> {
        Ok(Self(value))
    }

    fn into_db_string(self) -> String {
        self.0
    }
}

schema! {
    name custom_string_rust_type_schema

    model address {
        id          Int    @id @default(autoincrement())
        postal_code String @rust_ty(crate::PostalCode)
    }
}

#[derive(InsertInput)]
#[vitrail(schema = crate::custom_string_rust_type_schema::Schema, model = address)]
struct NewAddress {
    postal_code: PostalCode,
}

#[derive(InsertResult)]
#[vitrail(
    schema = crate::custom_string_rust_type_schema::Schema,
    model = address,
    input = NewAddress
)]
struct InsertedAddress {
    id: i64,
    postal_code: PostalCode,
}

#[derive(UpdateData)]
#[vitrail(schema = crate::custom_string_rust_type_schema::Schema, model = address)]
struct UpdateAddress {
    postal_code: PostalCode,
}

#[derive(QueryVariables)]
struct AddressVariables {
    postal_code: PostalCode,
}

#[derive(QueryResult)]
#[vitrail(
    schema = crate::custom_string_rust_type_schema::Schema,
    model = address,
    variables = AddressVariables,
    where(postal_code = eq(postal_code))
)]
struct AddressSummary {
    id: i64,
    postal_code: PostalCode,
}

fn main() {
    let _ = crate::custom_string_rust_type_schema::insert::<InsertedAddress>(NewAddress {
        postal_code: PostalCode("75001".to_owned()),
    });
    let _ = UpdateAddress {
        postal_code: PostalCode("94130".to_owned()),
    };
    let _ = crate::custom_string_rust_type_schema::query_with_variables::<AddressSummary>(
        AddressVariables {
            postal_code: PostalCode("75001".to_owned()),
        },
    );
}
