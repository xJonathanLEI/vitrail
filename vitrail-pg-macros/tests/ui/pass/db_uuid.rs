pub use vitrail_pg_core::*;
pub use vitrail_pg_macros::{
    InsertInput, InsertResult, QueryResult, QueryVariables, UpdateData, schema,
};
extern crate self as vitrail_pg;

use vitrail_pg::uuid::Uuid;

schema! {
    name db_uuid_schema

    model organization {
        id                   Int     @id @default(autoincrement())
        external_id          String  @unique @db.Uuid
        previous_external_id String? @db.Uuid
        name                 String
    }
}

#[derive(InsertInput)]
#[vitrail(schema = crate::db_uuid_schema::Schema, model = organization)]
struct NewOrganization {
    external_id: Uuid,
    previous_external_id: Option<Uuid>,
    name: String,
}

#[derive(InsertResult)]
#[vitrail(
    schema = crate::db_uuid_schema::Schema,
    model = organization,
    input = NewOrganization
)]
struct InsertedOrganization {
    id: i64,
    external_id: Uuid,
    previous_external_id: Option<Uuid>,
    name: String,
}

#[derive(UpdateData)]
#[vitrail(schema = crate::db_uuid_schema::Schema, model = organization)]
struct UpdateOrganization {
    external_id: Uuid,
    previous_external_id: Option<Uuid>,
}

#[derive(QueryVariables)]
struct OrganizationVariables {
    external_id: Uuid,
}

#[derive(QueryResult)]
#[vitrail(
    schema = crate::db_uuid_schema::Schema,
    model = organization,
    variables = OrganizationVariables,
    where(external_id = eq(external_id))
)]
struct OrganizationSummary {
    id: i64,
    external_id: Uuid,
    previous_external_id: Option<Uuid>,
    name: String,
}

fn main() {
    let external_id =
        Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").expect("UUID should parse");
    let previous_external_id =
        Uuid::parse_str("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb").expect("UUID should parse");

    let _ = crate::db_uuid_schema::insert::<InsertedOrganization>(NewOrganization {
        external_id,
        previous_external_id: Some(previous_external_id),
        name: "Acme".to_owned(),
    });

    let _ = UpdateOrganization {
        external_id,
        previous_external_id: None,
    };

    let _ = crate::db_uuid_schema::query_with_variables::<OrganizationSummary>(
        OrganizationVariables { external_id },
    );
}
