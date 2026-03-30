use crate::support::{TestDatabase, apply_schema};
use vitrail_pg::{
    PostgresSchema, QueryResult, QueryVariables, UpdateData, UpdateMany, VitrailClient, insert,
    query, schema, uuid::Uuid,
};

schema! {
    name uuid_schema

    model organization {
        id          Int      @id @default(autoincrement())
        external_id String   @unique @db.Uuid
        name        String
        api_keys    api_key[]
    }

    model api_key {
        id              Int          @id @default(autoincrement())
        key_id          String       @unique @db.Uuid
        previous_key_id String?      @db.Uuid
        label           String
        organization_id Int
        organization    organization @relation(fields: [organization_id], references: [id])
    }
}

pub(crate) use self::uuid_schema as pg_uuid_schema;

#[derive(QueryVariables)]
struct ApiKeyByIdVariables {
    key_id: Uuid,
}

#[allow(dead_code)]
#[derive(QueryResult)]
#[vitrail(
    schema = crate::uuid_schema::Schema,
    model = api_key,
    variables = ApiKeyByIdVariables,
    where(key_id = eq(key_id))
)]
struct ApiKeySummary {
    id: i64,
    key_id: Uuid,
    previous_key_id: Option<Uuid>,
    label: String,
}

#[derive(UpdateData)]
#[vitrail(schema = crate::uuid_schema::Schema, model = api_key)]
struct RotateApiKeyData {
    key_id: Uuid,
    previous_key_id: Option<Uuid>,
}

#[derive(UpdateMany)]
#[vitrail(
    schema = crate::uuid_schema::Schema,
    model = api_key,
    data = RotateApiKeyData,
    variables = ApiKeyByIdVariables,
    where(key_id = eq(key_id))
)]
struct RotateApiKeyById;

async fn setup_database(database_url: &str) {
    apply_schema(
        database_url,
        &PostgresSchema::from_schema_access::<crate::uuid_schema::Schema>(),
    )
    .await;
}

#[tokio::test]
async fn uuid_fields_round_trip_as_native_uuid_values() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();

    setup_database(&database_url).await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let organization_external_id =
        Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").expect("UUID should parse");
    let initial_key_id =
        Uuid::parse_str("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb").expect("UUID should parse");
    let rotated_key_id =
        Uuid::parse_str("cccccccc-cccc-4ccc-8ccc-cccccccccccc").expect("UUID should parse");

    let organization = client
        .insert(insert! {
            crate::uuid_schema,
            organization {
                data: {
                    external_id: organization_external_id,
                    name: "Acme".to_owned(),
                },
                select: {
                    id: true,
                    external_id: true,
                    name: true,
                },
            }
        })
        .await
        .expect("organization insert should succeed");

    assert!(organization.id > 0);
    assert_eq!(organization.external_id, organization_external_id);
    assert_eq!(organization.name, "Acme");

    let inserted_api_key = client
        .insert(insert! {
            crate::uuid_schema,
            api_key {
                data: {
                    key_id: initial_key_id,
                    previous_key_id: None::<Uuid>,
                    label: "Primary".to_owned(),
                    organization_id: organization.id,
                },
                select: {
                    id: true,
                    key_id: true,
                    previous_key_id: true,
                    label: true,
                },
            }
        })
        .await
        .expect("api key insert should succeed");

    assert!(inserted_api_key.id > 0);
    assert_eq!(inserted_api_key.key_id, initial_key_id);
    assert_eq!(inserted_api_key.previous_key_id, None);
    assert_eq!(inserted_api_key.label, "Primary");

    let before_rotation = client
        .find_many(crate::uuid_schema::query_with_variables::<ApiKeySummary>(
            ApiKeyByIdVariables {
                key_id: initial_key_id,
            },
        ))
        .await
        .expect("query by UUID should succeed");

    assert_eq!(before_rotation.len(), 1);
    assert_eq!(before_rotation[0].key_id, initial_key_id);
    assert_eq!(before_rotation[0].previous_key_id, None);
    assert_eq!(before_rotation[0].label, "Primary");

    let updated_count = client
        .update_many(crate::uuid_schema::update_many_with_variables::<
            RotateApiKeyById,
        >(
            ApiKeyByIdVariables {
                key_id: initial_key_id,
            },
            RotateApiKeyData {
                key_id: rotated_key_id,
                previous_key_id: Some(initial_key_id),
            },
        ))
        .await
        .expect("UUID update should succeed");

    assert_eq!(updated_count, 1);

    let after_rotation = client
        .find_many(crate::uuid_schema::query_with_variables::<ApiKeySummary>(
            ApiKeyByIdVariables {
                key_id: rotated_key_id,
            },
        ))
        .await
        .expect("query after UUID update should succeed");

    assert_eq!(after_rotation.len(), 1);
    assert_eq!(after_rotation[0].key_id, rotated_key_id);
    assert_eq!(after_rotation[0].previous_key_id, Some(initial_key_id));
    assert_eq!(after_rotation[0].label, "Primary");

    let organizations = client
        .find_many(query! {
            crate::uuid_schema,
            organization {
                select: {
                    id: true,
                    external_id: true,
                    name: true,
                },
                include: {
                    api_keys: {
                        select: {
                            id: true,
                            key_id: true,
                            previous_key_id: true,
                            label: true,
                        },
                    },
                },
                where: {
                    external_id: {
                        eq: organization_external_id
                    },
                },
            }
        })
        .await
        .expect("query with UUID include should succeed");

    assert_eq!(organizations.len(), 1);
    assert_eq!(organizations[0].external_id, organization_external_id);
    assert_eq!(organizations[0].name, "Acme");
    assert_eq!(organizations[0].api_keys.len(), 1);
    assert_eq!(organizations[0].api_keys[0].key_id, rotated_key_id);
    assert_eq!(
        organizations[0].api_keys[0].previous_key_id,
        Some(initial_key_id)
    );
    assert_eq!(organizations[0].api_keys[0].label, "Primary");

    client.close().await;
    database.cleanup().await;
}
