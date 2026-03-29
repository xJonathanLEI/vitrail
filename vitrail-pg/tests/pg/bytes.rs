use crate::support::{TestDatabase, apply_schema};
use sqlx::postgres::PgPoolOptions;
use vitrail_pg::{
    PostgresSchema, QueryResult, QueryVariables, UpdateData, UpdateMany, VitrailClient, insert,
    schema,
};

schema! {
    name bytes_schema

    model file {
        id       Int         @id @default(autoincrement())
        name     String      @unique
        data     Bytes
        checksum Bytes?
        chunks   file_chunk[]
    }

    model file_chunk {
        id       Int    @id @default(autoincrement())
        position Int
        payload  Bytes
        file_id  Int
        file     file   @relation(fields: [file_id], references: [id])

        @@unique([file_id, position])
    }
}

pub(crate) use self::bytes_schema as pg_bytes_schema;

#[derive(QueryVariables)]
struct FileByDataVariables {
    data: Vec<u8>,
}

#[derive(QueryResult)]
#[vitrail(schema = crate::bytes_schema::Schema, model = file_chunk)]
struct FileChunkSummary {
    id: i64,
    position: i64,
    payload: Vec<u8>,
}

#[derive(QueryResult)]
#[vitrail(
    schema = crate::bytes_schema::Schema,
    model = file,
    variables = FileByDataVariables,
    where(data = eq(data))
)]
struct FileByData {
    id: i64,
    name: String,
    data: Vec<u8>,
    checksum: Option<Vec<u8>>,
    #[vitrail(include)]
    chunks: Vec<FileChunkSummary>,
}

#[derive(UpdateData)]
#[vitrail(schema = crate::bytes_schema::Schema, model = file)]
struct UpdateFileBytesData {
    data: Vec<u8>,
    checksum: Option<Vec<u8>>,
}

#[derive(UpdateMany)]
#[vitrail(
    schema = crate::bytes_schema::Schema,
    model = file,
    data = UpdateFileBytesData,
    variables = FileByDataVariables,
    where(data = eq(data))
)]
struct UpdateFileBytesByData;

#[test]
fn bytes_columns_generate_bytea_migration_sql() {
    let sql = PostgresSchema::from_schema_access::<crate::bytes_schema::Schema>()
        .migrate_from(&PostgresSchema::empty())
        .to_sql();

    assert!(sql.contains(r#""data" BYTEA NOT NULL"#));
    assert!(sql.contains(r#""checksum" BYTEA"#));
    assert!(sql.contains(r#""payload" BYTEA NOT NULL"#));
}

#[tokio::test]
async fn bytes_columns_work_across_insert_query_update_and_include() {
    let database = TestDatabase::new().await;
    let database_url = database.url().to_owned();

    apply_schema(
        &database_url,
        &PostgresSchema::from_schema_access::<crate::bytes_schema::Schema>(),
    )
    .await;

    let client = VitrailClient::new(&database_url)
        .await
        .expect("should create vitrail client");

    let original_data = vec![0x00, 0x01, 0x7f, 0x80, 0xfe, 0xff];
    let original_checksum = Some(vec![0xaa, 0xbb, 0xcc, 0xdd]);

    let inserted_file = client
        .insert(insert! {
            crate::bytes_schema,
            file {
                data: {
                    name: "avatar.png".to_owned(),
                    data: original_data.clone(),
                    checksum: original_checksum.clone(),
                },
                select: {
                    id: true,
                    name: true,
                    data: true,
                    checksum: true,
                },
            }
        })
        .await
        .expect("bytes insert should succeed");

    assert!(inserted_file.id > 0);
    assert_eq!(inserted_file.name, "avatar.png");
    assert_eq!(inserted_file.data, original_data);
    assert_eq!(inserted_file.checksum, original_checksum);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("should connect to postgres");

    let chunk_payload = vec![0x10, 0x20, 0x30, 0x40];
    let chunk_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO "file_chunk" ("position", "payload", "file_id")
        VALUES ($1, $2, $3)
        RETURNING "id"::bigint
        "#,
    )
    .bind(0_i64)
    .bind(chunk_payload.clone())
    .bind(inserted_file.id)
    .fetch_one(&pool)
    .await
    .expect("should insert file chunk");

    let files = client
        .find_many(crate::bytes_schema::query_with_variables::<FileByData>(
            FileByDataVariables {
                data: original_data.clone(),
            },
        ))
        .await
        .expect("bytes query should succeed");

    assert_eq!(files.len(), 1);
    assert_eq!(files[0].id, inserted_file.id);
    assert_eq!(files[0].name, "avatar.png");
    assert_eq!(files[0].data, original_data);
    assert_eq!(files[0].checksum, original_checksum);
    assert_eq!(files[0].chunks.len(), 1);
    assert_eq!(files[0].chunks[0].id, chunk_id);
    assert_eq!(files[0].chunks[0].position, 0);
    assert_eq!(files[0].chunks[0].payload, chunk_payload);

    let updated_data = vec![0xde, 0xad, 0xbe, 0xef];
    let updated_checksum = None;

    let updated_count = client
        .update_many(crate::bytes_schema::update_many_with_variables::<
            UpdateFileBytesByData,
        >(
            FileByDataVariables {
                data: original_data.clone(),
            },
            UpdateFileBytesData {
                data: updated_data.clone(),
                checksum: updated_checksum.clone(),
            },
        ))
        .await
        .expect("bytes update should succeed");

    assert_eq!(updated_count, 1);

    let stored = sqlx::query_as::<_, (String, Vec<u8>, Option<Vec<u8>>, Vec<u8>)>(
        r#"
        SELECT f."name", f."data", f."checksum", c."payload"
        FROM "file" AS f
        JOIN "file_chunk" AS c ON c."file_id" = f."id"
        WHERE f."id" = $1
        "#,
    )
    .bind(inserted_file.id)
    .fetch_one(&pool)
    .await
    .expect("should read stored bytes directly from postgres");

    assert_eq!(stored.0, "avatar.png");
    assert_eq!(stored.1, updated_data);
    assert_eq!(stored.2, updated_checksum);
    assert_eq!(stored.3, chunk_payload);

    let updated_files = client
        .find_many(crate::bytes_schema::query_with_variables::<FileByData>(
            FileByDataVariables {
                data: updated_data.clone(),
            },
        ))
        .await
        .expect("updated bytes query should succeed");

    assert_eq!(updated_files.len(), 1);
    assert_eq!(updated_files[0].id, inserted_file.id);
    assert_eq!(updated_files[0].name, "avatar.png");
    assert_eq!(updated_files[0].data, updated_data);
    assert_eq!(updated_files[0].checksum, updated_checksum);
    assert_eq!(updated_files[0].chunks.len(), 1);
    assert_eq!(updated_files[0].chunks[0].payload, chunk_payload);

    pool.close().await;
    database.cleanup().await;
}
