use serde_json::{Value as JsonValue, json};
#[cfg(feature = "integration-test")]
use vitrail_d1::{
    DeleteMany, InsertInput, InsertResult, QueryVariables, SessionConstraint, UpdateData,
    UpdateMany,
};
use vitrail_d1::{Error as VitrailError, QueryResult, StringValueType, VitrailClient, schema};
use worker::{
    Context, Env, Error as WorkerError, Request, Response, Result as WorkerResult, event,
};

schema! {
    name d1_example_schema

    model scalar_record {
        id         Int      @id @default(autoincrement())
        min_value  Int
        max_value  BigInt
        active     Boolean
        score      Float
        label      String   @rust_ty(crate::RecordLabel) @unique
        payload    Bytes
        created_at DateTime
        metadata   Json
        note       String?
    }

    model author {
        id    Int    @id @default(autoincrement())
        name  String
        posts post[]
    }

    model post {
        id        Int    @id @default(autoincrement())
        title     String
        author_id Int
        author    author @relation(fields: [author_id], references: [id])

        @@index([author_id])
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RecordLabel(String);

impl RecordLabel {
    #[cfg(feature = "integration-test")]
    fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

impl StringValueType for RecordLabel {
    fn from_db_string(value: String) -> Result<Self, VitrailError> {
        Ok(Self(value))
    }

    fn into_db_string(self) -> String {
        self.0
    }
}

#[derive(Clone, Debug, QueryResult)]
#[vitrail(schema = crate::d1_example_schema::Schema, model = scalar_record)]
struct ScalarRecord {
    id: i64,
    min_value: i64,
    max_value: i64,
    active: bool,
    score: f64,
    label: RecordLabel,
    payload: Vec<u8>,
    created_at: chrono::DateTime<chrono::Utc>,
    metadata: JsonValue,
    note: Option<String>,
}

#[cfg(feature = "integration-test")]
#[derive(Clone, Debug, QueryVariables)]
struct RecordByMaxVariables {
    max_value: i64,
}

#[cfg(feature = "integration-test")]
#[derive(Clone, Debug, QueryResult)]
#[vitrail(
    schema = crate::d1_example_schema::Schema,
    model = scalar_record,
    variables = RecordByMaxVariables,
    where(max_value = eq(max_value))
)]
struct RecordByMax {
    id: i64,
    min_value: i64,
    max_value: i64,
    active: bool,
    score: f64,
    label: RecordLabel,
    payload: Vec<u8>,
    created_at: chrono::DateTime<chrono::Utc>,
    metadata: JsonValue,
    note: Option<String>,
}

#[cfg(feature = "integration-test")]
#[derive(Clone, Debug, QueryVariables)]
struct RecordIdsVariables {
    record_ids: Vec<i64>,
}

#[cfg(feature = "integration-test")]
#[derive(Clone, Debug, QueryResult)]
#[vitrail(
    schema = crate::d1_example_schema::Schema,
    model = scalar_record,
    variables = RecordIdsVariables,
    where(id = in(record_ids))
)]
struct RecordIdOnly {
    id: i64,
}

#[cfg(feature = "integration-test")]
#[derive(Clone, Debug, InsertInput)]
#[vitrail(schema = crate::d1_example_schema::Schema, model = scalar_record)]
struct NewScalarRecord {
    min_value: i64,
    max_value: i64,
    active: bool,
    score: f64,
    label: RecordLabel,
    payload: Vec<u8>,
    created_at: chrono::DateTime<chrono::Utc>,
    metadata: JsonValue,
    note: Option<String>,
}

#[cfg(feature = "integration-test")]
#[derive(Clone, Debug, InsertResult)]
#[vitrail(
    schema = crate::d1_example_schema::Schema,
    model = scalar_record,
    input = NewScalarRecord
)]
struct InsertedScalarRecord {
    id: i64,
    min_value: i64,
    max_value: i64,
    active: bool,
    score: f64,
    label: RecordLabel,
    payload: Vec<u8>,
    created_at: chrono::DateTime<chrono::Utc>,
    metadata: JsonValue,
    note: Option<String>,
}

#[cfg(feature = "integration-test")]
#[derive(Clone, Debug, UpdateData)]
#[vitrail(schema = crate::d1_example_schema::Schema, model = scalar_record)]
struct UpdateScalarRecord {
    active: bool,
    note: Option<String>,
}

#[cfg(feature = "integration-test")]
#[derive(Clone, Debug, QueryVariables)]
struct RecordIdVariables {
    record_id: i64,
}

#[cfg(feature = "integration-test")]
#[derive(Clone, Debug, UpdateMany)]
#[vitrail(
    schema = crate::d1_example_schema::Schema,
    model = scalar_record,
    data = UpdateScalarRecord,
    variables = RecordIdVariables,
    where(id = eq(record_id))
)]
struct UpdateScalarRecordById;

#[cfg(feature = "integration-test")]
#[derive(Clone, Debug, UpdateMany)]
#[vitrail(
    schema = crate::d1_example_schema::Schema,
    model = scalar_record,
    data = UpdateScalarRecord,
    variables = RecordByMaxVariables,
    where(max_value = eq(max_value))
)]
struct UpdateScalarRecordByMax;

#[cfg(feature = "integration-test")]
#[derive(Clone, Debug, DeleteMany)]
#[vitrail(
    schema = crate::d1_example_schema::Schema,
    model = scalar_record,
    variables = RecordIdVariables,
    where(id = eq(record_id))
)]
struct DeleteScalarRecordById;

#[cfg(feature = "integration-test")]
#[derive(Clone, Debug, DeleteMany)]
#[vitrail(
    schema = crate::d1_example_schema::Schema,
    model = scalar_record,
    variables = RecordByMaxVariables,
    where(max_value = eq(max_value))
)]
struct DeleteScalarRecordByMax;

#[event(fetch)]
pub async fn fetch(request: Request, env: Env, _context: Context) -> WorkerResult<Response> {
    let path = request.url()?.path().to_owned();

    match path.as_str() {
        "/" => Response::ok("Vitrail D1 example. Apply the schema migration, then GET /records."),
        "/records" => list_records(&env).await,
        #[cfg(feature = "integration-test")]
        "/__test/setup" => setup_test_schema(&env).await,
        #[cfg(feature = "integration-test")]
        "/__test/crud" => run_crud_probe(&env).await,
        #[cfg(feature = "integration-test")]
        "/__test/sessions" => run_session_probe(&env).await,
        #[cfg(feature = "integration-test")]
        "/__test/atomic-batches" => run_atomic_batch_probe(&env).await,
        #[cfg(feature = "integration-test")]
        "/__test/atomic-batch-rollback" => run_atomic_batch_rollback_probe(&env).await,
        #[cfg(feature = "integration-test")]
        "/__test/atomic-batch-decode-error" => run_atomic_batch_decode_error_probe(&env).await,
        _ => Response::error("Not found", 404),
    }
}

async fn list_records(env: &Env) -> WorkerResult<Response> {
    let client = VitrailClient::new(env.d1("DB")?);
    let records = client
        .find_many(d1_example_schema::query::<ScalarRecord>())
        .await
        .map_err(worker_error)?;

    let response = records.into_iter().map(record_json).collect::<Vec<_>>();

    Response::from_json(&response)
}

fn record_json(record: ScalarRecord) -> JsonValue {
    json!({
        "id": record.id.to_string(),
        "minValue": record.min_value.to_string(),
        "maxValue": record.max_value.to_string(),
        "active": record.active,
        "score": record.score,
        "label": record.label.as_str(),
        "payload": record.payload,
        "createdAt": record.created_at.to_rfc3339(),
        "metadata": record.metadata,
        "note": record.note,
    })
}

#[cfg(feature = "integration-test")]
fn d1_binding_setup_sql(migration: &str) -> String {
    let mut statements = Vec::new();
    let mut statement = String::new();

    for line in migration.lines() {
        let line = line.trim();

        if line.is_empty() || line.starts_with("--") {
            continue;
        }

        if !statement.is_empty() {
            statement.push(' ');
        }

        statement.push_str(line);

        if line.ends_with(';') {
            statements.push(std::mem::take(&mut statement));
        }
    }

    if !statement.is_empty() {
        statements.push(statement);
    }

    statements.join("\n")
}

#[cfg(feature = "integration-test")]
async fn setup_test_schema(env: &Env) -> WorkerResult<Response> {
    const INITIAL_MIGRATION: &str =
        include_str!("../migrations/20260701000000_initial_schema/migration.sql");
    const REQUIRE_POST_TITLE_MIGRATION: &str =
        include_str!("../migrations/20260701000001_require_post_title/migration.sql");

    let database = env.d1("DB")?;

    database
        .exec(
            r#"DROP TABLE IF EXISTS "post";
DROP TABLE IF EXISTS "author";
DROP TABLE IF EXISTS "scalar_record";"#,
        )
        .await?;
    database
        .exec(&d1_binding_setup_sql(INITIAL_MIGRATION))
        .await?;
    database
        .exec(&d1_binding_setup_sql(REQUIRE_POST_TITLE_MIGRATION))
        .await?;

    Response::from_json(&json!({ "ok": true }))
}

#[cfg(feature = "integration-test")]
async fn run_crud_probe(env: &Env) -> WorkerResult<Response> {
    let client = VitrailClient::new(env.d1("DB")?);
    let created_at = chrono::DateTime::parse_from_rfc3339("2026-07-14T12:34:56.123456Z")
        .map_err(|error| WorkerError::RustError(error.to_string()))?
        .with_timezone(&chrono::Utc);
    let payload = vec![0, 1, 2, 127, 128, 254, 255];
    let metadata = json!({
        "kind": "d1-probe",
        "nested": {
            "enabled": true,
            "count": 7,
        },
    });

    let inserted = client
        .insert(d1_example_schema::insert::<InsertedScalarRecord>(
            NewScalarRecord {
                min_value: i64::MIN,
                max_value: i64::MAX,
                active: true,
                score: 1234.5,
                label: RecordLabel::new("edge-values"),
                payload: payload.clone(),
                created_at,
                metadata: metadata.clone(),
                note: None,
            },
        ))
        .await
        .map_err(worker_error)?;

    ensure(
        inserted.id > 0,
        "insert did not return an autoincremented ID",
    )?;
    ensure(
        inserted.min_value == i64::MIN,
        "insert returning changed i64::MIN",
    )?;
    ensure(
        inserted.max_value == i64::MAX,
        "insert returning changed i64::MAX",
    )?;
    ensure(
        inserted.active,
        "insert returning changed the boolean value",
    )?;
    ensure(
        inserted.score == 1234.5,
        "insert returning changed the floating-point value",
    )?;
    ensure(
        inserted.label.as_str() == "edge-values",
        "insert returning changed the custom string value",
    )?;
    ensure(
        inserted.payload == payload,
        "insert returning changed the BLOB value",
    )?;
    ensure(
        inserted.created_at == created_at,
        "insert returning changed the datetime value",
    )?;
    ensure(
        inserted.metadata == metadata,
        "insert returning changed the JSON value",
    )?;
    ensure(
        inserted.note.is_none(),
        "insert returning changed the nullable value",
    )?;

    let queried = client
        .find_first(d1_example_schema::query_with_variables::<RecordByMax>(
            RecordByMaxVariables {
                max_value: i64::MAX,
            },
        ))
        .await
        .map_err(worker_error)?;

    ensure(queried.id == inserted.id, "query returned the wrong row")?;
    ensure(queried.min_value == i64::MIN, "root query changed i64::MIN")?;
    ensure(
        queried.max_value == i64::MAX,
        "root query or exact integer filter changed i64::MAX",
    )?;
    ensure(queried.active, "root query changed the boolean value")?;
    ensure(
        queried.score == inserted.score,
        "root query changed the floating-point value",
    )?;
    ensure(
        queried.label == inserted.label,
        "root query changed the custom string value",
    )?;
    ensure(
        queried.payload == inserted.payload,
        "root query changed the BLOB value",
    )?;
    ensure(
        queried.created_at == inserted.created_at,
        "root query changed the datetime value",
    )?;
    ensure(
        queried.metadata == inserted.metadata,
        "root query changed the JSON value",
    )?;
    ensure(
        queried.note.is_none(),
        "root query changed the nullable value",
    )?;

    let updated = client
        .update_many(d1_example_schema::update_many_with_variables::<
            UpdateScalarRecordById,
        >(
            RecordIdVariables {
                record_id: inserted.id,
            },
            UpdateScalarRecord {
                active: false,
                note: Some("updated".to_owned()),
            },
        ))
        .await
        .map_err(worker_error)?;

    ensure(updated == 1, "bulk update did not report one changed row")?;

    let updated_record = client
        .find_first(d1_example_schema::query_with_variables::<RecordByMax>(
            RecordByMaxVariables {
                max_value: i64::MAX,
            },
        ))
        .await
        .map_err(worker_error)?;

    ensure(
        !updated_record.active,
        "bulk update did not persist the boolean value",
    )?;
    ensure(
        updated_record.note.as_deref() == Some("updated"),
        "bulk update did not persist the nullable string",
    )?;

    let deleted = client
        .delete_many(d1_example_schema::delete_many_with_variables::<
            DeleteScalarRecordById,
        >(RecordIdVariables {
            record_id: inserted.id,
        }))
        .await
        .map_err(worker_error)?;

    ensure(deleted == 1, "bulk delete did not report one changed row")?;

    let remaining = client
        .find_optional(d1_example_schema::query::<ScalarRecord>())
        .await
        .map_err(worker_error)?;

    ensure(
        remaining.is_none(),
        "bulk delete left the inserted row in the database",
    )?;

    Response::from_json(&json!({
        "ok": true,
        "inserted": {
            "id": inserted.id.to_string(),
            "minValue": inserted.min_value.to_string(),
            "maxValue": inserted.max_value.to_string(),
            "active": inserted.active,
            "score": inserted.score,
            "label": inserted.label.as_str(),
            "payload": inserted.payload,
            "createdAt": inserted.created_at.to_rfc3339(),
            "metadata": inserted.metadata,
            "note": inserted.note,
        },
        "queried": {
            "id": queried.id.to_string(),
            "minValue": queried.min_value.to_string(),
            "maxValue": queried.max_value.to_string(),
        },
        "updatedCount": updated,
        "deletedCount": deleted,
    }))
}

#[cfg(feature = "integration-test")]
async fn run_session_probe(env: &Env) -> WorkerResult<Response> {
    let client = VitrailClient::new(env.d1("DB")?);
    let created_at = chrono::DateTime::parse_from_rfc3339("2026-07-14T13:45:56.654321Z")
        .map_err(|error| WorkerError::RustError(error.to_string()))?
        .with_timezone(&chrono::Utc);

    let first_primary = client
        .with_session(SessionConstraint::FirstPrimary)
        .map_err(worker_error)?;

    let inserted = first_primary
        .insert(d1_example_schema::insert::<InsertedScalarRecord>(
            NewScalarRecord {
                min_value: i64::MIN,
                max_value: i64::MAX,
                active: true,
                score: 42.5,
                label: RecordLabel::new("session-record"),
                payload: vec![1, 3, 5, 7],
                created_at,
                metadata: json!({
                    "kind": "session-probe",
                    "version": 1,
                }),
                note: None,
            },
        ))
        .await
        .map_err(worker_error)?;

    let initial_bookmark = first_primary
        .latest_bookmark()
        .map_err(worker_error)?
        .ok_or_else(|| {
            WorkerError::RustError(
                "first-primary session did not return a bookmark after insertion".to_owned(),
            )
        })?;

    ensure(
        !initial_bookmark.as_str().is_empty(),
        "first-primary session returned an empty bookmark",
    )?;

    let sequential_read = first_primary
        .find_first(d1_example_schema::query_with_variables::<RecordByMax>(
            RecordByMaxVariables {
                max_value: i64::MAX,
            },
        ))
        .await
        .map_err(worker_error)?;

    ensure(
        sequential_read.id == inserted.id,
        "sequential session read returned the wrong row",
    )?;

    let bookmark_before_update = first_primary
        .latest_bookmark()
        .map_err(worker_error)?
        .ok_or_else(|| {
            WorkerError::RustError(
                "first-primary session lost its bookmark before mutation".to_owned(),
            )
        })?;

    let updated = first_primary
        .update_many(d1_example_schema::update_many_with_variables::<
            UpdateScalarRecordById,
        >(
            RecordIdVariables {
                record_id: inserted.id,
            },
            UpdateScalarRecord {
                active: false,
                note: Some("session-updated".to_owned()),
            },
        ))
        .await
        .map_err(worker_error)?;

    ensure(
        updated == 1,
        "session update did not report one changed row",
    )?;

    let advanced_bookmark = first_primary
        .latest_bookmark()
        .map_err(worker_error)?
        .ok_or_else(|| {
            WorkerError::RustError(
                "first-primary session did not return a bookmark after mutation".to_owned(),
            )
        })?;

    ensure(
        advanced_bookmark != bookmark_before_update,
        "session bookmark did not advance after mutation",
    )?;

    let bookmark_session = client
        .with_session(SessionConstraint::Bookmark(advanced_bookmark.clone()))
        .map_err(worker_error)?;

    let bookmark_read = bookmark_session
        .find_first(d1_example_schema::query_with_variables::<RecordByMax>(
            RecordByMaxVariables {
                max_value: i64::MAX,
            },
        ))
        .await
        .map_err(worker_error)?;

    ensure(
        bookmark_read.id == inserted.id,
        "bookmark-based session returned the wrong row",
    )?;
    ensure(
        !bookmark_read.active,
        "bookmark-based session did not observe the prior mutation",
    )?;
    ensure(
        bookmark_read.note.as_deref() == Some("session-updated"),
        "bookmark-based session did not observe the updated nullable value",
    )?;

    let first_unconstrained = client
        .with_session(SessionConstraint::FirstUnconstrained)
        .map_err(worker_error)?;

    let unconstrained_first = first_unconstrained
        .find_first(d1_example_schema::query_with_variables::<RecordByMax>(
            RecordByMaxVariables {
                max_value: i64::MAX,
            },
        ))
        .await
        .map_err(worker_error)?;

    let unconstrained_second = first_unconstrained
        .find_optional(d1_example_schema::query_with_variables::<RecordByMax>(
            RecordByMaxVariables {
                max_value: i64::MAX,
            },
        ))
        .await
        .map_err(worker_error)?
        .ok_or_else(|| {
            WorkerError::RustError(
                "second sequential first-unconstrained read returned no row".to_owned(),
            )
        })?;

    ensure(
        unconstrained_first.id == inserted.id && unconstrained_second.id == inserted.id,
        "sequential first-unconstrained reads returned inconsistent rows",
    )?;

    let unconstrained_bookmark = first_unconstrained
        .latest_bookmark()
        .map_err(worker_error)?
        .ok_or_else(|| {
            WorkerError::RustError(
                "first-unconstrained session did not return a bookmark after reads".to_owned(),
            )
        })?;

    Response::from_json(&json!({
        "ok": true,
        "insertedId": inserted.id.to_string(),
        "initialBookmark": initial_bookmark.as_str(),
        "advancedBookmark": advanced_bookmark.as_str(),
        "bookmarkReadNote": bookmark_read.note,
        "updatedCount": updated,
        "unconstrainedBookmark": unconstrained_bookmark.as_str(),
        "sequentialReadCount": 2,
    }))
}

#[cfg(feature = "integration-test")]
fn batch_test_record(
    label: &str,
    min_value: i64,
    max_value: i64,
    created_at: chrono::DateTime<chrono::Utc>,
) -> NewScalarRecord {
    NewScalarRecord {
        min_value,
        max_value,
        active: true,
        score: 99.25,
        label: RecordLabel::new(label),
        payload: vec![2, 4, 8, 16, 32, 64, 128, 255],
        created_at,
        metadata: json!({
            "kind": "atomic-batch-probe",
            "label": label,
        }),
        note: None,
    }
}

#[cfg(feature = "integration-test")]
async fn run_atomic_batch_probe(env: &Env) -> WorkerResult<Response> {
    let client = VitrailClient::new(env.d1("DB")?);
    let created_at = chrono::DateTime::parse_from_rfc3339("2026-07-14T14:56:57.123456Z")
        .map_err(|error| WorkerError::RustError(error.to_string()))?
        .with_timezone(&chrono::Utc);

    let empty_results = client
        .atomic_batch()
        .execute()
        .await
        .map_err(worker_error)?;
    ensure(
        empty_results.is_empty(),
        "empty atomic batch returned unexpected result slots",
    )?;

    let mut batch = client.atomic_batch();
    let inserted_handle = batch
        .insert(d1_example_schema::insert::<InsertedScalarRecord>(
            batch_test_record("atomic-batch-record", i64::MIN, i64::MAX, created_at),
        ))
        .map_err(worker_error)?;
    let many_handle = batch
        .find_many(d1_example_schema::query_with_variables::<RecordByMax>(
            RecordByMaxVariables {
                max_value: i64::MAX,
            },
        ))
        .map_err(worker_error)?;
    let optional_handle = batch
        .find_optional(d1_example_schema::query_with_variables::<RecordByMax>(
            RecordByMaxVariables {
                max_value: i64::MAX,
            },
        ))
        .map_err(worker_error)?;
    let first_handle = batch
        .find_first(d1_example_schema::query_with_variables::<RecordByMax>(
            RecordByMaxVariables {
                max_value: i64::MAX,
            },
        ))
        .map_err(worker_error)?;
    let updated_handle = batch
        .update_many(d1_example_schema::update_many_with_variables::<
            UpdateScalarRecordByMax,
        >(
            RecordByMaxVariables {
                max_value: i64::MAX,
            },
            UpdateScalarRecord {
                active: false,
                note: Some("atomic-batch-updated".to_owned()),
            },
        ))
        .map_err(worker_error)?;
    let deleted_handle = batch
        .delete_many(d1_example_schema::delete_many_with_variables::<
            DeleteScalarRecordByMax,
        >(RecordByMaxVariables {
            max_value: i64::MAX,
        }))
        .map_err(worker_error)?;

    ensure(
        batch.len() == 6,
        "heterogeneous atomic batch did not retain every queued operation",
    )?;

    let mut results = batch.execute().await.map_err(worker_error)?;
    ensure(
        results.len() == 6,
        "heterogeneous atomic batch returned the wrong result count",
    )?;

    let inserted = results.take(inserted_handle).map_err(worker_error)?;
    let many = results.take(many_handle).map_err(worker_error)?;
    let optional = results
        .take(optional_handle)
        .map_err(worker_error)?
        .ok_or_else(|| {
            WorkerError::RustError("atomic batch optional query returned no row".to_owned())
        })?;
    let first = results.take(first_handle).map_err(worker_error)?;
    let updated = results.take(updated_handle).map_err(worker_error)?;
    let deleted = results.take(deleted_handle).map_err(worker_error)?;

    ensure(
        inserted.min_value == i64::MIN && inserted.max_value == i64::MAX,
        "atomic batch insert returning changed exact integer values",
    )?;
    ensure(
        many.len() == 1 && many[0].id == inserted.id,
        "atomic batch find-many returned the wrong rows",
    )?;
    ensure(
        optional.id == inserted.id,
        "atomic batch optional query returned the wrong row",
    )?;
    ensure(
        first.id == inserted.id,
        "atomic batch first query returned the wrong row",
    )?;
    ensure(
        updated == 1,
        "atomic batch update did not report one changed row",
    )?;
    ensure(
        deleted == 1,
        "atomic batch delete did not report one changed row",
    )?;

    let remaining = client
        .find_optional(d1_example_schema::query::<ScalarRecord>())
        .await
        .map_err(worker_error)?;
    ensure(
        remaining.is_none(),
        "atomic batch delete left the inserted row in the database",
    )?;

    let session = client
        .with_session(SessionConstraint::FirstPrimary)
        .map_err(worker_error)?;
    let session_max_value = -7_777_777_i64;
    let mut session_batch = session.atomic_batch();
    let session_inserted_handle = session_batch
        .insert(d1_example_schema::insert::<InsertedScalarRecord>(
            batch_test_record(
                "session-atomic-batch-record",
                17,
                session_max_value,
                created_at,
            ),
        ))
        .map_err(worker_error)?;
    let session_queried_handle = session_batch
        .find_first(d1_example_schema::query_with_variables::<RecordByMax>(
            RecordByMaxVariables {
                max_value: session_max_value,
            },
        ))
        .map_err(worker_error)?;

    let mut session_results = session_batch.execute().await.map_err(worker_error)?;
    let session_inserted = session_results
        .take(session_inserted_handle)
        .map_err(worker_error)?;
    let session_queried = session_results
        .take(session_queried_handle)
        .map_err(worker_error)?;
    ensure(
        session_inserted.id == session_queried.id,
        "session atomic batch query did not observe its preceding insert",
    )?;

    let session_bookmark = session
        .latest_bookmark()
        .map_err(worker_error)?
        .ok_or_else(|| {
            WorkerError::RustError("session atomic batch did not produce a bookmark".to_owned())
        })?;
    ensure(
        !session_bookmark.as_str().is_empty(),
        "session atomic batch returned an empty bookmark",
    )?;

    let mut limit_batch = client.atomic_batch();
    let accepted_binding_handle = limit_batch
        .find_many(d1_example_schema::query_with_variables::<RecordIdOnly>(
            RecordIdsVariables {
                record_ids: (0_i64..100_i64).collect(),
            },
        ))
        .map_err(worker_error)?;

    let rejected_binding_error = match limit_batch.find_many(
        d1_example_schema::query_with_variables::<RecordIdOnly>(RecordIdsVariables {
            record_ids: (0_i64..101_i64).collect(),
        }),
    ) {
        Ok(_) => {
            return Err(WorkerError::RustError(
                "atomic batch accepted a statement with 101 bindings".to_owned(),
            ));
        }
        Err(error) => error,
    };

    ensure(
        matches!(&rejected_binding_error, VitrailError::Compile(_)),
        "atomic batch returned the wrong error for a 101-binding statement",
    )?;

    let mut limit_results = limit_batch.execute().await.map_err(worker_error)?;
    let accepted_binding_rows = limit_results
        .take(accepted_binding_handle)
        .map_err(worker_error)?;
    let accepted_binding_id_sum = accepted_binding_rows
        .iter()
        .map(|record| record.id)
        .sum::<i64>();

    Response::from_json(&json!({
        "ok": true,
        "emptyBatch": true,
        "insertedId": inserted.id.to_string(),
        "manyCount": many.len(),
        "optionalId": optional.id.to_string(),
        "firstId": first.id.to_string(),
        "updatedCount": updated,
        "deletedCount": deleted,
        "sessionInsertedId": session_inserted.id.to_string(),
        "sessionBookmark": session_bookmark.as_str(),
        "acceptedBindingCount": 100,
        "acceptedBindingRows": accepted_binding_rows.len(),
        "acceptedBindingIdSum": accepted_binding_id_sum.to_string(),
        "rejectedBindingError": rejected_binding_error.to_string(),
    }))
}

#[cfg(feature = "integration-test")]
async fn run_atomic_batch_rollback_probe(env: &Env) -> WorkerResult<Response> {
    let client = VitrailClient::new(env.d1("DB")?);
    let created_at = chrono::DateTime::parse_from_rfc3339("2026-07-14T15:01:02.000000Z")
        .map_err(|error| WorkerError::RustError(error.to_string()))?
        .with_timezone(&chrono::Utc);

    let mut invalid_record = batch_test_record("binding-failure", 10, 201, created_at);
    invalid_record.score = f64::NAN;

    let mut binding_failure_batch = client.atomic_batch();
    binding_failure_batch
        .insert(d1_example_schema::insert::<InsertedScalarRecord>(
            batch_test_record("binding-before-failure", 9, 200, created_at),
        ))
        .map_err(worker_error)?;
    binding_failure_batch
        .insert(d1_example_schema::insert::<InsertedScalarRecord>(
            invalid_record,
        ))
        .map_err(worker_error)?;

    let binding_error = match binding_failure_batch.execute().await {
        Ok(_) => {
            return Err(WorkerError::RustError(
                "atomic batch with an invalid local binding unexpectedly succeeded".to_owned(),
            ));
        }
        Err(error) => error,
    };

    ensure(
        matches!(&binding_error, VitrailError::Binding(_)),
        "atomic batch returned the wrong error for an invalid local binding",
    )?;

    let rows_after_binding_failure = client
        .find_many(d1_example_schema::query::<ScalarRecord>())
        .await
        .map_err(worker_error)?;
    ensure(
        rows_after_binding_failure.is_empty(),
        "local atomic batch binding failure submitted an earlier statement",
    )?;

    let mut batch = client.atomic_batch();
    batch
        .insert(d1_example_schema::insert::<InsertedScalarRecord>(
            batch_test_record("rollback-duplicate", 1, 101, created_at),
        ))
        .map_err(worker_error)?;
    batch
        .insert(d1_example_schema::insert::<InsertedScalarRecord>(
            batch_test_record("rollback-duplicate", 2, 102, created_at),
        ))
        .map_err(worker_error)?;
    batch
        .insert(d1_example_schema::insert::<InsertedScalarRecord>(
            batch_test_record("rollback-after-failure", 3, 103, created_at),
        ))
        .map_err(worker_error)?;

    let execution_error = match batch.execute().await {
        Ok(_) => {
            return Err(WorkerError::RustError(
                "constraint-violating atomic batch unexpectedly succeeded".to_owned(),
            ));
        }
        Err(error) => error,
    };

    let remaining = client
        .find_many(d1_example_schema::query::<ScalarRecord>())
        .await
        .map_err(worker_error)?;
    ensure(
        remaining.is_empty(),
        "failed atomic batch left partial writes in the database",
    )?;

    Response::from_json(&json!({
        "ok": true,
        "bindingError": binding_error.to_string(),
        "error": execution_error.to_string(),
        "remainingRows": remaining.len(),
    }))
}

#[cfg(feature = "integration-test")]
async fn run_atomic_batch_decode_error_probe(env: &Env) -> WorkerResult<Response> {
    let database = env.d1("DB")?;

    database
        .exec(
            r#"INSERT INTO "scalar_record" ("min_value", "max_value", "active", "score", "label", "payload", "created_at", "metadata", "note") VALUES ('not-an-integer', 1, 1, 1.0, 'malformed-batch-row', X'00', '2026-07-14T15:02:03.000000Z', json('{}'), NULL);"#,
        )
        .await?;

    let client = VitrailClient::new(database);
    let mut batch = client.atomic_batch();
    batch
        .find_many(d1_example_schema::query::<ScalarRecord>())
        .map_err(worker_error)?;

    let decode_error = match batch.execute().await {
        Ok(_) => {
            return Err(WorkerError::RustError(
                "atomic batch decoded a malformed integer row".to_owned(),
            ));
        }
        Err(error) => error,
    };

    ensure(
        matches!(&decode_error, VitrailError::Decode(_)),
        "malformed atomic batch row returned a non-decoding error",
    )?;

    Response::from_json(&json!({
        "ok": true,
        "error": decode_error.to_string(),
    }))
}

#[cfg(feature = "integration-test")]
fn ensure(condition: bool, message: &str) -> WorkerResult<()> {
    if condition {
        Ok(())
    } else {
        Err(WorkerError::RustError(message.to_owned()))
    }
}

fn worker_error(error: VitrailError) -> WorkerError {
    WorkerError::RustError(error.to_string())
}
