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
        label      String   @rust_ty(crate::RecordLabel)
        payload    Bytes
        created_at DateTime
        metadata   Json
        note       String?
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
#[derive(Clone, Debug, DeleteMany)]
#[vitrail(
    schema = crate::d1_example_schema::Schema,
    model = scalar_record,
    variables = RecordIdVariables,
    where(id = eq(record_id))
)]
struct DeleteScalarRecordById;

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
async fn setup_test_schema(env: &Env) -> WorkerResult<Response> {
    let database = env.d1("DB")?;

    database
        .exec(
            r#"DROP TABLE IF EXISTS "scalar_record";
CREATE TABLE "scalar_record" ("id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "min_value" INTEGER NOT NULL, "max_value" BIGINT NOT NULL, "active" BOOLEAN NOT NULL, "score" REAL NOT NULL, "label" TEXT NOT NULL, "payload" BLOB NOT NULL, "created_at" DATETIME NOT NULL, "metadata" JSONB NOT NULL, "note" TEXT);"#,
        )
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
