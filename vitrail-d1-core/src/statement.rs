use std::sync::Arc;

use chrono::SecondsFormat;
use vitrail_sqlite_dialect::{BindingValue, CompiledStatement, OperationKind};
use worker::d1::{D1PreparedStatement, D1Result};
use worker::js_sys::Uint8Array;
use worker::wasm_bindgen::JsValue;

use crate::row::D1RowMetadata;
use crate::{D1Executor, D1Row, Error};

pub(crate) async fn execute_rows(
    executor: &dyn D1Executor,
    statement: &CompiledStatement,
) -> Result<Vec<D1Row>, Error> {
    let metadata = Arc::new(D1RowMetadata::new(statement.result_columns())?);
    let prepared = prepare_statement(executor, statement)?;
    let raw_rows = prepared.raw_js_value().await?;
    let mut rows = Vec::with_capacity(raw_rows.len());

    for raw_row in raw_rows {
        rows.push(D1Row::from_raw(raw_row, Arc::clone(&metadata))?);
    }

    Ok(rows)
}

pub(crate) async fn execute_changes(
    executor: &dyn D1Executor,
    statement: &CompiledStatement,
) -> Result<u64, Error> {
    let prepared = prepare_statement(executor, statement)?;
    let result = prepared.run().await?;
    changes_from_result(&result, statement)
}

pub(crate) fn changes_from_result(
    result: &D1Result,
    statement: &CompiledStatement,
) -> Result<u64, Error> {
    let metadata = result.meta()?.ok_or(Error::MissingWriteMetadata {
        operation: operation_name(statement.operation()),
    })?;
    let changes = metadata.changes.ok_or(Error::MissingWriteMetadata {
        operation: operation_name(statement.operation()),
    })?;

    u64::try_from(changes).map_err(|_| {
        Error::decode(format!(
            "D1 returned an affected-row count that does not fit in `u64` for {}",
            operation_name(statement.operation()),
        ))
    })
}

pub(crate) fn prepare_statement(
    executor: &dyn D1Executor,
    statement: &CompiledStatement,
) -> Result<D1PreparedStatement, Error> {
    let prepared = executor.prepare(statement.sql());

    if statement.bindings().is_empty() {
        return Ok(prepared);
    }

    let bindings = statement
        .bindings()
        .iter()
        .map(binding_to_js)
        .collect::<Result<Vec<_>, _>>()?;

    prepared.bind(&bindings).map_err(Error::from)
}

fn binding_to_js(binding: &BindingValue) -> Result<JsValue, Error> {
    match binding {
        BindingValue::Null => Ok(JsValue::null()),
        BindingValue::Int(value) => Ok(JsValue::from_str(&value.to_string())),
        BindingValue::String(value) => Ok(JsValue::from_str(value)),
        BindingValue::Bool(value) => Ok(JsValue::from_bool(*value)),
        BindingValue::Float(value) => {
            if !value.is_finite() {
                return Err(Error::binding("D1 floating-point bindings must be finite"));
            }

            Ok(JsValue::from_f64(*value))
        }
        BindingValue::Bytes(value) => {
            let bytes = Uint8Array::from(value.as_slice());
            Ok(bytes.buffer().into())
        }
        BindingValue::DateTime(value) => Ok(JsValue::from_str(
            &value.to_rfc3339_opts(SecondsFormat::Micros, true),
        )),
        BindingValue::Json(value) => {
            let serialized = serde_json::to_string(value).map_err(|error| {
                Error::decode_with_source("failed to serialize JSON for a D1 binding", error)
            })?;
            Ok(JsValue::from_str(&serialized))
        }
    }
}

fn operation_name(operation: OperationKind) -> &'static str {
    match operation {
        OperationKind::Query => "query operation",
        OperationKind::Insert => "insert operation",
        OperationKind::UpdateMany => "update-many operation",
        OperationKind::DeleteMany => "delete-many operation",
    }
}
