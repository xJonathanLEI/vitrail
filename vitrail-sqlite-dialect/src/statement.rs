use serde_json::Value as JsonValue;

use crate::flavor::SqliteFamilyFlavor;
use crate::{CompileError, ScalarType};

/// A normalized value bound to a compiled SQLite-family statement.
#[derive(Clone, Debug, PartialEq)]
pub enum BindingValue {
    Null,
    /// A signed 64-bit integer.
    ///
    /// D1 executors bind this value as decimal text; D1-flavored SQL casts the
    /// corresponding parameter to `INTEGER` to preserve the full `i64` range.
    Int(i64),
    String(String),
    Bool(bool),
    Float(f64),
    Bytes(Vec<u8>),
    DateTime(chrono::DateTime<chrono::Utc>),
    Json(JsonValue),
}

/// Identifies the operation represented by a compiled statement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationKind {
    Query,
    Insert,
    UpdateMany,
    DeleteMany,
}

/// Describes how a result column is represented.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultColumnKind {
    Scalar(ScalarType),
    Relation { many: bool },
}

/// Metadata for an ordered root result column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResultColumn {
    alias: String,
    kind: ResultColumnKind,
    nullable: bool,
}

impl ResultColumn {
    pub(crate) fn scalar(alias: impl Into<String>, scalar: ScalarType, nullable: bool) -> Self {
        Self {
            alias: alias.into(),
            kind: ResultColumnKind::Scalar(scalar),
            nullable,
        }
    }

    pub(crate) fn relation(alias: impl Into<String>, many: bool, nullable: bool) -> Self {
        Self {
            alias: alias.into(),
            kind: ResultColumnKind::Relation { many },
            nullable,
        }
    }

    pub fn alias(&self) -> &str {
        &self.alias
    }

    pub fn kind(&self) -> ResultColumnKind {
        self.kind
    }

    pub fn nullable(&self) -> bool {
        self.nullable
    }
}

/// A fully validated SQLite-family statement ready for runtime binding.
#[derive(Clone, Debug, PartialEq)]
pub struct CompiledStatement {
    sql: String,
    bindings: Vec<BindingValue>,
    result_columns: Vec<ResultColumn>,
    operation: OperationKind,
}

impl CompiledStatement {
    pub(crate) fn new(
        flavor: SqliteFamilyFlavor,
        sql: impl Into<String>,
        bindings: Vec<BindingValue>,
        result_columns: Vec<ResultColumn>,
        operation: OperationKind,
    ) -> Result<Self, CompileError> {
        let sql = sql.into();
        flavor
            .capabilities()
            .validate_statement(operation, &sql, bindings.len())?;

        Ok(Self {
            sql,
            bindings,
            result_columns,
            operation,
        })
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }

    pub fn bindings(&self) -> &[BindingValue] {
        &self.bindings
    }

    pub fn result_columns(&self) -> &[ResultColumn] {
        &self.result_columns
    }

    pub fn operation(&self) -> OperationKind {
        self.operation
    }

    pub fn into_parts(self) -> (String, Vec<BindingValue>, Vec<ResultColumn>, OperationKind) {
        (self.sql, self.bindings, self.result_columns, self.operation)
    }
}
