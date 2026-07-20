use sqlx::sqlite::SqliteArguments;
use sqlx::{Sqlite, query::Query as SqlxQuery};
use vitrail_sqlite_dialect::{BindingValue, CompileError};

pub(crate) fn map_compile_error(error: CompileError) -> sqlx::Error {
    sqlx::Error::Protocol(error.to_string())
}

pub(crate) fn bind_statement<'q>(
    mut query: SqlxQuery<'q, Sqlite, SqliteArguments<'q>>,
    bindings: &'q [BindingValue],
) -> SqlxQuery<'q, Sqlite, SqliteArguments<'q>> {
    for binding in bindings {
        query = match binding {
            BindingValue::Null => query.bind(Option::<i64>::None),
            BindingValue::Int(value) => query.bind(*value),
            BindingValue::String(value) => query.bind(value),
            BindingValue::Bool(value) => query.bind(*value),
            BindingValue::Float(value) => query.bind(*value),
            BindingValue::Bytes(value) => query.bind(value),
            BindingValue::DateTime(value) => query.bind(*value),
            BindingValue::Json(value) => query.bind(value.to_string()),
        };
    }

    query
}
