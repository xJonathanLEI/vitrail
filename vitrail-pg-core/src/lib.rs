mod client;
mod delete;
mod insert;
mod migration;
mod migrator;
mod query;
mod schema;
mod transaction;
mod update;
mod validation;

use sqlx::postgres::{PgArguments, PgPool, PgQueryResult, PgRow};
use sqlx::{Postgres, query::Query as SqlxQuery};

/// Internal execution abstraction for Postgres-backed query and write operations.
///
/// This trait is used by `VitrailClient` and `VitrailTransaction` so the runtime
/// specs can execute against either a pool or an explicit transaction while
/// sharing the same SQL generation logic.
#[doc(hidden)]
pub trait PgExecutor: private::Sealed + Send + Sync {
    fn fetch_all<'a>(
        &'a self,
        query: SqlxQuery<'a, Postgres, PgArguments>,
    ) -> BoxFuture<'a, Result<Vec<PgRow>, sqlx::Error>>;

    fn fetch_optional<'a>(
        &'a self,
        query: SqlxQuery<'a, Postgres, PgArguments>,
    ) -> BoxFuture<'a, Result<Option<PgRow>, sqlx::Error>>;

    fn fetch_one<'a>(
        &'a self,
        query: SqlxQuery<'a, Postgres, PgArguments>,
    ) -> BoxFuture<'a, Result<PgRow, sqlx::Error>>;

    fn execute<'a>(
        &'a self,
        query: SqlxQuery<'a, Postgres, PgArguments>,
    ) -> BoxFuture<'a, Result<PgQueryResult, sqlx::Error>>;
}

impl private::Sealed for PgPool {}
impl private::Sealed for transaction::VitrailTransaction {}

impl PgExecutor for PgPool {
    fn fetch_all<'a>(
        &'a self,
        query: SqlxQuery<'a, Postgres, PgArguments>,
    ) -> BoxFuture<'a, Result<Vec<PgRow>, sqlx::Error>> {
        Box::pin(async move { query.fetch_all(self).await })
    }

    fn fetch_optional<'a>(
        &'a self,
        query: SqlxQuery<'a, Postgres, PgArguments>,
    ) -> BoxFuture<'a, Result<Option<PgRow>, sqlx::Error>> {
        Box::pin(async move { query.fetch_optional(self).await })
    }

    fn fetch_one<'a>(
        &'a self,
        query: SqlxQuery<'a, Postgres, PgArguments>,
    ) -> BoxFuture<'a, Result<PgRow, sqlx::Error>> {
        Box::pin(async move { query.fetch_one(self).await })
    }

    fn execute<'a>(
        &'a self,
        query: SqlxQuery<'a, Postgres, PgArguments>,
    ) -> BoxFuture<'a, Result<PgQueryResult, sqlx::Error>> {
        Box::pin(async move { query.execute(self).await })
    }
}

mod private {
    /// Seals internal traits that must be visible across the crate but must not
    /// be implementable by external crates.
    pub trait Sealed {}
}

pub use client::VitrailClient;
pub use delete::{DeleteMany, DeleteManyModel, DeleteSpec};
pub use insert::{
    Insert, InsertModel, InsertScalar, InsertSpec, InsertValue, InsertValueSet, InsertValues,
};
pub use migration::{
    ColumnDefault, ColumnType, ForeignKeyAction, PostgresColumn, PostgresForeignKey, PostgresIndex,
    PostgresMigration, PostgresPrimaryKey, PostgresSchema, PostgresTable,
};
pub use migrator::{
    AppliedMigration, ApplyMigrationsReport, DiskMigration, GeneratedMigration, MigrationDirectory,
    MigratorError, PostgresMigrator,
};
pub use query::{
    BoxFuture, Query, QueryFilter, QueryFilterValue, QueryFilterValues, QueryModel,
    QueryRelationSelection, QueryResultValue, QueryScalar, QuerySelection, QuerySpec, QueryValue,
    QueryVariableSet, QueryVariableValue, QueryVariables, SchemaAccess, StringValueType,
    alias_name, json_array_field, json_as_bool, json_as_bytes, json_as_datetime_utc,
    json_as_decimal, json_as_f64, json_as_i64, json_as_string, json_value, parse_decimal,
    query_model_is_null, row_as_bytes, row_as_datetime_utc, row_as_decimal, row_value,
    schema_error,
};
pub use schema::{
    Attribute, DefaultAttribute, DefaultFunction, Field, FieldBuilder, FieldKind, FieldType, Model,
    ModelAttribute, ModelBuilder, ModelIndexAttribute, ModelIndexAttributeBuilder,
    ModelPrimaryKeyAttribute, ModelPrimaryKeyAttributeBuilder, ModelUniqueAttribute,
    ModelUniqueAttributeBuilder, RelationAttribute, RelationAttributeBuilder, Resolution,
    RustTypeAttribute, ScalarFieldType, ScalarType, Schema, SchemaBuilder,
};
pub use transaction::{TransactionIsolationLevel, TransactionOptions, VitrailTransaction};
pub use update::{
    UpdateMany, UpdateManyModel, UpdateScalar, UpdateSpec, UpdateValue, UpdateValueSet,
    UpdateValues,
};
pub use validation::{ValidationError, ValidationErrors, ValidationLocation};

#[cfg(test)]
mod tests;

pub use rust_decimal;
pub use serde_json;
pub use uuid;
