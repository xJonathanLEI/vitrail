mod client;
mod filter;
mod insert;
mod migration;
mod migrator;
mod query;
mod schema;
mod validation;

use sqlx::sqlite::{SqliteArguments, SqlitePool, SqliteRow};
use sqlx::{Sqlite, query::Query as SqlxQuery};

/// Internal execution abstraction for SQLite-backed query and write operations.
///
/// This trait keeps runtime execution independent from the public client while
/// restricting executors to implementations owned by this crate.
#[doc(hidden)]
pub trait SqliteExecutor: private::Sealed + Send + Sync {
    fn fetch_all<'a>(
        &'a self,
        query: SqlxQuery<'a, Sqlite, SqliteArguments<'a>>,
    ) -> futures_util::future::BoxFuture<'a, Result<Vec<SqliteRow>, sqlx::Error>>;

    fn fetch_optional<'a>(
        &'a self,
        query: SqlxQuery<'a, Sqlite, SqliteArguments<'a>>,
    ) -> futures_util::future::BoxFuture<'a, Result<Option<SqliteRow>, sqlx::Error>>;

    fn fetch_one<'a>(
        &'a self,
        query: SqlxQuery<'a, Sqlite, SqliteArguments<'a>>,
    ) -> futures_util::future::BoxFuture<'a, Result<SqliteRow, sqlx::Error>>;
}

impl private::Sealed for SqlitePool {}

impl SqliteExecutor for SqlitePool {
    fn fetch_all<'a>(
        &'a self,
        query: SqlxQuery<'a, Sqlite, SqliteArguments<'a>>,
    ) -> futures_util::future::BoxFuture<'a, Result<Vec<SqliteRow>, sqlx::Error>> {
        Box::pin(async move { query.fetch_all(self).await })
    }

    fn fetch_optional<'a>(
        &'a self,
        query: SqlxQuery<'a, Sqlite, SqliteArguments<'a>>,
    ) -> futures_util::future::BoxFuture<'a, Result<Option<SqliteRow>, sqlx::Error>> {
        Box::pin(async move { query.fetch_optional(self).await })
    }

    fn fetch_one<'a>(
        &'a self,
        query: SqlxQuery<'a, Sqlite, SqliteArguments<'a>>,
    ) -> futures_util::future::BoxFuture<'a, Result<SqliteRow, sqlx::Error>> {
        Box::pin(async move { query.fetch_one(self).await })
    }
}

mod private {
    /// Seals internal traits that must be visible across the crate but must not
    /// be implementable by external crates.
    pub trait Sealed {}
}

pub use client::VitrailClient;
pub use insert::{
    Insert, InsertFieldValue, InsertModel, InsertScalar, InsertSpec, InsertValue, InsertValueSet,
    InsertValues,
};
pub use migration::{
    ColumnDefault, ColumnType, ForeignKeyAction, SqliteColumn, SqliteForeignKey, SqliteIndex,
    SqliteMigration, SqlitePrimaryKey, SqliteSchema, SqliteTable,
};
pub use migrator::{
    AppliedMigration, ApplyMigrationsReport, EmbeddedMigrations, GeneratedMigration, Migration,
    MigrationDirectory, MigrationSource, MigratorError, SqliteMigrator,
};
pub use query::{
    BoxFuture, Query, QueryFilter, QueryFilterValue, QueryFilterValues, QueryModel, QueryOrder,
    QueryOrderDirection, QueryPagination, QueryRelationSelection, QueryResultValue, QueryScalar,
    QuerySelection, QuerySpec, QueryValue, QueryVariableSet, QueryVariableValue, QueryVariables,
    StringValueType, alias_name, json_array_field, json_as_bool, json_as_bytes,
    json_as_datetime_utc, json_as_f64, json_as_i64, json_as_string, json_value,
    query_model_is_null, row_as_bytes, row_as_datetime_utc, row_optional_relation_json,
    row_relation_json, row_value, schema_error,
};
pub use schema::{
    Attribute, DefaultAttribute, DefaultFunction, Field, FieldBuilder, FieldKind, FieldType, Model,
    ModelAttribute, ModelBuilder, ModelIndexAttribute, ModelIndexAttributeBuilder,
    ModelPrimaryKeyAttribute, ModelPrimaryKeyAttributeBuilder, ModelUniqueAttribute,
    ModelUniqueAttributeBuilder, RelationAttribute, RelationAttributeBuilder, Resolution,
    RustTypeAttribute, ScalarFieldType, ScalarType, Schema, SchemaAccess, SchemaBuilder,
};
pub use validation::{ValidationError, ValidationErrors, ValidationLocation};

#[cfg(test)]
mod tests;

pub use serde_json;
pub use sqlx;
