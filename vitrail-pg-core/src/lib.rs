mod client;
mod insert;
mod migration;
mod migrator;
mod query;
mod schema;
mod update;
mod validation;

pub use client::VitrailClient;
pub use insert::{Insert, InsertModel, InsertSpec, InsertValue, InsertValueSet, InsertValues};
pub use migration::{
    ColumnDefault, ColumnType, ForeignKeyAction, PostgresColumn, PostgresForeignKey, PostgresIndex,
    PostgresMigration, PostgresPrimaryKey, PostgresSchema, PostgresTable,
};
pub use migrator::{
    AppliedMigration, ApplyMigrationsReport, DiskMigration, GeneratedMigration, MigrationDirectory,
    MigratorError, PostgresMigrator,
};
pub use query::{
    BoxFuture, Query, QueryFilter, QueryFilterValue, QueryModel, QueryRelationSelection,
    QuerySelection, QuerySpec, QueryValue, QueryVariableSet, QueryVariableValue, QueryVariables,
    SchemaAccess, alias_name, json_array_field, json_as_bool, json_as_datetime_utc, json_as_f64,
    json_as_i64, json_as_string, query_model_is_null, row_as_datetime_utc, schema_error,
};
pub use schema::{
    Attribute, DefaultAttribute, DefaultFunction, Field, FieldBuilder, FieldKind, FieldType, Model,
    ModelBuilder, RelationAttribute, RelationAttributeBuilder, Resolution, ScalarFieldType,
    ScalarType, Schema, SchemaBuilder,
};
pub use update::{
    UpdateMany, UpdateManyModel, UpdateSpec, UpdateValue, UpdateValueSet, UpdateValues,
};
pub use validation::{ValidationError, ValidationErrors, ValidationLocation};

#[cfg(test)]
mod tests;

pub use serde_json;
