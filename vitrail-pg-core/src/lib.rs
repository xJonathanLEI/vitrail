mod client;
mod delete;
mod insert;
mod migration;
mod migrator;
mod query;
mod schema;
mod update;
mod validation;

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
    BoxFuture, Query, QueryFilter, QueryFilterValue, QueryModel, QueryRelationSelection,
    QueryResultValue, QueryScalar, QuerySelection, QuerySpec, QueryValue, QueryVariableSet,
    QueryVariableValue, QueryVariables, SchemaAccess, StringValueType, alias_name,
    json_array_field, json_as_bool, json_as_datetime_utc, json_as_f64, json_as_i64, json_as_string,
    json_value, query_model_is_null, row_as_datetime_utc, row_value, schema_error,
};
pub use schema::{
    Attribute, DefaultAttribute, DefaultFunction, Field, FieldBuilder, FieldKind, FieldType, Model,
    ModelAttribute, ModelBuilder, ModelPrimaryKeyAttribute, ModelPrimaryKeyAttributeBuilder,
    ModelUniqueAttribute, ModelUniqueAttributeBuilder, RelationAttribute, RelationAttributeBuilder,
    Resolution, RustTypeAttribute, ScalarFieldType, ScalarType, Schema, SchemaBuilder,
};
pub use update::{
    UpdateMany, UpdateManyModel, UpdateScalar, UpdateSpec, UpdateValue, UpdateValueSet,
    UpdateValues,
};
pub use validation::{ValidationError, ValidationErrors, ValidationLocation};

#[cfg(test)]
mod tests;

pub use serde_json;
