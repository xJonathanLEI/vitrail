mod delete;
mod error;
mod filter;
mod insert;
mod migration;
mod query;
mod schema;
mod statement;
mod update;

pub use delete::compile_delete_many;
pub use error::CompileError;
pub use insert::{InsertFieldValue, InsertValue, InsertValues, compile_insert};
pub use migration::{
    ColumnDefault, ColumnType, ForeignKeyAction, SqliteColumn, SqliteForeignKey, SqliteIndex,
    SqliteMigration, SqlitePrimaryKey, SqliteSchema, SqliteTable,
};
pub use query::{
    QueryFilter, QueryFilterValue, QueryFilterValues, QueryOrder, QueryOrderDirection,
    QueryPagination, QueryRelationSelection, QuerySelection, QueryVariableValue, QueryVariables,
    alias_name, compile_query,
};
pub use schema::{
    Attribute, DefaultAttribute, DefaultFunction, Field, FieldBuilder, FieldKind, FieldType, Model,
    ModelAttribute, ModelBuilder, ModelIndexAttribute, ModelIndexAttributeBuilder,
    ModelPrimaryKeyAttribute, ModelPrimaryKeyAttributeBuilder, ModelUniqueAttribute,
    ModelUniqueAttributeBuilder, RelationAttribute, RelationAttributeBuilder, Resolution,
    RustTypeAttribute, ScalarFieldType, ScalarType, Schema, SchemaAccess, SchemaBuilder,
    SqliteDialect, SqliteDialectPolicy,
};
pub use statement::{
    BindingValue, CompiledStatement, OperationKind, ResultColumn, ResultColumnKind,
};
pub use update::{UpdateFieldValue, UpdateValue, UpdateValues, compile_update_many};
pub use vitrail_core::validation::{ValidationError, ValidationErrors, ValidationLocation};

#[cfg(test)]
mod tests;
