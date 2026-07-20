mod delete;
mod error;
mod filter;
mod flavor;
mod insert;
mod migration;
mod query;
mod schema;
mod statement;
mod update;

pub use delete::{compile_delete_many, compile_delete_many_with_flavor};
pub use error::CompileError;
pub use flavor::{
    D1_JSON_FUNCTION_ARGUMENT_LIMIT, D1_MAX_BINDINGS, D1_MAX_COLUMNS, D1_MAX_SQL_BYTES,
    SqliteFamilyFlavor,
};
pub use insert::{
    InsertFieldValue, InsertValue, InsertValues, compile_insert, compile_insert_with_flavor,
};
pub use migration::{
    ColumnDefault, ColumnType, ForeignKeyAction, SqliteColumn, SqliteForeignKey, SqliteIndex,
    SqliteMigration, SqlitePrimaryKey, SqliteSchema, SqliteTable,
};
pub use query::{
    QueryFilter, QueryFilterValue, QueryFilterValues, QueryOrder, QueryOrderDirection,
    QueryPagination, QueryRelationSelection, QuerySelection, QueryVariableValue, QueryVariables,
    alias_name, compile_query, compile_query_with_flavor,
};
pub use schema::{
    Attribute, DefaultAttribute, DefaultFunction, Field, FieldBuilder, FieldKind, FieldType, Model,
    ModelAttribute, ModelBuilder, ModelIndexAttribute, ModelIndexAttributeBuilder,
    ModelPrimaryKeyAttribute, ModelPrimaryKeyAttributeBuilder, ModelUniqueAttribute,
    ModelUniqueAttributeBuilder, RelationAttribute, RelationAttributeBuilder, Resolution,
    RustTypeAttribute, ScalarFieldType, ScalarType, Schema, SchemaAccess, SchemaBuilder,
    SqliteDialect, SqliteDialectPolicy, validate_d1_schema, validate_d1_schema_for_macro,
};
pub use statement::{
    BindingValue, CompiledStatement, OperationKind, ResultColumn, ResultColumnKind,
};
pub use update::{
    UpdateFieldValue, UpdateValue, UpdateValues, compile_update_many,
    compile_update_many_with_flavor,
};
pub use vitrail_core::validation::{ValidationError, ValidationErrors, ValidationLocation};

#[cfg(test)]
mod d1_tests;
#[cfg(test)]
mod tests;
