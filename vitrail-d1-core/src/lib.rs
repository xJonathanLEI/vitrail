mod client;
mod delete;
mod error;
mod executor;
mod insert;
mod query;
mod row;
mod session;
mod statement;
mod update;

pub use client::VitrailClient;
pub use delete::{DeleteMany, DeleteManyModel, DeleteSpec};
pub use error::{DecodeError, Error, decode_error};
#[doc(hidden)]
pub use executor::D1Executor;
pub use insert::{
    Insert, InsertFieldValue, InsertModel, InsertScalar, InsertSpec, InsertValue, InsertValueSet,
    InsertValues,
};
pub use query::{
    BoxFuture, Query, QueryFilter, QueryFilterValue, QueryFilterValues, QueryListScalar,
    QueryModel, QueryOrder, QueryOrderDirection, QueryPagination, QueryRelationSelection,
    QueryResultValue, QueryScalar, QuerySelection, QuerySpec, QueryValue, QueryVariableSet,
    QueryVariableValue, QueryVariables, StringValueType, alias_name, json_array_field,
    json_as_bool, json_as_bytes, json_as_datetime_utc, json_as_f64, json_as_i64, json_as_string,
    json_value, query_model_is_null, row_as_bytes, row_as_datetime_utc, row_optional_relation_json,
    row_relation_json, row_value, schema_error,
};
pub use row::D1Row;
pub use session::{Bookmark, SessionConstraint, VitrailSession};
pub use update::{
    UpdateFieldValue, UpdateMany, UpdateManyModel, UpdateScalar, UpdateSpec, UpdateValue,
    UpdateValueSet, UpdateValues,
};
pub use vitrail_sqlite_dialect::{
    Attribute, DefaultAttribute, DefaultFunction, Field, FieldBuilder, FieldKind, FieldType, Model,
    ModelAttribute, ModelBuilder, ModelIndexAttribute, ModelIndexAttributeBuilder,
    ModelPrimaryKeyAttribute, ModelPrimaryKeyAttributeBuilder, ModelUniqueAttribute,
    ModelUniqueAttributeBuilder, RelationAttribute, RelationAttributeBuilder, Resolution,
    RustTypeAttribute, ScalarFieldType, ScalarType, Schema, SchemaAccess, SchemaBuilder,
    ValidationError, ValidationErrors, ValidationLocation, validate_d1_schema,
};

pub use serde_json;

#[cfg(test)]
mod tests;
