mod client;
mod query;
mod schema;
mod validation;

pub use client::SqlxVitrailClient;
pub use query::{
    BoxFuture, Query, QueryModel, QueryRelationSelection, QuerySelection, QuerySpec, SchemaAccess,
    alias_name, query_model_is_null,
};
pub use schema::{
    Attribute, DefaultAttribute, DefaultFunction, Field, FieldBuilder, FieldKind, FieldType, Model,
    ModelBuilder, RelationAttribute, RelationAttributeBuilder, Resolution, ScalarFieldType,
    ScalarType, Schema, SchemaBuilder,
};
pub use validation::{ValidationError, ValidationErrors, ValidationLocation};

#[cfg(test)]
mod tests;
