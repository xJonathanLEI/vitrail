use std::fmt;

use vitrail_sqlite_dialect::{Schema as DialectSchema, SchemaBuilder as DialectSchemaBuilder};

pub use vitrail_sqlite_dialect::{
    Attribute, DefaultAttribute, DefaultFunction, Field, FieldBuilder, FieldKind, FieldType, Model,
    ModelAttribute, ModelBuilder, ModelIndexAttribute, ModelIndexAttributeBuilder,
    ModelPrimaryKeyAttribute, ModelPrimaryKeyAttributeBuilder, ModelUniqueAttribute,
    ModelUniqueAttributeBuilder, RelationAttribute, RelationAttributeBuilder, Resolution,
    RustTypeAttribute, ScalarFieldType, ScalarType,
};

pub(crate) use vitrail_sqlite_dialect::SqliteDialect;

/// Schema definition for `vitrail-sqlite`.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct Schema {
    inner: DialectSchema,
}

impl fmt::Debug for Schema {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(formatter)
    }
}

impl Schema {
    #[doc(hidden)]
    pub fn __macro_dialect() -> impl vitrail_core::schema::Dialect {
        DialectSchema::__macro_dialect()
    }

    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::new()
    }

    pub fn models(&self) -> &[Model] {
        self.inner.models()
    }

    pub fn external_tables(&self) -> &[String] {
        self.inner.external_tables()
    }

    pub fn model(&self, name: &str) -> Option<&Model> {
        self.inner.model(name)
    }

    pub(crate) fn as_dialect(&self) -> &DialectSchema {
        &self.inner
    }
}

#[derive(Clone, Default, Eq, PartialEq)]
pub struct SchemaBuilder {
    inner: DialectSchemaBuilder,
}

impl fmt::Debug for SchemaBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(formatter)
    }
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn model(mut self, model: Model) -> Self {
        self.inner = self.inner.model(model);
        self
    }

    pub fn models(mut self, models: Vec<Model>) -> Self {
        self.inner = self.inner.models(models);
        self
    }

    pub fn external_table(mut self, table: impl Into<String>) -> Self {
        self.inner = self.inner.external_table(table);
        self
    }

    pub fn external_tables(mut self, tables: Vec<String>) -> Self {
        self.inner = self.inner.external_tables(tables);
        self
    }

    pub fn build(self) -> Result<Schema, crate::ValidationErrors> {
        self.inner.build().map(|inner| Schema { inner })
    }
}

/// Provides access to a generated SQLite schema.
pub trait SchemaAccess: Send + Sync + 'static {
    fn schema() -> &'static Schema;
}
