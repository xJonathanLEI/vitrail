use std::fmt;

use vitrail_core::schema::{
    Attribute as SharedAttribute, DefaultAttribute as SharedDefaultAttribute, DialectMarker,
    DialectPolicy, Field as SharedField, FieldBuilder as SharedFieldBuilder, Model as SharedModel,
    ModelAttribute as SharedModelAttribute, ModelBuilder as SharedModelBuilder,
    ModelIndexAttribute as SharedModelIndexAttribute,
    ModelIndexAttributeBuilder as SharedModelIndexAttributeBuilder,
    ModelPrimaryKeyAttribute as SharedModelPrimaryKeyAttribute,
    ModelPrimaryKeyAttributeBuilder as SharedModelPrimaryKeyAttributeBuilder,
    ModelUniqueAttribute as SharedModelUniqueAttribute,
    ModelUniqueAttributeBuilder as SharedModelUniqueAttributeBuilder, NativeAttribute,
    RelationAttribute as SharedRelationAttribute,
    RelationAttributeBuilder as SharedRelationAttributeBuilder, Resolution as SharedResolution,
    RustTypeAttribute as SharedRustTypeAttribute, Schema as SharedSchema,
    SchemaBuilder as SharedSchemaBuilder,
};

pub use vitrail_core::schema::{
    DefaultFunction, FieldKind, FieldType, ScalarFieldType, ScalarType,
};

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PostgresDialectPolicy;

impl DialectPolicy for PostgresDialectPolicy {
    fn validate_scalar_type(_scalar: ScalarType) -> Result<(), String> {
        Ok(())
    }

    fn validate_native_attribute(
        attribute: NativeAttribute,
        field_type: &FieldType,
    ) -> Result<(), String> {
        match attribute {
            NativeAttribute::DbUuid => {
                if matches!(field_type, FieldType::Relation { .. }) {
                    Err("`@db.Uuid` can only be used on scalar fields".to_owned())
                } else if !matches!(
                    field_type,
                    FieldType::Scalar(scalar) if scalar.scalar() == ScalarType::String
                ) {
                    Err("`@db.Uuid` is only supported on `String` fields".to_owned())
                } else {
                    Ok(())
                }
            }
        }
    }

    fn validate_default(field_type: &FieldType, function: &DefaultFunction) -> Result<(), String> {
        if matches!(field_type, FieldType::Relation { .. }) {
            return Err("`@default` can only be used on scalar fields".to_owned());
        }

        match function {
            DefaultFunction::Autoincrement => {
                if field_type != &FieldType::int() && field_type != &FieldType::big_int() {
                    Err(
                        "`@default(autoincrement())` is only supported on `Int` and `BigInt` fields"
                            .to_owned(),
                    )
                } else {
                    Ok(())
                }
            }
            DefaultFunction::Now => {
                if field_type != &FieldType::date_time() {
                    Err("`@default(now())` is only supported on `DateTime` fields".to_owned())
                } else {
                    Ok(())
                }
            }
            DefaultFunction::Other(other) => Err(format!(
                "unsupported default function `{}`; expected `autoincrement` or `now`",
                other
            )),
        }
    }

    fn normalize_external_table_name(table: &str) -> Result<String, String> {
        if table.is_empty() {
            return Err("external table name must not be empty".to_owned());
        }

        if let Some((schema, table_name)) = table.split_once('.') {
            if schema != "public" {
                return Err(format!(
                    "external table `{}` must target the `public` schema",
                    table
                ));
            }

            if table_name.is_empty() {
                return Err(format!(
                    "external table `{}` must include a table name",
                    table
                ));
            }

            return Ok(table_name.to_owned());
        }

        Ok(table.to_owned())
    }
}

pub(crate) type PostgresDialect = DialectMarker<PostgresDialectPolicy>;

pub type Attribute = SharedAttribute<PostgresDialect>;
pub type DefaultAttribute = SharedDefaultAttribute<PostgresDialect>;
pub type Field = SharedField<PostgresDialect>;
pub type FieldBuilder = SharedFieldBuilder<PostgresDialect>;
pub type Model = SharedModel<PostgresDialect>;
pub type ModelAttribute = SharedModelAttribute<PostgresDialect>;
pub type ModelBuilder = SharedModelBuilder<PostgresDialect>;
pub type ModelIndexAttribute = SharedModelIndexAttribute<PostgresDialect>;
pub type ModelIndexAttributeBuilder = SharedModelIndexAttributeBuilder<PostgresDialect>;
pub type ModelPrimaryKeyAttribute = SharedModelPrimaryKeyAttribute<PostgresDialect>;
pub type ModelPrimaryKeyAttributeBuilder = SharedModelPrimaryKeyAttributeBuilder<PostgresDialect>;
pub type ModelUniqueAttribute = SharedModelUniqueAttribute<PostgresDialect>;
pub type ModelUniqueAttributeBuilder = SharedModelUniqueAttributeBuilder<PostgresDialect>;
pub type RelationAttribute = SharedRelationAttribute<PostgresDialect>;
pub type RelationAttributeBuilder = SharedRelationAttributeBuilder<PostgresDialect>;
pub type Resolution<'a> = SharedResolution<'a, PostgresDialect>;
pub type RustTypeAttribute = SharedRustTypeAttribute<PostgresDialect>;

/// Schema definition for `vitrail-pg`.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct Schema {
    inner: SharedSchema<PostgresDialect>,
}

impl fmt::Debug for Schema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl Schema {
    #[doc(hidden)]
    pub fn __macro_dialect() -> impl vitrail_core::schema::Dialect {
        PostgresDialect::default()
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

    pub(crate) fn resolve_model(&self, requested: &str) -> Resolution<'_> {
        self.inner.resolve_model(requested)
    }
}

#[derive(Clone, Default, Eq, PartialEq)]
pub struct SchemaBuilder {
    inner: SharedSchemaBuilder<PostgresDialect>,
}

impl fmt::Debug for SchemaBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
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

/// Provides access to a generated PostgreSQL schema.
pub trait SchemaAccess: Send + Sync + 'static {
    fn schema() -> &'static Schema;
}
