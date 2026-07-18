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
pub struct SqliteDialectPolicy;

impl DialectPolicy for SqliteDialectPolicy {
    fn validate_scalar_type(scalar: ScalarType) -> Result<(), String> {
        match scalar {
            ScalarType::Decimal => Err(
                "`Decimal` fields are not supported by the SQLite dialect because SQLx cannot preserve arbitrary-precision decimals for SQLite"
                    .to_owned(),
            ),
            ScalarType::Int
            | ScalarType::BigInt
            | ScalarType::String
            | ScalarType::Boolean
            | ScalarType::DateTime
            | ScalarType::Float
            | ScalarType::Bytes
            | ScalarType::Json => Ok(()),
        }
    }

    fn validate_native_attribute(
        attribute: NativeAttribute,
        _field_type: &FieldType,
    ) -> Result<(), String> {
        match attribute {
            NativeAttribute::DbUuid => Err(
                "`@db.Uuid` is a PostgreSQL native attribute and is not supported by the SQLite dialect"
                    .to_owned(),
            ),
        }
    }

    fn validate_default(field_type: &FieldType, function: &DefaultFunction) -> Result<(), String> {
        if matches!(field_type, FieldType::Relation { .. }) {
            return Err("`@default` can only be used on scalar fields".to_owned());
        }

        match function {
            DefaultFunction::Autoincrement => {
                if field_type == &FieldType::int() {
                    Ok(())
                } else {
                    Err(
                        "`@default(autoincrement())` is only supported on `Int` fields in SQLite"
                            .to_owned(),
                    )
                }
            }
            DefaultFunction::Now => {
                if field_type == &FieldType::date_time() {
                    Ok(())
                } else {
                    Err("`@default(now())` is only supported on `DateTime` fields".to_owned())
                }
            }
            DefaultFunction::Other(other) => Err(format!(
                "unsupported default function `{}`; expected `autoincrement` or `now`",
                other
            )),
        }
    }

    fn validate_autoincrement_primary_key(
        is_single_column_primary_key: bool,
    ) -> Result<(), String> {
        if is_single_column_primary_key {
            Ok(())
        } else {
            Err(
                "`@default(autoincrement())` is only supported on a field that is the sole primary key column in SQLite"
                    .to_owned(),
            )
        }
    }

    fn normalize_external_table_name(table: &str) -> Result<String, String> {
        if table.is_empty() {
            return Err("external table name must not be empty".to_owned());
        }

        if table.contains('.') {
            return Err(format!(
                "external table `{table}` must use an unqualified table name in SQLite"
            ));
        }

        Ok(table.to_owned())
    }
}

pub(crate) type SqliteDialect = DialectMarker<SqliteDialectPolicy>;

pub type Attribute = SharedAttribute<SqliteDialect>;
pub type DefaultAttribute = SharedDefaultAttribute<SqliteDialect>;
pub type Field = SharedField<SqliteDialect>;
pub type FieldBuilder = SharedFieldBuilder<SqliteDialect>;
pub type Model = SharedModel<SqliteDialect>;
pub type ModelAttribute = SharedModelAttribute<SqliteDialect>;
pub type ModelBuilder = SharedModelBuilder<SqliteDialect>;
pub type ModelIndexAttribute = SharedModelIndexAttribute<SqliteDialect>;
pub type ModelIndexAttributeBuilder = SharedModelIndexAttributeBuilder<SqliteDialect>;
pub type ModelPrimaryKeyAttribute = SharedModelPrimaryKeyAttribute<SqliteDialect>;
pub type ModelPrimaryKeyAttributeBuilder = SharedModelPrimaryKeyAttributeBuilder<SqliteDialect>;
pub type ModelUniqueAttribute = SharedModelUniqueAttribute<SqliteDialect>;
pub type ModelUniqueAttributeBuilder = SharedModelUniqueAttributeBuilder<SqliteDialect>;
pub type RelationAttribute = SharedRelationAttribute<SqliteDialect>;
pub type RelationAttributeBuilder = SharedRelationAttributeBuilder<SqliteDialect>;
pub type Resolution<'a> = SharedResolution<'a, SqliteDialect>;
pub type RustTypeAttribute = SharedRustTypeAttribute<SqliteDialect>;

/// Schema definition for `vitrail-sqlite`.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct Schema {
    inner: SharedSchema<SqliteDialect>,
}

impl fmt::Debug for Schema {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(formatter)
    }
}

impl Schema {
    #[doc(hidden)]
    pub fn __macro_dialect() -> impl vitrail_core::schema::Dialect {
        SqliteDialect::default()
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
}

#[derive(Clone, Default, Eq, PartialEq)]
pub struct SchemaBuilder {
    inner: SharedSchemaBuilder<SqliteDialect>,
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
