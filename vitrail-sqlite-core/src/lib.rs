mod migration;
mod migrator;
mod schema;
mod validation;

pub use migration::{
    ColumnDefault, ColumnType, ForeignKeyAction, SqliteColumn, SqliteForeignKey, SqliteIndex,
    SqliteMigration, SqlitePrimaryKey, SqliteSchema, SqliteTable,
};
pub use migrator::{
    AppliedMigration, ApplyMigrationsReport, EmbeddedMigrations, GeneratedMigration, Migration,
    MigrationDirectory, MigrationSource, MigratorError, SqliteMigrator,
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
