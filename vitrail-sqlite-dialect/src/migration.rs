use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::flavor::SqliteFamilyFlavor;
use crate::{
    Attribute, CompileError, DefaultFunction, Field, FieldType, Resolution, ScalarType, Schema,
    SchemaAccess,
};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SqliteSchema {
    tables: Vec<SqliteTable>,
}

impl SqliteSchema {
    pub fn empty() -> Self {
        Self::default()
    }

    #[doc(hidden)]
    pub fn from_introspection(tables: Vec<SqliteTable>) -> Self {
        Self { tables }
    }

    pub fn from_schema(schema: &Schema) -> Self {
        let mut tables = Vec::with_capacity(schema.models().len());

        for model in schema.models() {
            let primary_key = SqlitePrimaryKey {
                name: format!("{}_pkey", model.name()),
                columns: model
                    .primary_key_columns()
                    .into_iter()
                    .map(str::to_owned)
                    .collect(),
            };

            let mut columns = Vec::new();
            let mut field_unique_indexes = Vec::new();
            let mut field_indexes = Vec::new();
            let mut model_indexes = Vec::new();
            let mut model_unique_indexes = Vec::new();
            let mut foreign_keys = Vec::new();

            for field in model.fields() {
                if field.kind().is_scalar() {
                    columns.push(SqliteColumn::from_field(model.name(), field));

                    if field
                        .attributes()
                        .iter()
                        .any(|attribute| matches!(attribute, Attribute::Unique))
                    {
                        field_unique_indexes.push(SqliteIndex {
                            name: format!("{}_{}_key", model.name(), field.name()),
                            columns: vec![field.name().to_owned()],
                            unique: true,
                            definition_supported: true,
                        });
                    }

                    if field
                        .attributes()
                        .iter()
                        .any(|attribute| matches!(attribute, Attribute::Index))
                    {
                        field_indexes.push(SqliteIndex {
                            name: format!("{}_{}_idx", model.name(), field.name()),
                            columns: vec![field.name().to_owned()],
                            unique: false,
                            definition_supported: true,
                        });
                    }

                    continue;
                }

                let Some(relation) = field.relation() else {
                    continue;
                };

                if field.ty().is_many() {
                    continue;
                }

                let referenced_table = match schema.resolve_model(field.ty().name()) {
                    Resolution::Found(model) => model.name().to_owned(),
                    Resolution::NotFound | Resolution::Ambiguous(_) => {
                        unreachable!("schema relations were validated before migration modeling")
                    }
                };

                foreign_keys.push(SqliteForeignKey {
                    name: format!("{}_{}_fkey", model.name(), relation.fields().join("_")),
                    columns: relation.fields().to_vec(),
                    referenced_table,
                    referenced_columns: relation.references().to_vec(),
                    on_delete: if field.ty().is_optional() {
                        ForeignKeyAction::SetNull
                    } else {
                        ForeignKeyAction::Restrict
                    },
                    on_update: ForeignKeyAction::Cascade,
                });
            }

            for index_columns in model.index_column_sets() {
                model_indexes.push(SqliteIndex {
                    name: format!("{}_{}_idx", model.name(), index_columns.join("_")),
                    columns: index_columns.into_iter().map(str::to_owned).collect(),
                    unique: false,
                    definition_supported: true,
                });
            }

            for unique_columns in model.unique_column_sets() {
                model_unique_indexes.push(SqliteIndex {
                    name: format!("{}_{}_key", model.name(), unique_columns.join("_")),
                    columns: unique_columns.into_iter().map(str::to_owned).collect(),
                    unique: true,
                    definition_supported: true,
                });
            }

            let mut indexes = field_unique_indexes;
            indexes.extend(field_indexes);
            indexes.extend(model_indexes);
            indexes.extend(model_unique_indexes);

            tables.push(SqliteTable {
                name: model.name().to_owned(),
                columns,
                primary_key,
                indexes,
                foreign_keys,
            });
        }

        Self { tables }
    }

    pub fn from_schema_access<S>() -> Self
    where
        S: SchemaAccess,
    {
        Self::from_schema(S::schema())
    }

    pub fn tables(&self) -> &[SqliteTable] {
        &self.tables
    }

    pub fn migrate_from(&self, current: &Self) -> SqliteMigration {
        let current_tables = current
            .tables
            .iter()
            .map(|table| (table.name.as_str(), table))
            .collect::<HashMap<_, _>>();
        let target_tables = self
            .tables
            .iter()
            .map(|table| (table.name.as_str(), table))
            .collect::<HashMap<_, _>>();

        let mut ordered_current_tables = current.tables.iter().collect::<Vec<_>>();
        ordered_current_tables.sort_by(|left, right| left.name.cmp(&right.name));

        let mut ordered_target_tables = self.tables.iter().collect::<Vec<_>>();
        ordered_target_tables.sort_by(|left, right| left.name.cmp(&right.name));

        let table_changes = self
            .tables
            .iter()
            .filter_map(|target_table| {
                current_tables
                    .get(target_table.name.as_str())
                    .map(|current_table| {
                        (
                            target_table.name.as_str(),
                            classify_table_change(current_table, target_table),
                        )
                    })
            })
            .collect::<HashMap<_, _>>();

        let redefined_tables = table_changes
            .iter()
            .filter_map(|(table, change)| matches!(change, TableChange::Redefine).then_some(*table))
            .collect::<HashSet<_>>();

        let mut steps = Vec::new();

        for current_table in &ordered_current_tables {
            let Some(target_table) = target_tables.get(current_table.name.as_str()) else {
                continue;
            };

            if redefined_tables.contains(current_table.name.as_str()) {
                continue;
            }

            // Prisma drops SQLite indexes in reverse creation order.
            for current_index in current_table.indexes.iter().rev() {
                if target_table
                    .index_named(&current_index.name)
                    .is_none_or(|target_index| target_index != current_index)
                {
                    steps.push(MigrationStep::DropIndex {
                        name: current_index.name.clone(),
                    });
                }
            }
        }

        for target_table in &ordered_target_tables {
            let Some(TableChange::AddColumns(columns)) =
                table_changes.get(target_table.name.as_str())
            else {
                continue;
            };

            steps.push(MigrationStep::AlterTable {
                table: target_table.name.clone(),
                columns: columns.clone(),
            });
        }

        for current_table in &ordered_current_tables {
            if !target_tables.contains_key(current_table.name.as_str()) {
                steps.push(MigrationStep::DropTable {
                    name: current_table.name.clone(),
                });
            }
        }

        for target_table in &self.tables {
            if !current_tables.contains_key(target_table.name.as_str()) {
                steps.push(MigrationStep::CreateTable {
                    table: target_table.clone(),
                });
            }
        }

        let tables_to_redefine = ordered_target_tables
            .iter()
            .copied()
            .filter_map(|target_table| {
                if !redefined_tables.contains(target_table.name.as_str()) {
                    return None;
                }

                let current_table = current_tables[target_table.name.as_str()];
                let mut copied_columns = target_table
                    .columns
                    .iter()
                    .filter(|column| current_table.column_named(&column.name).is_some())
                    .map(|column| column.name.clone())
                    .collect::<Vec<_>>();
                copied_columns.sort();

                Some(RedefinedTable {
                    table: target_table.clone(),
                    copied_columns,
                })
            })
            .collect::<Vec<_>>();

        if !tables_to_redefine.is_empty() {
            steps.push(MigrationStep::RedefineTables {
                tables: tables_to_redefine,
            });
        }

        for target_table in &self.tables {
            if current_tables.contains_key(target_table.name.as_str()) {
                continue;
            }

            for index in &target_table.indexes {
                steps.push(MigrationStep::CreateIndex {
                    table: target_table.name.clone(),
                    index: index.clone(),
                });
            }
        }

        for target_table in &ordered_target_tables {
            let Some(current_table) = current_tables.get(target_table.name.as_str()) else {
                continue;
            };

            if redefined_tables.contains(target_table.name.as_str()) {
                continue;
            }

            for target_index in &target_table.indexes {
                if current_table
                    .index_named(&target_index.name)
                    .is_none_or(|current_index| current_index != target_index)
                {
                    steps.push(MigrationStep::CreateIndex {
                        table: target_table.name.clone(),
                        index: target_index.clone(),
                    });
                }
            }
        }

        SqliteMigration { steps }
    }
}

impl Schema {
    pub fn to_sqlite_schema(&self) -> SqliteSchema {
        SqliteSchema::from_schema(self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqliteTable {
    name: String,
    columns: Vec<SqliteColumn>,
    primary_key: SqlitePrimaryKey,
    indexes: Vec<SqliteIndex>,
    foreign_keys: Vec<SqliteForeignKey>,
}

impl SqliteTable {
    #[doc(hidden)]
    pub fn from_introspection(
        name: String,
        columns: Vec<SqliteColumn>,
        primary_key: SqlitePrimaryKey,
        indexes: Vec<SqliteIndex>,
        foreign_keys: Vec<SqliteForeignKey>,
    ) -> Self {
        Self {
            name,
            columns,
            primary_key,
            indexes,
            foreign_keys,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> &[SqliteColumn] {
        &self.columns
    }

    pub fn primary_key(&self) -> &SqlitePrimaryKey {
        &self.primary_key
    }

    pub fn indexes(&self) -> &[SqliteIndex] {
        &self.indexes
    }

    pub fn foreign_keys(&self) -> &[SqliteForeignKey] {
        &self.foreign_keys
    }

    fn column_named(&self, name: &str) -> Option<&SqliteColumn> {
        self.columns.iter().find(|column| column.name == name)
    }

    fn index_named(&self, name: &str) -> Option<&SqliteIndex> {
        self.indexes.iter().find(|index| index.name == name)
    }

    fn foreign_key_named(&self, name: &str) -> Option<&SqliteForeignKey> {
        self.foreign_keys
            .iter()
            .find(|foreign_key| foreign_key.name == name)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqliteColumn {
    name: String,
    ty: ColumnType,
    nullable: bool,
    default: Option<ColumnDefault>,
}

impl SqliteColumn {
    #[doc(hidden)]
    pub fn from_introspection(
        name: String,
        ty: ColumnType,
        nullable: bool,
        default: Option<ColumnDefault>,
    ) -> Self {
        Self {
            name,
            ty,
            nullable,
            default,
        }
    }

    #[doc(hidden)]
    pub fn set_nullable_from_introspection(&mut self, nullable: bool) {
        self.nullable = nullable;
    }

    #[doc(hidden)]
    pub fn set_default_from_introspection(&mut self, default: Option<ColumnDefault>) {
        self.default = default;
    }

    fn from_field(model_name: &str, field: &Field) -> Self {
        let default = field
            .attributes()
            .iter()
            .find_map(|attribute| match attribute {
                Attribute::Default(default) => Some(match default.function() {
                    DefaultFunction::Autoincrement => ColumnDefault::Autoincrement,
                    DefaultFunction::Now => ColumnDefault::CurrentTimestamp,
                    DefaultFunction::Other(other) => ColumnDefault::Raw(other.clone()),
                }),
                _ => None,
            });

        let ty = match field.ty() {
            FieldType::Scalar(scalar) => ColumnType::from_scalar_type(scalar.scalar()),
            FieldType::Relation { .. } => {
                panic!(
                    "relation field `{}.{}` cannot map to a SQLite column",
                    model_name,
                    field.name()
                )
            }
        };

        Self {
            name: field.name().to_owned(),
            ty,
            nullable: field.ty().is_optional(),
            default,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn column_type(&self) -> ColumnType {
        self.ty.clone()
    }

    pub fn nullable(&self) -> bool {
        self.nullable
    }

    pub fn default(&self) -> Option<&ColumnDefault> {
        self.default.as_ref()
    }

    fn is_fast_additive(&self) -> bool {
        self.nullable && self.default.is_none()
    }

    fn render_column_definition(&self, inline_primary_key: bool) -> String {
        let mut rendered = format!("{} {}", quoted_identifier(&self.name), self.ty.render());

        if !self.nullable {
            rendered.push_str(" NOT NULL");
        }

        if inline_primary_key {
            rendered.push_str(" PRIMARY KEY");

            if matches!(self.default, Some(ColumnDefault::Autoincrement)) {
                rendered.push_str(" AUTOINCREMENT");
            }
        }

        if let Some(default) = &self.default
            && !matches!(default, ColumnDefault::Autoincrement)
        {
            rendered.push_str(" DEFAULT ");
            rendered.push_str(default.render());
        }

        rendered
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ColumnType {
    Integer,
    BigInt,
    Text,
    Boolean,
    DateTime,
    Real,
    Blob,
    JsonB,
    #[doc(hidden)]
    Raw(String),
}

impl ColumnType {
    fn from_scalar_type(scalar: ScalarType) -> Self {
        match scalar {
            ScalarType::Int => Self::Integer,
            ScalarType::BigInt => Self::BigInt,
            ScalarType::String => Self::Text,
            ScalarType::Boolean => Self::Boolean,
            ScalarType::DateTime => Self::DateTime,
            ScalarType::Float => Self::Real,
            ScalarType::Bytes => Self::Blob,
            ScalarType::Json => Self::JsonB,
            ScalarType::Decimal => {
                unreachable!("validated SQLite schemas cannot contain Decimal fields")
            }
        }
    }

    #[doc(hidden)]
    pub fn from_introspection(declared_type: &str) -> Self {
        let normalized = declared_type.trim().to_ascii_uppercase();
        let base_type = normalized
            .split_once('(')
            .map_or(normalized.as_str(), |(base, _)| base)
            .trim();

        match base_type {
            "INTEGER" | "INT" => Self::Integer,
            "BIGINT" | "INT8" | "UNSIGNED BIG INT" => Self::BigInt,
            "TEXT" | "CHAR" | "CHARACTER" | "VARCHAR" | "NCHAR" | "NVARCHAR" | "CLOB" => Self::Text,
            "BOOLEAN" | "BOOL" => Self::Boolean,
            "DATETIME" | "TIMESTAMP" => Self::DateTime,
            "REAL" | "DOUBLE" | "DOUBLE PRECISION" | "FLOAT" => Self::Real,
            "BLOB" => Self::Blob,
            "JSON" | "JSONB" => Self::JsonB,
            _ => Self::Raw(declared_type.to_owned()),
        }
    }

    fn render(&self) -> &str {
        match self {
            Self::Integer => "INTEGER",
            Self::BigInt => "BIGINT",
            Self::Text => "TEXT",
            Self::Boolean => "BOOLEAN",
            Self::DateTime => "DATETIME",
            Self::Real => "REAL",
            Self::Blob => "BLOB",
            Self::JsonB => "JSONB",
            Self::Raw(raw) => raw,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ColumnDefault {
    Autoincrement,
    CurrentTimestamp,
    Raw(String),
}

impl ColumnDefault {
    #[doc(hidden)]
    pub fn from_introspection(default: Option<String>) -> Option<Self> {
        match default {
            Some(raw) if is_current_timestamp_default(&raw) => Some(Self::CurrentTimestamp),
            Some(raw) => Some(Self::Raw(raw)),
            None => None,
        }
    }

    fn render(&self) -> &str {
        match self {
            Self::Autoincrement => {
                unreachable!("autoincrement defaults are rendered with inline primary keys")
            }
            Self::CurrentTimestamp => "CURRENT_TIMESTAMP",
            Self::Raw(raw) => raw,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlitePrimaryKey {
    name: String,
    columns: Vec<String>,
}

impl SqlitePrimaryKey {
    #[doc(hidden)]
    pub fn from_introspection(name: String, columns: Vec<String>) -> Self {
        Self { name, columns }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct SqliteIndex {
    name: String,
    columns: Vec<String>,
    unique: bool,
    definition_supported: bool,
}

impl fmt::Debug for SqliteIndex {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteIndex")
            .field("name", &self.name)
            .field("columns", &self.columns)
            .field("unique", &self.unique)
            .finish()
    }
}

impl SqliteIndex {
    #[doc(hidden)]
    pub fn from_introspection(
        name: String,
        columns: Vec<String>,
        unique: bool,
        definition_supported: bool,
    ) -> Self {
        Self {
            name,
            columns,
            unique,
            definition_supported,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn unique(&self) -> bool {
        self.unique
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqliteForeignKey {
    name: String,
    columns: Vec<String>,
    referenced_table: String,
    referenced_columns: Vec<String>,
    on_delete: ForeignKeyAction,
    on_update: ForeignKeyAction,
}

impl SqliteForeignKey {
    #[doc(hidden)]
    pub fn from_introspection(
        name: String,
        columns: Vec<String>,
        referenced_table: String,
        referenced_columns: Vec<String>,
        on_delete: ForeignKeyAction,
        on_update: ForeignKeyAction,
    ) -> Self {
        Self {
            name,
            columns,
            referenced_table,
            referenced_columns,
            on_delete,
            on_update,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn referenced_table(&self) -> &str {
        &self.referenced_table
    }

    pub fn referenced_columns(&self) -> &[String] {
        &self.referenced_columns
    }

    pub fn on_delete(&self) -> ForeignKeyAction {
        self.on_delete
    }

    pub fn on_update(&self) -> ForeignKeyAction {
        self.on_update
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ForeignKeyAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

impl ForeignKeyAction {
    #[doc(hidden)]
    pub fn from_introspection(action: &str) -> Result<Self, CompileError> {
        match action.to_ascii_uppercase().as_str() {
            "NO ACTION" => Ok(Self::NoAction),
            "RESTRICT" => Ok(Self::Restrict),
            "CASCADE" => Ok(Self::Cascade),
            "SET NULL" => Ok(Self::SetNull),
            "SET DEFAULT" => Ok(Self::SetDefault),
            other => Err(CompileError::new(format!(
                "unsupported SQLite foreign-key action `{other}`"
            ))),
        }
    }

    fn render(self) -> &'static str {
        match self {
            Self::NoAction => "NO ACTION",
            Self::Restrict => "RESTRICT",
            Self::Cascade => "CASCADE",
            Self::SetNull => "SET NULL",
            Self::SetDefault => "SET DEFAULT",
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SqliteMigration {
    steps: Vec<MigrationStep>,
}

impl SqliteMigration {
    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }

    pub fn to_sql(&self) -> String {
        self.render(SqliteFamilyFlavor::Native)
    }

    #[doc(hidden)]
    pub fn to_d1_sql(&self) -> String {
        self.render(SqliteFamilyFlavor::D1)
    }

    fn render(&self, flavor: SqliteFamilyFlavor) -> String {
        if self.steps.is_empty() {
            return String::new();
        }

        let rendered = self
            .steps
            .iter()
            .map(|step| step.render(flavor))
            .collect::<Vec<_>>()
            .join("\n\n");

        if flavor == SqliteFamilyFlavor::D1
            && self
                .steps
                .iter()
                .any(MigrationStep::requires_deferred_foreign_keys)
        {
            format!(
                "PRAGMA defer_foreign_keys=ON;\n\n{rendered}\n\nPRAGMA defer_foreign_keys=OFF;\n\n"
            )
        } else {
            format!("{rendered}\n\n")
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum MigrationStep {
    DropIndex {
        name: String,
    },
    AlterTable {
        table: String,
        columns: Vec<SqliteColumn>,
    },
    DropTable {
        name: String,
    },
    CreateTable {
        table: SqliteTable,
    },
    RedefineTables {
        tables: Vec<RedefinedTable>,
    },
    CreateIndex {
        table: String,
        index: SqliteIndex,
    },
}

impl MigrationStep {
    fn requires_deferred_foreign_keys(&self) -> bool {
        matches!(self, Self::DropTable { .. } | Self::RedefineTables { .. })
    }

    fn render(&self, flavor: SqliteFamilyFlavor) -> String {
        match self {
            Self::DropIndex { name } => {
                format!("-- DropIndex\nDROP INDEX {};", quoted_identifier(name))
            }
            Self::AlterTable { table, columns } => {
                let statements = columns
                    .iter()
                    .map(|column| {
                        format!(
                            "ALTER TABLE {} ADD COLUMN {};",
                            quoted_identifier(table),
                            column.render_column_definition(false)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");

                format!("-- AlterTable\n{statements}")
            }
            Self::DropTable { name } => match flavor {
                SqliteFamilyFlavor::Native => format!(
                    "-- DropTable\nPRAGMA foreign_keys=off;\nDROP TABLE {};\nPRAGMA foreign_keys=on;",
                    quoted_identifier(name)
                ),
                SqliteFamilyFlavor::D1 => {
                    format!("-- DropTable\nDROP TABLE {};", quoted_identifier(name))
                }
            },
            Self::CreateTable { table } => {
                format!(
                    "-- CreateTable\n{}",
                    render_create_table(table, &table.name)
                )
            }
            Self::RedefineTables { tables } => {
                let mut lines = vec!["-- RedefineTables".to_owned()];

                if flavor == SqliteFamilyFlavor::Native {
                    lines.push("PRAGMA defer_foreign_keys=ON;".to_owned());
                    lines.push("PRAGMA foreign_keys=OFF;".to_owned());
                }

                for redefined in tables {
                    let replacement_name = format!("new_{}", redefined.table.name);
                    lines.push(render_create_table(&redefined.table, &replacement_name));

                    if !redefined.copied_columns.is_empty() {
                        let columns = render_identifier_list(&redefined.copied_columns);
                        lines.push(format!(
                            "INSERT INTO {} ({columns}) SELECT {columns} FROM {};",
                            quoted_identifier(&replacement_name),
                            quoted_identifier(&redefined.table.name)
                        ));
                    }

                    lines.push(format!(
                        "DROP TABLE {};",
                        quoted_identifier(&redefined.table.name)
                    ));
                    lines.push(format!(
                        "ALTER TABLE {} RENAME TO {};",
                        quoted_identifier(&replacement_name),
                        quoted_identifier(&redefined.table.name)
                    ));

                    for index in &redefined.table.indexes {
                        lines.push(render_create_index(&redefined.table.name, index));
                    }
                }

                if flavor == SqliteFamilyFlavor::Native {
                    lines.push("PRAGMA foreign_keys=ON;".to_owned());
                    lines.push("PRAGMA defer_foreign_keys=OFF;".to_owned());
                }

                lines.join("\n")
            }
            Self::CreateIndex { table, index } => {
                format!("-- CreateIndex\n{}", render_create_index(table, index))
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RedefinedTable {
    table: SqliteTable,
    copied_columns: Vec<String>,
}

enum TableChange {
    None,
    AddColumns(Vec<SqliteColumn>),
    Redefine,
}

fn classify_table_change(current: &SqliteTable, target: &SqliteTable) -> TableChange {
    let current_columns = current
        .columns
        .iter()
        .map(|column| (column.name.as_str(), column))
        .collect::<HashMap<_, _>>();
    let target_columns = target
        .columns
        .iter()
        .map(|column| (column.name.as_str(), column))
        .collect::<HashMap<_, _>>();

    let retained_columns_unchanged = current.columns.iter().all(|current_column| {
        target_columns
            .get(current_column.name.as_str())
            .is_some_and(|target_column| *target_column == current_column)
    });

    let mut added_columns = target
        .columns
        .iter()
        .filter(|target_column| !current_columns.contains_key(target_column.name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    added_columns.sort_by(|left, right| left.name.cmp(&right.name));

    let primary_key_unchanged = current.primary_key.columns == target.primary_key.columns;
    let foreign_keys_unchanged = foreign_keys_equal(current, target);

    if retained_columns_unchanged
        && added_columns.is_empty()
        && primary_key_unchanged
        && foreign_keys_unchanged
    {
        return TableChange::None;
    }

    if retained_columns_unchanged
        && !added_columns.is_empty()
        && added_columns.iter().all(SqliteColumn::is_fast_additive)
        && primary_key_unchanged
        && foreign_keys_unchanged
    {
        return TableChange::AddColumns(added_columns);
    }

    TableChange::Redefine
}

fn foreign_keys_equal(current: &SqliteTable, target: &SqliteTable) -> bool {
    current.foreign_keys.len() == target.foreign_keys.len()
        && current.foreign_keys.iter().all(|current_foreign_key| {
            target
                .foreign_key_named(&current_foreign_key.name)
                .is_some_and(|target_foreign_key| target_foreign_key == current_foreign_key)
        })
        && target.foreign_keys.iter().all(|target_foreign_key| {
            current
                .foreign_key_named(&target_foreign_key.name)
                .is_some_and(|current_foreign_key| current_foreign_key == target_foreign_key)
        })
}

fn render_create_table(table: &SqliteTable, rendered_name: &str) -> String {
    let inline_primary_key =
        (table.primary_key.columns.len() == 1).then(|| table.primary_key.columns[0].as_str());
    let has_compound_primary_key = table.primary_key.columns.len() > 1;
    let constraint_count = usize::from(has_compound_primary_key) + table.foreign_keys.len();

    let mut lines = Vec::new();

    for (index, column) in table.columns.iter().enumerate() {
        let is_inline_primary_key = inline_primary_key == Some(column.name.as_str());
        let has_following_column = index + 1 < table.columns.len();
        let has_following_constraint = constraint_count > 0;
        let comma = if has_following_column || has_following_constraint {
            ","
        } else {
            ""
        };

        lines.push(format!(
            "    {}{comma}",
            column.render_column_definition(is_inline_primary_key)
        ));
    }

    let mut constraints = Vec::with_capacity(constraint_count);

    if has_compound_primary_key {
        constraints.push(format!(
            "PRIMARY KEY ({})",
            render_identifier_list(&table.primary_key.columns)
        ));
    }

    constraints.extend(table.foreign_keys.iter().map(render_foreign_key));

    if has_compound_primary_key {
        lines.push(String::new());
    }

    for (index, constraint) in constraints.iter().enumerate() {
        let comma = if index + 1 < constraints.len() {
            ","
        } else {
            ""
        };
        lines.push(format!("    {constraint}{comma}"));
    }

    format!(
        "CREATE TABLE {} (\n{}\n);",
        quoted_identifier(rendered_name),
        lines.join("\n")
    )
}

fn render_foreign_key(foreign_key: &SqliteForeignKey) -> String {
    format!(
        "CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({}) ON DELETE {} ON UPDATE {}",
        quoted_identifier(&foreign_key.name),
        render_identifier_list(&foreign_key.columns),
        quoted_identifier(&foreign_key.referenced_table),
        render_identifier_list(&foreign_key.referenced_columns),
        foreign_key.on_delete.render(),
        foreign_key.on_update.render(),
    )
}

fn render_create_index(table: &str, index: &SqliteIndex) -> String {
    format!(
        "CREATE {}INDEX {} ON {}({});",
        if index.unique { "UNIQUE " } else { "" },
        quoted_identifier(&index.name),
        quoted_identifier(table),
        render_identifier_list(&index.columns)
    )
}

fn render_identifier_list(columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| quoted_identifier(column))
        .collect::<Vec<_>>()
        .join(", ")
}

fn quoted_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn is_current_timestamp_default(default: &str) -> bool {
    let mut normalized = default.trim();

    while normalized.len() >= 2 && normalized.starts_with('(') && normalized.ends_with(')') {
        normalized = normalized[1..normalized.len() - 1].trim();
    }

    normalized.eq_ignore_ascii_case("CURRENT_TIMESTAMP")
}
