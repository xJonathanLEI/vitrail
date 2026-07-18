use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

use sqlx::sqlite::SqliteConnection;
use sqlx::{Connection as _, Row as _};

use crate::{
    Attribute, DefaultFunction, Field, FieldType, Resolution, ScalarType, Schema, SchemaAccess,
};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SqliteSchema {
    tables: Vec<SqliteTable>,
}

impl SqliteSchema {
    pub fn empty() -> Self {
        Self::default()
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

    pub async fn introspect(database_url: &str) -> Result<Self, sqlx::Error> {
        Self::introspect_ignoring(database_url, &[]).await
    }

    pub async fn introspect_ignoring(
        database_url: &str,
        ignored_tables: &[String],
    ) -> Result<Self, sqlx::Error> {
        let mut connection = SqliteConnection::connect(database_url).await?;

        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(&mut connection)
            .await?;

        Self::introspect_from_connection(&mut connection, ignored_tables).await
    }

    pub async fn introspect_ignoring_external_tables<S>(
        database_url: &str,
    ) -> Result<Self, sqlx::Error>
    where
        S: SchemaAccess,
    {
        Self::introspect_ignoring(database_url, S::schema().external_tables()).await
    }

    async fn introspect_from_connection(
        connection: &mut SqliteConnection,
        ignored_tables: &[String],
    ) -> Result<Self, sqlx::Error> {
        // SQLite identifiers use ASCII case-insensitive comparison.
        let ignored_tables = ignored_tables
            .iter()
            .map(|table| table.to_ascii_lowercase())
            .collect::<HashSet<_>>();

        let table_rows = sqlx::query(
            r#"
            SELECT name, sql
            FROM sqlite_master
            WHERE type = 'table'
              AND lower(name) <> '_vitrail_migrations'
              AND substr(lower(name), 1, 7) <> 'sqlite_'
            ORDER BY name
            "#,
        )
        .fetch_all(&mut *connection)
        .await?;

        let mut tables = Vec::new();
        let mut primary_key_columns_by_table = HashMap::<String, Vec<String>>::new();

        for table_row in table_rows {
            let table_name = table_row.try_get::<String, _>("name")?;

            if ignored_tables.contains(&table_name.to_ascii_lowercase()) {
                continue;
            }

            let create_table_sql = table_row.try_get::<Option<String>, _>("sql")?;
            let column_rows = sqlx::query(
                r#"
                SELECT
                    cid,
                    name,
                    type AS declared_type,
                    "notnull" AS not_null,
                    dflt_value,
                    pk AS primary_key_ordinal
                FROM pragma_table_info(?1)
                ORDER BY cid
                "#,
            )
            .bind(&table_name)
            .fetch_all(&mut *connection)
            .await?;

            let mut columns = Vec::with_capacity(column_rows.len());
            let mut primary_key_columns = Vec::new();
            let mut integer_primary_key_column = None;

            for column_row in column_rows {
                let name = column_row.try_get::<String, _>("name")?;
                let declared_type = column_row.try_get::<String, _>("declared_type")?;
                let primary_key_ordinal = column_row.try_get::<i64, _>("primary_key_ordinal")?;

                if primary_key_ordinal > 0 {
                    primary_key_columns.push((primary_key_ordinal, name.clone()));

                    if declared_type.trim().eq_ignore_ascii_case("INTEGER") {
                        integer_primary_key_column = Some(name.clone());
                    }
                }

                columns.push(SqliteColumn {
                    name,
                    ty: ColumnType::from_introspection(&declared_type),
                    nullable: column_row.try_get::<i64, _>("not_null")? == 0,
                    default: ColumnDefault::from_introspection(
                        column_row.try_get::<Option<String>, _>("dflt_value")?,
                    ),
                });
            }

            primary_key_columns.sort_by_key(|(ordinal, _)| *ordinal);
            let primary_key_columns = primary_key_columns
                .into_iter()
                .map(|(_, column)| column)
                .collect::<Vec<_>>();

            primary_key_columns_by_table.insert(table_name.clone(), primary_key_columns.clone());

            if let [primary_key_column] = primary_key_columns.as_slice()
                && create_table_sql
                    .as_deref()
                    .is_some_and(|sql| contains_sql_keyword(sql, "AUTOINCREMENT"))
                && let Some(column) = columns
                    .iter_mut()
                    .find(|column| column.name == primary_key_column.as_str())
            {
                column.default = Some(ColumnDefault::Autoincrement);
            }

            let index_rows = sqlx::query(
                r#"
                SELECT
                    seq,
                    name,
                    "unique" AS is_unique,
                    origin,
                    partial
                FROM pragma_index_list(?1)
                ORDER BY seq
                "#,
            )
            .bind(&table_name)
            .fetch_all(&mut *connection)
            .await?;

            let mut indexes = Vec::new();
            let mut has_primary_key_index = false;

            for index_row in index_rows {
                let index_name = index_row.try_get::<String, _>("name")?;
                let origin = index_row.try_get::<String, _>("origin")?;

                if origin == "pk" {
                    has_primary_key_index = true;
                    continue;
                }

                if origin != "c" || index_name.starts_with("sqlite_autoindex_") {
                    continue;
                }

                let index_column_rows = sqlx::query(
                    r#"
                    SELECT seqno, name AS column_name
                    FROM pragma_index_info(?1)
                    ORDER BY seqno
                    "#,
                )
                .bind(&index_name)
                .fetch_all(&mut *connection)
                .await?;

                // `pragma_index_info` omits sort direction, so inspect the
                // extended metadata before treating the index as supported.
                let descending_column_count = sqlx::query_scalar::<_, i64>(
                    r#"
                    SELECT COUNT(*)
                    FROM pragma_index_xinfo(?1)
                    WHERE "key" = 1
                      AND "desc" <> 0
                    "#,
                )
                .bind(&index_name)
                .fetch_one(&mut *connection)
                .await?;

                let mut index_columns = Vec::with_capacity(index_column_rows.len());
                let mut definition_supported =
                    index_row.try_get::<i64, _>("partial")? == 0 && descending_column_count == 0;

                for index_column_row in index_column_rows {
                    if let Some(column_name) =
                        index_column_row.try_get::<Option<String>, _>("column_name")?
                    {
                        index_columns.push(column_name);
                    } else {
                        definition_supported = false;
                    }
                }

                indexes.push(SqliteIndex {
                    name: index_name,
                    columns: index_columns,
                    unique: index_row.try_get::<i64, _>("is_unique")? != 0,
                    definition_supported,
                });
            }

            // A sole INTEGER primary key aliases rowid unless SQLite created a
            // separate primary-key index, as it does for `INTEGER PRIMARY KEY DESC`.
            if !has_primary_key_index
                && let [primary_key_column] = primary_key_columns.as_slice()
                && integer_primary_key_column.as_deref() == Some(primary_key_column.as_str())
                && let Some(column) = columns
                    .iter_mut()
                    .find(|column| column.name == primary_key_column.as_str())
            {
                column.nullable = false;
            }

            // SQLite returns indexes in reverse creation order. Keep the internal
            // representation in creation order so migration planning can reverse it
            // again when rendering Prisma-compatible index drops.
            indexes.reverse();

            let foreign_key_rows = sqlx::query(
                r#"
                SELECT
                    id,
                    seq,
                    "table" AS referenced_table,
                    "from" AS local_column,
                    "to" AS referenced_column,
                    on_update,
                    on_delete
                FROM pragma_foreign_key_list(?1)
                ORDER BY id, seq
                "#,
            )
            .bind(&table_name)
            .fetch_all(&mut *connection)
            .await?;

            let mut pending_foreign_keys = BTreeMap::<i64, PendingForeignKey>::new();

            for foreign_key_row in foreign_key_rows {
                let id = foreign_key_row.try_get::<i64, _>("id")?;
                let sequence = foreign_key_row.try_get::<i64, _>("seq")?;
                let local_column = foreign_key_row.try_get::<String, _>("local_column")?;
                let referenced_table = foreign_key_row.try_get::<String, _>("referenced_table")?;
                let referenced_column = match foreign_key_row
                    .try_get::<Option<String>, _>("referenced_column")?
                {
                    Some(referenced_column) => referenced_column,
                    None => {
                        if !primary_key_columns_by_table.contains_key(&referenced_table) {
                            let columns =
                                introspect_primary_key_columns(connection, &referenced_table)
                                    .await?;
                            primary_key_columns_by_table.insert(referenced_table.clone(), columns);
                        }

                        let sequence = usize::try_from(sequence).map_err(|_| {
                            invalid_introspection_data(format!(
                                "foreign key {id} on table `{table_name}` has invalid sequence {sequence}"
                            ))
                        })?;

                        primary_key_columns_by_table
                            .get(&referenced_table)
                            .and_then(|columns| columns.get(sequence))
                            .cloned()
                            .ok_or_else(|| {
                                invalid_introspection_data(format!(
                                    "foreign key {id} on table `{table_name}` omits referenced columns, but table `{referenced_table}` has no primary-key column at position {sequence}"
                                ))
                            })?
                    }
                };

                if let Some(foreign_key) = pending_foreign_keys.get_mut(&id) {
                    foreign_key.columns.push(local_column);
                    foreign_key.referenced_columns.push(referenced_column);
                } else {
                    pending_foreign_keys.insert(
                        id,
                        PendingForeignKey {
                            columns: vec![local_column],
                            referenced_table,
                            referenced_columns: vec![referenced_column],
                            on_delete: ForeignKeyAction::from_introspection(
                                &foreign_key_row.try_get::<String, _>("on_delete")?,
                            )?,
                            on_update: ForeignKeyAction::from_introspection(
                                &foreign_key_row.try_get::<String, _>("on_update")?,
                            )?,
                        },
                    );
                }
            }

            let mut foreign_keys = pending_foreign_keys
                .into_values()
                .map(|foreign_key| SqliteForeignKey {
                    name: format!("{}_{}_fkey", table_name, foreign_key.columns.join("_")),
                    columns: foreign_key.columns,
                    referenced_table: foreign_key.referenced_table,
                    referenced_columns: foreign_key.referenced_columns,
                    on_delete: foreign_key.on_delete,
                    on_update: foreign_key.on_update,
                })
                .collect::<Vec<_>>();

            foreign_keys.sort_by(|left, right| left.name.cmp(&right.name));

            tables.push(SqliteTable {
                name: table_name.clone(),
                columns,
                primary_key: SqlitePrimaryKey {
                    name: format!("{table_name}_pkey"),
                    columns: primary_key_columns,
                },
                indexes,
                foreign_keys,
            });
        }

        Ok(Self { tables })
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

    fn from_introspection(declared_type: &str) -> Self {
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
    fn from_introspection(default: Option<String>) -> Option<Self> {
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
    fn from_introspection(action: &str) -> Result<Self, sqlx::Error> {
        match action.to_ascii_uppercase().as_str() {
            "NO ACTION" => Ok(Self::NoAction),
            "RESTRICT" => Ok(Self::Restrict),
            "CASCADE" => Ok(Self::Cascade),
            "SET NULL" => Ok(Self::SetNull),
            "SET DEFAULT" => Ok(Self::SetDefault),
            other => Err(invalid_introspection_data(format!(
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
        if self.steps.is_empty() {
            return String::new();
        }

        let rendered = self
            .steps
            .iter()
            .map(MigrationStep::render)
            .collect::<Vec<_>>()
            .join("\n\n");

        format!("{rendered}\n\n")
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
    fn render(&self) -> String {
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
            Self::DropTable { name } => format!(
                "-- DropTable\nPRAGMA foreign_keys=off;\nDROP TABLE {};\nPRAGMA foreign_keys=on;",
                quoted_identifier(name)
            ),
            Self::CreateTable { table } => {
                format!(
                    "-- CreateTable\n{}",
                    render_create_table(table, &table.name)
                )
            }
            Self::RedefineTables { tables } => {
                let mut lines = vec![
                    "-- RedefineTables".to_owned(),
                    "PRAGMA defer_foreign_keys=ON;".to_owned(),
                    "PRAGMA foreign_keys=OFF;".to_owned(),
                ];

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

                lines.push("PRAGMA foreign_keys=ON;".to_owned());
                lines.push("PRAGMA defer_foreign_keys=OFF;".to_owned());
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

struct PendingForeignKey {
    columns: Vec<String>,
    referenced_table: String,
    referenced_columns: Vec<String>,
    on_delete: ForeignKeyAction,
    on_update: ForeignKeyAction,
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

async fn introspect_primary_key_columns(
    connection: &mut SqliteConnection,
    table_name: &str,
) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar::<_, String>(
        r#"
        SELECT name
        FROM pragma_table_info(?1)
        WHERE pk > 0
        ORDER BY pk
        "#,
    )
    .bind(table_name)
    .fetch_all(&mut *connection)
    .await
}

fn contains_sql_keyword(sql: &str, keyword: &str) -> bool {
    let mut characters = sql.chars().peekable();
    let mut token = String::new();

    while let Some(character) = characters.next() {
        match character {
            '\'' | '"' | '`' => {
                if take_sql_keyword(&mut token, keyword) {
                    return true;
                }

                skip_quoted_sql(&mut characters, character);
            }
            '[' => {
                if take_sql_keyword(&mut token, keyword) {
                    return true;
                }

                skip_bracketed_sql_identifier(&mut characters);
            }
            '-' if characters.peek().copied() == Some('-') => {
                if take_sql_keyword(&mut token, keyword) {
                    return true;
                }

                characters.next();

                for comment_character in characters.by_ref() {
                    if matches!(comment_character, '\n' | '\r') {
                        break;
                    }
                }
            }
            '/' if characters.peek().copied() == Some('*') => {
                if take_sql_keyword(&mut token, keyword) {
                    return true;
                }

                characters.next();

                while let Some(comment_character) = characters.next() {
                    if comment_character == '*' && characters.peek().copied() == Some('/') {
                        characters.next();
                        break;
                    }
                }
            }
            character if character.is_ascii_alphanumeric() || character == '_' => {
                token.push(character);
            }
            _ => {
                if take_sql_keyword(&mut token, keyword) {
                    return true;
                }
            }
        }
    }

    take_sql_keyword(&mut token, keyword)
}

fn take_sql_keyword(token: &mut String, keyword: &str) -> bool {
    let matches = token.eq_ignore_ascii_case(keyword);
    token.clear();
    matches
}

fn skip_quoted_sql(characters: &mut std::iter::Peekable<std::str::Chars<'_>>, delimiter: char) {
    while let Some(character) = characters.next() {
        if character != delimiter {
            continue;
        }

        if characters.peek().copied() == Some(delimiter) {
            characters.next();
        } else {
            break;
        }
    }
}

fn skip_bracketed_sql_identifier(characters: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    while let Some(character) = characters.next() {
        if character != ']' {
            continue;
        }

        if characters.peek().copied() == Some(']') {
            characters.next();
        } else {
            break;
        }
    }
}

fn is_current_timestamp_default(default: &str) -> bool {
    let mut normalized = default.trim();

    while normalized.len() >= 2 && normalized.starts_with('(') && normalized.ends_with(')') {
        normalized = normalized[1..normalized.len() - 1].trim();
    }

    normalized.eq_ignore_ascii_case("CURRENT_TIMESTAMP")
}

fn invalid_introspection_data(message: impl Into<String>) -> sqlx::Error {
    sqlx::Error::Decode(Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message.into(),
    )))
}
