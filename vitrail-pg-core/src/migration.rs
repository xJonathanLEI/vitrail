use std::collections::HashMap;

use sqlx::Row as _;
use sqlx::postgres::PgPoolOptions;

use crate::{Attribute, DefaultFunction, Field, FieldType, ScalarType, Schema, SchemaAccess};

#[derive(Clone, Debug, Default)]
pub struct PostgresSchema {
    tables: Vec<PostgresTable>,
}

impl PostgresSchema {
    pub fn empty() -> Self {
        Self { tables: Vec::new() }
    }

    pub fn from_schema(schema: &Schema) -> Self {
        let mut tables = Vec::with_capacity(schema.models().len());

        for model in schema.models() {
            let mut columns = Vec::new();
            let mut indexes = Vec::new();
            let mut foreign_keys = Vec::new();

            let primary_key = PostgresPrimaryKey {
                name: format!("{}_pkey", model.name()),
                columns: model
                    .primary_key_columns()
                    .into_iter()
                    .map(str::to_owned)
                    .collect(),
            };

            for field in model.fields() {
                if field.kind().is_scalar() {
                    columns.push(PostgresColumn::from_field(model.name(), field));

                    if field
                        .attributes()
                        .iter()
                        .any(|attribute| matches!(attribute, Attribute::Unique))
                    {
                        indexes.push(PostgresIndex {
                            name: format!("{}_{}_key", model.name(), field.name()),
                            columns: vec![field.name().to_owned()],
                            unique: true,
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

                foreign_keys.push(PostgresForeignKey {
                    name: format!("{}_{}_fkey", model.name(), relation.fields().join("_")),
                    columns: relation.fields().to_vec(),
                    referenced_table: field.ty().name().to_owned(),
                    referenced_columns: relation.references().to_vec(),
                    on_delete: ForeignKeyAction::Restrict,
                    on_update: ForeignKeyAction::Cascade,
                });
            }

            for unique_columns in model.unique_column_sets() {
                indexes.push(PostgresIndex {
                    name: format!("{}_{}_key", model.name(), unique_columns.join("_")),
                    columns: unique_columns.into_iter().map(str::to_owned).collect(),
                    unique: true,
                });
            }

            tables.push(PostgresTable {
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
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(database_url)
            .await?;
        let schema = Self::introspect_from_pool(&pool).await?;
        pool.close().await;
        Ok(schema)
    }

    async fn introspect_from_pool(pool: &sqlx::postgres::PgPool) -> Result<Self, sqlx::Error> {
        let table_rows = sqlx::query(
            r#"
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE'
              AND table_name <> '_vitrail_migrations'
            ORDER BY table_name
            "#,
        )
        .fetch_all(pool)
        .await?;

        let mut tables = table_rows
            .into_iter()
            .map(|row| PostgresTable {
                name: row.get::<String, _>("table_name"),
                columns: Vec::new(),
                primary_key: PostgresPrimaryKey {
                    name: String::new(),
                    columns: Vec::new(),
                },
                indexes: Vec::new(),
                foreign_keys: Vec::new(),
            })
            .collect::<Vec<_>>();

        let mut table_indexes = HashMap::new();
        for (index, table) in tables.iter().enumerate() {
            table_indexes.insert(table.name.clone(), index);
        }

        let column_rows = sqlx::query(
            r#"
            SELECT
                table_name,
                column_name,
                is_nullable,
                udt_name,
                column_default,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name <> '_vitrail_migrations'
            ORDER BY table_name, ordinal_position
            "#,
        )
        .fetch_all(pool)
        .await?;

        for row in column_rows {
            let table_name = row.get::<String, _>("table_name");
            let table_index = table_indexes[&table_name];
            let column_name = row.get::<String, _>("column_name");
            let is_nullable = row.get::<String, _>("is_nullable") == "YES";
            let udt_name = row.get::<String, _>("udt_name");
            let column_default = row.get::<Option<String>, _>("column_default");

            tables[table_index].columns.push(PostgresColumn {
                name: column_name,
                ty: ColumnType::from_introspection(&udt_name, column_default.as_deref()),
                nullable: is_nullable,
                default: ColumnDefault::from_introspection(column_default.as_deref()),
            });
        }

        let primary_key_rows = sqlx::query(
            r#"
            SELECT
                tc.table_name,
                tc.constraint_name,
                kcu.column_name,
                kcu.ordinal_position
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = 'public'
              AND tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_name <> '_vitrail_migrations'
            ORDER BY tc.table_name, kcu.ordinal_position
            "#,
        )
        .fetch_all(pool)
        .await?;

        for row in primary_key_rows {
            let table_name = row.get::<String, _>("table_name");
            let table_index = table_indexes[&table_name];
            let constraint_name = row.get::<String, _>("constraint_name");
            let column_name = row.get::<String, _>("column_name");

            if tables[table_index].primary_key.name.is_empty() {
                tables[table_index].primary_key.name = constraint_name;
            }
            tables[table_index].primary_key.columns.push(column_name);
        }

        let index_rows = sqlx::query(
            r#"
            SELECT
                tbl.relname AS table_name,
                idx.relname AS index_name,
                ind.indisunique AS is_unique,
                ARRAY_AGG(att.attname ORDER BY key.ord) AS columns
            FROM pg_class AS tbl
            JOIN pg_namespace AS ns
              ON ns.oid = tbl.relnamespace
            JOIN pg_index AS ind
              ON ind.indrelid = tbl.oid
            JOIN pg_class AS idx
              ON idx.oid = ind.indexrelid
            JOIN LATERAL UNNEST(ind.indkey) WITH ORDINALITY AS key(attnum, ord)
              ON TRUE
            JOIN pg_attribute AS att
              ON att.attrelid = tbl.oid
             AND att.attnum = key.attnum
            WHERE ns.nspname = 'public'
              AND NOT ind.indisprimary
              AND tbl.relname <> '_vitrail_migrations'
            GROUP BY tbl.relname, idx.relname, ind.indisunique
            ORDER BY tbl.relname, idx.relname
            "#,
        )
        .fetch_all(pool)
        .await?;

        for row in index_rows {
            let table_name = row.get::<String, _>("table_name");
            let table_index = table_indexes[&table_name];
            tables[table_index].indexes.push(PostgresIndex {
                name: row.get::<String, _>("index_name"),
                unique: row.get::<bool, _>("is_unique"),
                columns: row.get::<Vec<String>, _>("columns"),
            });
        }

        let foreign_key_rows = sqlx::query(
            r#"
            SELECT
                src.relname AS table_name,
                con.conname AS constraint_name,
                ARRAY_AGG(src_att.attname ORDER BY ord.n) AS columns,
                dst.relname AS referenced_table,
                ARRAY_AGG(dst_att.attname ORDER BY ord.n) AS referenced_columns,
                con.confdeltype::text AS on_delete,
                con.confupdtype::text AS on_update
            FROM pg_constraint AS con
            JOIN pg_class AS src
              ON src.oid = con.conrelid
            JOIN pg_namespace AS src_ns
              ON src_ns.oid = src.relnamespace
            JOIN pg_class AS dst
              ON dst.oid = con.confrelid
            JOIN LATERAL UNNEST(con.conkey, con.confkey) WITH ORDINALITY AS ord(src_attnum, dst_attnum, n)
              ON TRUE
            JOIN pg_attribute AS src_att
              ON src_att.attrelid = src.oid
             AND src_att.attnum = ord.src_attnum
            JOIN pg_attribute AS dst_att
              ON dst_att.attrelid = dst.oid
             AND dst_att.attnum = ord.dst_attnum
            WHERE src_ns.nspname = 'public'
              AND con.contype = 'f'
              AND src.relname <> '_vitrail_migrations'
              AND dst.relname <> '_vitrail_migrations'
            GROUP BY src.relname, con.conname, dst.relname, con.confdeltype, con.confupdtype
            ORDER BY src.relname, con.conname
            "#,
        )
        .fetch_all(pool)
        .await?;

        for row in foreign_key_rows {
            let table_name = row.get::<String, _>("table_name");
            let table_index = table_indexes[&table_name];
            let on_delete = row.get::<String, _>("on_delete");
            let on_update = row.get::<String, _>("on_update");

            tables[table_index].foreign_keys.push(PostgresForeignKey {
                name: row.get::<String, _>("constraint_name"),
                columns: row.get::<Vec<String>, _>("columns"),
                referenced_table: row.get::<String, _>("referenced_table"),
                referenced_columns: row.get::<Vec<String>, _>("referenced_columns"),
                on_delete: ForeignKeyAction::from_pg_code(&on_delete),
                on_update: ForeignKeyAction::from_pg_code(&on_update),
            });
        }

        Ok(Self { tables })
    }

    pub fn tables(&self) -> &[PostgresTable] {
        &self.tables
    }

    pub fn migrate_from(&self, current: &Self) -> PostgresMigration {
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
        let mut steps = Vec::new();

        for current_table in &current.tables {
            let target_table = target_tables.get(current_table.name.as_str());

            for foreign_key in &current_table.foreign_keys {
                let should_drop = match target_table {
                    Some(target_table) => target_table
                        .foreign_key_named(&foreign_key.name)
                        .is_none_or(|candidate| candidate != foreign_key),
                    None => true,
                };

                if should_drop {
                    steps.push(MigrationStep::DropForeignKey {
                        table: current_table.name.clone(),
                        name: foreign_key.name.clone(),
                    });
                }
            }
        }

        for current_table in &current.tables {
            let Some(target_table) = target_tables.get(current_table.name.as_str()) else {
                continue;
            };

            for index in &current_table.indexes {
                if target_table
                    .index_named(&index.name)
                    .is_none_or(|candidate| candidate != index)
                {
                    steps.push(MigrationStep::DropIndex {
                        name: index.name.clone(),
                    });
                }
            }
        }

        for target_table in &self.tables {
            let Some(current_table) = current_tables.get(target_table.name.as_str()) else {
                continue;
            };

            let current_columns = current_table
                .columns
                .iter()
                .map(|column| (column.name.as_str(), column))
                .collect::<HashMap<_, _>>();
            let target_columns = target_table
                .columns
                .iter()
                .map(|column| (column.name.as_str(), column))
                .collect::<HashMap<_, _>>();
            let mut actions = Vec::new();

            for current_column in &current_table.columns {
                if !target_columns.contains_key(current_column.name.as_str()) {
                    actions.push(AlterTableAction::DropColumn {
                        name: current_column.name.clone(),
                    });
                }
            }

            for target_column in &target_table.columns {
                let Some(current_column) = current_columns.get(target_column.name.as_str()) else {
                    actions.push(AlterTableAction::AddColumn {
                        column: target_column.clone(),
                    });
                    continue;
                };

                if current_column.ty != target_column.ty {
                    actions.push(AlterTableAction::SetColumnType {
                        name: target_column.name.clone(),
                        ty: target_column.ty,
                    });
                }

                if current_column.default != target_column.default {
                    match &target_column.default {
                        Some(default) => actions.push(AlterTableAction::SetDefault {
                            name: target_column.name.clone(),
                            default: default.clone(),
                        }),
                        None => actions.push(AlterTableAction::DropDefault {
                            name: target_column.name.clone(),
                        }),
                    }
                }

                if current_column.nullable != target_column.nullable {
                    if target_column.nullable {
                        actions.push(AlterTableAction::DropNotNull {
                            name: target_column.name.clone(),
                        });
                    } else {
                        actions.push(AlterTableAction::SetNotNull {
                            name: target_column.name.clone(),
                        });
                    }
                }
            }

            if !actions.is_empty() {
                steps.push(MigrationStep::AlterTable {
                    table: target_table.name.clone(),
                    actions,
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

        for current_table in current.tables.iter().rev() {
            if !target_tables.contains_key(current_table.name.as_str()) {
                steps.push(MigrationStep::DropTable {
                    name: current_table.name.clone(),
                });
            }
        }

        for target_table in &self.tables {
            let current_table = current_tables.get(target_table.name.as_str());

            for index in &target_table.indexes {
                let should_create = match current_table {
                    Some(current_table) => current_table
                        .index_named(&index.name)
                        .is_none_or(|candidate| candidate != index),
                    None => true,
                };

                if should_create {
                    steps.push(MigrationStep::CreateIndex {
                        table: target_table.name.clone(),
                        index: index.clone(),
                    });
                }
            }
        }

        for target_table in &self.tables {
            let current_table = current_tables.get(target_table.name.as_str());

            for foreign_key in &target_table.foreign_keys {
                let should_create = match current_table {
                    Some(current_table) => current_table
                        .foreign_key_named(&foreign_key.name)
                        .is_none_or(|candidate| candidate != foreign_key),
                    None => true,
                };

                if should_create {
                    steps.push(MigrationStep::AddForeignKey {
                        table: target_table.name.clone(),
                        foreign_key: foreign_key.clone(),
                    });
                }
            }
        }

        PostgresMigration { steps }
    }
}

impl Schema {
    pub fn to_postgres_schema(&self) -> PostgresSchema {
        PostgresSchema::from_schema(self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostgresTable {
    name: String,
    columns: Vec<PostgresColumn>,
    primary_key: PostgresPrimaryKey,
    indexes: Vec<PostgresIndex>,
    foreign_keys: Vec<PostgresForeignKey>,
}

impl PostgresTable {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> &[PostgresColumn] {
        &self.columns
    }

    pub fn primary_key(&self) -> &PostgresPrimaryKey {
        &self.primary_key
    }

    pub fn indexes(&self) -> &[PostgresIndex] {
        &self.indexes
    }

    pub fn foreign_keys(&self) -> &[PostgresForeignKey] {
        &self.foreign_keys
    }

    fn index_named(&self, name: &str) -> Option<&PostgresIndex> {
        self.indexes.iter().find(|index| index.name == name)
    }

    fn foreign_key_named(&self, name: &str) -> Option<&PostgresForeignKey> {
        self.foreign_keys
            .iter()
            .find(|foreign_key| foreign_key.name == name)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostgresColumn {
    name: String,
    ty: ColumnType,
    nullable: bool,
    default: Option<ColumnDefault>,
}

impl PostgresColumn {
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
            FieldType::Scalar(scalar) => {
                let has_db_uuid = field
                    .attributes()
                    .iter()
                    .any(|attribute| matches!(attribute, Attribute::DbUuid));

                ColumnType::from_scalar_type(scalar.scalar(), has_db_uuid)
            }
            FieldType::Relation { .. } => {
                panic!(
                    "relation field `{}.{}` cannot map to a postgres column",
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
        self.ty
    }

    pub fn nullable(&self) -> bool {
        self.nullable
    }

    pub fn default(&self) -> Option<&ColumnDefault> {
        self.default.as_ref()
    }

    fn render_column_definition(&self) -> String {
        let mut rendered = format!(
            r#""{}" {}"#,
            self.name,
            self.ty.render(self.default.as_ref())
        );

        if !self.nullable {
            rendered.push_str(" NOT NULL");
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColumnType {
    Integer,
    Text,
    Boolean,
    Timestamp3,
    DoublePrecision,
    Numeric,
    Bytea,
    JsonB,
    Uuid,
}

impl ColumnType {
    fn from_scalar_type(scalar: ScalarType, has_db_uuid: bool) -> Self {
        match scalar {
            ScalarType::Int => Self::Integer,
            ScalarType::String if has_db_uuid => Self::Uuid,
            ScalarType::String => Self::Text,
            ScalarType::Boolean => Self::Boolean,
            ScalarType::DateTime => Self::Timestamp3,
            ScalarType::Float => Self::DoublePrecision,
            ScalarType::Decimal => Self::Numeric,
            ScalarType::Bytes => Self::Bytea,
            ScalarType::Json => Self::JsonB,
        }
    }

    fn from_introspection(udt_name: &str, default: Option<&str>) -> Self {
        match udt_name {
            "int4" if default.is_some_and(|value| value.contains("nextval(")) => Self::Integer,
            "int4" => Self::Integer,
            "text" | "varchar" => Self::Text,
            "bool" => Self::Boolean,
            "timestamp" | "timestamptz" => Self::Timestamp3,
            "float8" => Self::DoublePrecision,
            "numeric" => Self::Numeric,
            "bytea" => Self::Bytea,
            "json" | "jsonb" => Self::JsonB,
            "uuid" => Self::Uuid,
            other => panic!("unsupported postgres column type `{other}` during introspection"),
        }
    }

    fn render(self, default: Option<&ColumnDefault>) -> &'static str {
        match (self, default) {
            (Self::Integer, Some(ColumnDefault::Autoincrement)) => "SERIAL",
            (Self::Integer, _) => "INTEGER",
            (Self::Text, _) => "TEXT",
            (Self::Boolean, _) => "BOOLEAN",
            (Self::Timestamp3, _) => "TIMESTAMP(3)",
            (Self::DoublePrecision, _) => "DOUBLE PRECISION",
            (Self::Numeric, _) => "DECIMAL",
            (Self::Bytea, _) => "BYTEA",
            (Self::JsonB, _) => "JSONB",
            (Self::Uuid, _) => "UUID",
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
    fn from_introspection(default: Option<&str>) -> Option<Self> {
        let default = default?;

        if default.contains("nextval(") {
            Some(Self::Autoincrement)
        } else if default.contains("CURRENT_TIMESTAMP") || default.starts_with("now(") {
            Some(Self::CurrentTimestamp)
        } else {
            Some(Self::Raw(default.to_owned()))
        }
    }

    fn render(&self) -> &str {
        match self {
            Self::Autoincrement => unreachable!("autoincrement defaults are rendered via SERIAL"),
            Self::CurrentTimestamp => "CURRENT_TIMESTAMP",
            Self::Raw(raw) => raw,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostgresPrimaryKey {
    name: String,
    columns: Vec<String>,
}

impl PostgresPrimaryKey {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostgresIndex {
    name: String,
    columns: Vec<String>,
    unique: bool,
}

impl PostgresIndex {
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
pub struct PostgresForeignKey {
    name: String,
    columns: Vec<String>,
    referenced_table: String,
    referenced_columns: Vec<String>,
    on_delete: ForeignKeyAction,
    on_update: ForeignKeyAction,
}

impl PostgresForeignKey {
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
    fn from_pg_code(code: &str) -> Self {
        match code {
            "a" => Self::NoAction,
            "r" => Self::Restrict,
            "c" => Self::Cascade,
            "n" => Self::SetNull,
            "d" => Self::SetDefault,
            other => panic!("unsupported foreign key action code `{other}` during introspection"),
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
pub struct PostgresMigration {
    steps: Vec<MigrationStep>,
}

impl PostgresMigration {
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
    DropForeignKey {
        table: String,
        name: String,
    },
    DropIndex {
        name: String,
    },
    AlterTable {
        table: String,
        actions: Vec<AlterTableAction>,
    },
    CreateTable {
        table: PostgresTable,
    },
    DropTable {
        name: String,
    },
    CreateIndex {
        table: String,
        index: PostgresIndex,
    },
    AddForeignKey {
        table: String,
        foreign_key: PostgresForeignKey,
    },
}

impl MigrationStep {
    fn render(&self) -> String {
        match self {
            Self::DropForeignKey { table, name } => {
                format!("-- DropForeignKey\nALTER TABLE \"{table}\" DROP CONSTRAINT \"{name}\";")
            }
            Self::DropIndex { name } => {
                format!("-- DropIndex\nDROP INDEX \"{name}\";")
            }
            Self::AlterTable { table, actions } => {
                let actions = actions
                    .iter()
                    .map(AlterTableAction::render)
                    .collect::<Vec<_>>()
                    .join(",\n");
                format!("-- AlterTable\nALTER TABLE \"{table}\" {actions};")
            }
            Self::CreateTable { table } => {
                let mut lines = table
                    .columns
                    .iter()
                    .map(|column| format!("    {},", column.render_column_definition()))
                    .collect::<Vec<_>>();
                lines.push(String::new());
                lines.push(format!(
                    "    CONSTRAINT \"{}\" PRIMARY KEY ({})",
                    table.primary_key.name,
                    render_primary_key_identifier_list(&table.primary_key.columns)
                ));

                format!(
                    "-- CreateTable\nCREATE TABLE \"{}\" (\n{}\n);",
                    table.name,
                    lines.join("\n")
                )
            }
            Self::DropTable { name } => {
                format!("-- DropTable\nDROP TABLE \"{name}\";")
            }
            Self::CreateIndex { table, index } => format!(
                "-- CreateIndex\nCREATE {}INDEX \"{}\" ON \"{}\"({});",
                if index.unique { "UNIQUE " } else { "" },
                index.name,
                table,
                render_index_identifier_list(&index.columns)
            ),
            Self::AddForeignKey { table, foreign_key } => format!(
                "-- AddForeignKey\nALTER TABLE \"{table}\" ADD CONSTRAINT \"{}\" FOREIGN KEY ({}) REFERENCES \"{}\"({}) ON DELETE {} ON UPDATE {};",
                foreign_key.name,
                render_foreign_key_identifier_list(&foreign_key.columns),
                foreign_key.referenced_table,
                render_foreign_key_identifier_list(&foreign_key.referenced_columns),
                foreign_key.on_delete.render(),
                foreign_key.on_update.render(),
            ),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum AlterTableAction {
    AddColumn {
        column: PostgresColumn,
    },
    DropColumn {
        name: String,
    },
    SetColumnType {
        name: String,
        ty: ColumnType,
    },
    SetDefault {
        name: String,
        default: ColumnDefault,
    },
    DropDefault {
        name: String,
    },
    SetNotNull {
        name: String,
    },
    DropNotNull {
        name: String,
    },
}

impl AlterTableAction {
    fn render(&self) -> String {
        match self {
            Self::AddColumn { column } => {
                format!("ADD COLUMN     {}", column.render_column_definition())
            }
            Self::DropColumn { name } => format!("DROP COLUMN \"{name}\""),
            Self::SetColumnType { name, ty } => {
                format!("ALTER COLUMN \"{name}\" SET DATA TYPE {}", ty.render(None))
            }
            Self::SetDefault { name, default } => {
                format!("ALTER COLUMN \"{name}\" SET DEFAULT {}", default.render())
            }
            Self::DropDefault { name } => {
                format!("ALTER COLUMN \"{name}\" DROP DEFAULT")
            }
            Self::SetNotNull { name } => format!("ALTER COLUMN \"{name}\" SET NOT NULL"),
            Self::DropNotNull { name } => format!("ALTER COLUMN \"{name}\" DROP NOT NULL"),
        }
    }
}

fn render_primary_key_identifier_list(columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| format!(r#""{column}""#))
        .collect::<Vec<_>>()
        .join(",")
}

fn render_index_identifier_list(columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| format!(r#""{column}""#))
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_foreign_key_identifier_list(columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| format!(r#""{column}""#))
        .collect::<Vec<_>>()
        .join(", ")
}
