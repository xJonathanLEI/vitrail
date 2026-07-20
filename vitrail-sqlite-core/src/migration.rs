use std::collections::{BTreeMap, HashMap, HashSet};

use sqlx::sqlite::SqliteConnection;
use sqlx::{Connection as _, Row as _};

use crate::{Schema, SchemaAccess};

pub use vitrail_sqlite_dialect::{
    ColumnDefault, ColumnType, ForeignKeyAction, SqliteColumn, SqliteForeignKey, SqliteIndex,
    SqliteMigration, SqlitePrimaryKey, SqliteTable,
};

#[derive(Clone, Default, Eq, PartialEq)]
pub struct SqliteSchema {
    inner: vitrail_sqlite_dialect::SqliteSchema,
}

impl std::fmt::Debug for SqliteSchema {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqliteSchema")
            .field("tables", &self.tables())
            .finish()
    }
}

impl SqliteSchema {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn from_schema(schema: &Schema) -> Self {
        Self {
            inner: vitrail_sqlite_dialect::SqliteSchema::from_schema(schema.as_dialect()),
        }
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

    pub(crate) async fn introspect_from_connection(
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

                columns.push(SqliteColumn::from_introspection(
                    name,
                    ColumnType::from_introspection(&declared_type),
                    column_row.try_get::<i64, _>("not_null")? == 0,
                    ColumnDefault::from_introspection(
                        column_row.try_get::<Option<String>, _>("dflt_value")?,
                    ),
                ));
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
                    .find(|column| column.name() == primary_key_column.as_str())
            {
                column.set_default_from_introspection(Some(ColumnDefault::Autoincrement));
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

                indexes.push(SqliteIndex::from_introspection(
                    index_name,
                    index_columns,
                    index_row.try_get::<i64, _>("is_unique")? != 0,
                    definition_supported,
                ));
            }

            // A sole INTEGER primary key aliases rowid unless SQLite created a
            // separate primary-key index, as it does for `INTEGER PRIMARY KEY DESC`.
            if !has_primary_key_index
                && let [primary_key_column] = primary_key_columns.as_slice()
                && integer_primary_key_column.as_deref() == Some(primary_key_column.as_str())
                && let Some(column) = columns
                    .iter_mut()
                    .find(|column| column.name() == primary_key_column.as_str())
            {
                column.set_nullable_from_introspection(false);
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
                            )
                            .map_err(|error| invalid_introspection_data(error.to_string()))?,
                            on_update: ForeignKeyAction::from_introspection(
                                &foreign_key_row.try_get::<String, _>("on_update")?,
                            )
                            .map_err(|error| invalid_introspection_data(error.to_string()))?,
                        },
                    );
                }
            }

            let mut foreign_keys = pending_foreign_keys
                .into_values()
                .map(|foreign_key| {
                    SqliteForeignKey::from_introspection(
                        format!("{}_{}_fkey", table_name, foreign_key.columns.join("_")),
                        foreign_key.columns,
                        foreign_key.referenced_table,
                        foreign_key.referenced_columns,
                        foreign_key.on_delete,
                        foreign_key.on_update,
                    )
                })
                .collect::<Vec<_>>();

            foreign_keys.sort_by(|left, right| left.name().cmp(right.name()));

            tables.push(SqliteTable::from_introspection(
                table_name.clone(),
                columns,
                SqlitePrimaryKey::from_introspection(
                    format!("{table_name}_pkey"),
                    primary_key_columns,
                ),
                indexes,
                foreign_keys,
            ));
        }

        Ok(Self {
            inner: vitrail_sqlite_dialect::SqliteSchema::from_introspection(tables),
        })
    }

    pub fn tables(&self) -> &[SqliteTable] {
        self.inner.tables()
    }

    pub fn migrate_from(&self, current: &Self) -> SqliteMigration {
        self.inner.migrate_from(&current.inner)
    }
}

impl Schema {
    pub fn to_sqlite_schema(&self) -> SqliteSchema {
        SqliteSchema::from_schema(self)
    }
}

/// Applies migration scripts atomically to an in-memory SQLite database and
/// returns its runtime-neutral introspected schema.
///
/// Each script executes in its own transaction so host migration tooling can
/// model runtimes that apply individual migration files atomically.
#[doc(hidden)]
pub async fn introspect_atomic_shadow_schema<I, S>(
    migration_scripts: I,
    ignored_tables: &[String],
) -> Result<vitrail_sqlite_dialect::SqliteSchema, sqlx::Error>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut connection = SqliteConnection::connect("sqlite::memory:").await?;

    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&mut connection)
        .await?;

    for migration_sql in migration_scripts {
        let migration_sql = migration_sql.as_ref();

        if migration_sql.trim().is_empty() {
            continue;
        }

        let mut transaction = connection.begin().await?;

        sqlx::raw_sql(migration_sql)
            .execute(&mut *transaction)
            .await?;

        transaction.commit().await?;
    }

    let schema = SqliteSchema::introspect_from_connection(&mut connection, ignored_tables).await?;

    Ok(schema.inner)
}

struct PendingForeignKey {
    columns: Vec<String>,
    referenced_table: String,
    referenced_columns: Vec<String>,
    on_delete: ForeignKeyAction,
    on_update: ForeignKeyAction,
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

fn invalid_introspection_data(message: impl Into<String>) -> sqlx::Error {
    sqlx::Error::Decode(Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message.into(),
    )))
}
