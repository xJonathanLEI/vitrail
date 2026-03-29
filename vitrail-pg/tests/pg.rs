#[path = "pg/support.rs"]
mod support;

#[path = "pg/insert.rs"]
mod insert;
pub(crate) use insert::pg_insert_schema as insert_schema;

#[path = "pg/migrations.rs"]
mod migrations;

#[path = "pg/migrator.rs"]
mod migrator;

#[path = "pg/query.rs"]
mod query;
pub(crate) use query::pg_query_schema as query_schema;

#[path = "pg/statements.rs"]
mod statements;
pub(crate) use statements::pg_compound_statements_schema as compound_statements_schema;
pub(crate) use statements::pg_statements_schema as statements_schema;

#[path = "pg/update.rs"]
mod update;
pub(crate) use update::pg_update_schema as update_schema;
