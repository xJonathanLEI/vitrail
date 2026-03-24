#[path = "pg/support.rs"]
mod support;

#[path = "pg/migrations.rs"]
mod migrations;

#[path = "pg/query.rs"]
mod query;
pub(crate) use query::pg_query_schema as query_schema;

#[path = "pg/statements.rs"]
mod statements;
pub(crate) use statements::pg_statements_schema as statements_schema;
