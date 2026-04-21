#[path = "pg/support.rs"]
mod support;

#[path = "pg/insert.rs"]
mod insert;
pub(crate) use insert::pg_insert_schema as insert_schema;

#[path = "pg/bytes.rs"]
mod bytes;
pub(crate) use bytes::pg_bytes_schema as bytes_schema;

#[path = "pg/bigint.rs"]
mod bigint;
pub(crate) use bigint::pg_bigint_schema as bigint_schema;

#[path = "pg/custom_types.rs"]
mod custom_types;
pub(crate) use custom_types::pg_custom_types_schema as custom_types_schema;

#[path = "pg/decimal.rs"]
mod decimal;
pub(crate) use decimal::pg_decimal_schema as decimal_schema;

#[path = "pg/uuid.rs"]
mod uuid;
pub(crate) use uuid::pg_uuid_schema as uuid_schema;

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

#[path = "pg/delete.rs"]
mod delete;
pub(crate) use delete::pg_delete_schema as delete_schema;

#[path = "pg/transaction.rs"]
mod transaction;
pub(crate) use transaction::pg_transaction_schema as transaction_schema;
