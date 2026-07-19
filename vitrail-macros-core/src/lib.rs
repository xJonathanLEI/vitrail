//! Backend-neutral procedural macro expansion internals for Vitrail dialect crates.

mod delete;
pub mod embedded_migrations;
mod filter;
mod helper_macro;
mod insert;
mod order;
pub mod query;
pub mod schema;
mod update;
pub mod write;

pub use embedded_migrations::expand_embedded_migrations;
pub use query::{QueryMacroConfig, expand_query, expand_query_result, expand_query_variables};
pub use schema::{
    NativeAttributeKind, NativeAttributeMapping, OperationFamilies, SchemaMacroConfig,
    expand_schema,
};
pub use write::{
    WriteMacroConfig, expand_delete, expand_delete_many, expand_insert, expand_insert_input,
    expand_insert_result, expand_update, expand_update_data, expand_update_many,
};
