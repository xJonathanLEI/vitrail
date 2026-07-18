//! Backend-neutral procedural macro expansion internals for Vitrail dialect crates.

pub mod embedded_migrations;
mod filter;
mod helper_macro;
mod order;
pub mod query;
pub mod schema;

pub use embedded_migrations::expand_embedded_migrations;
pub use filter::{RootFilter, parse_root_filter};
pub use helper_macro::expand_helper_macro;
pub use query::{QueryMacroConfig, expand_query, expand_query_result, expand_query_variables};
pub use schema::{
    NativeAttributeKind, NativeAttributeMapping, OperationFamilies, SchemaMacroConfig,
    expand_schema,
};
