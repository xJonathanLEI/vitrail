//! Backend-neutral procedural macro expansion internals for Vitrail dialect crates.

pub mod embedded_migrations;
pub mod schema;

pub use embedded_migrations::expand_embedded_migrations;
pub use schema::{
    NativeAttributeKind, NativeAttributeMapping, OperationFamilies, SchemaMacroConfig,
    expand_schema,
};
