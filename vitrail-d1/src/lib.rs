#[cfg(not(target_arch = "wasm32"))]
pub mod cli;
#[cfg(not(target_arch = "wasm32"))]
mod migration;

#[cfg(not(target_arch = "wasm32"))]
pub use cli::{VitrailCli, run_cli};
#[cfg(not(target_arch = "wasm32"))]
pub use migration::{
    D1Migration, D1MigrationError, D1MigrationGenerator, GeneratedMigration, Migration,
};
pub use serde_json;
pub use vitrail_d1_core::*;
pub use vitrail_d1_macros::*;
pub use worker;
