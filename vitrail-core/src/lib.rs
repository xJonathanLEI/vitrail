//! Backend-neutral internals shared by Vitrail dialect crates.

#[doc(hidden)]
pub mod migrations;
#[doc(hidden)]
pub mod schema;
#[doc(hidden)]
pub mod validation;

#[cfg(test)]
mod tests;
