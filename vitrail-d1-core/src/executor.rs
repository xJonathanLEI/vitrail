use worker::d1::{D1Database, D1DatabaseSession, D1PreparedStatement};

/// Internal execution abstraction for D1-backed query and write operations.
///
/// This trait is used by [`crate::VitrailClient`] and [`crate::VitrailSession`]
/// so runtime specs can execute against either a database binding or an explicit
/// session while sharing compilation, binding, and decoding logic. The
/// object-safe trait only abstracts synchronous statement preparation; shared
/// asynchronous execution remains in the runtime helpers.
#[doc(hidden)]
pub trait D1Executor: private::Sealed + Send + Sync {
    fn prepare(&self, sql: &str) -> D1PreparedStatement;
}

impl private::Sealed for D1Database {}
impl private::Sealed for D1DatabaseSession {}

impl D1Executor for D1Database {
    fn prepare(&self, sql: &str) -> D1PreparedStatement {
        D1Database::prepare(self, sql)
    }
}

impl D1Executor for D1DatabaseSession {
    fn prepare(&self, sql: &str) -> D1PreparedStatement {
        D1DatabaseSession::prepare(self, sql)
    }
}

mod private {
    /// Seals the executor abstraction so external crates cannot add execution
    /// targets with unsupported consistency semantics.
    pub trait Sealed {}
}
