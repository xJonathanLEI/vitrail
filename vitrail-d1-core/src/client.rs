use std::sync::Arc;

use worker::d1::{D1Database, D1SessionConstraint};

use crate::{
    AtomicBatch, D1Executor, DeleteSpec, Error, InsertSpec, QuerySpec, SessionConstraint,
    UpdateSpec, VitrailSession,
};

/// Cloudflare D1 client entry point.
///
/// Operations performed directly through this client use D1's primary-backed
/// database binding.
#[derive(Clone, Debug)]
pub struct VitrailClient {
    database: Arc<D1Database>,
}

impl VitrailClient {
    /// Creates a Vitrail client from a Workers D1 database binding.
    pub fn new(database: D1Database) -> Self {
        Self {
            database: Arc::new(database),
        }
    }

    /// Creates an explicit logical D1 session with the requested initial
    /// consistency constraint.
    ///
    /// [`SessionConstraint::FirstPrimary`] starts at the current primary state,
    /// [`SessionConstraint::FirstUnconstrained`] allows a replica-eligible first
    /// operation, and [`SessionConstraint::Bookmark`] continues from at least a
    /// previously observed database version.
    pub fn with_session(&self, constraint: SessionConstraint) -> Result<VitrailSession, Error> {
        let session = match constraint {
            SessionConstraint::FirstPrimary => self
                .database
                .with_session_constraint(D1SessionConstraint::FirstPrimary)?,
            SessionConstraint::FirstUnconstrained => self
                .database
                .with_session_constraint(D1SessionConstraint::FirstUnconstrained)?,
            SessionConstraint::Bookmark(bookmark) => {
                self.database.with_session(Some(bookmark.as_str()))?
            }
        };

        Ok(VitrailSession::new(session))
    }

    /// Creates a typed atomic batch backed by this client's primary D1 binding.
    ///
    /// The queued operations are submitted together through one D1 `batch()`
    /// call when [`AtomicBatch::execute`] is invoked.
    pub fn atomic_batch(&self) -> AtomicBatch<'_> {
        AtomicBatch::for_database(self.database.as_ref())
    }

    pub async fn find_many<Q>(&self, query: Q) -> Result<Vec<Q::Output>, Error>
    where
        Q: QuerySpec,
    {
        let executor: &dyn D1Executor = self.database.as_ref();
        query.fetch_many(executor).await
    }

    pub async fn find_optional<Q>(&self, query: Q) -> Result<Option<Q::Output>, Error>
    where
        Q: QuerySpec,
    {
        let executor: &dyn D1Executor = self.database.as_ref();
        query.fetch_optional(executor).await
    }

    pub async fn find_first<Q>(&self, query: Q) -> Result<Q::Output, Error>
    where
        Q: QuerySpec,
    {
        let executor: &dyn D1Executor = self.database.as_ref();
        query.fetch_first(executor).await
    }

    pub async fn insert<I>(&self, insert: I) -> Result<I::Output, Error>
    where
        I: InsertSpec,
    {
        let executor: &dyn D1Executor = self.database.as_ref();
        insert.execute(executor).await
    }

    pub async fn update_many<U>(&self, update: U) -> Result<U::Output, Error>
    where
        U: UpdateSpec,
    {
        let executor: &dyn D1Executor = self.database.as_ref();
        update.execute(executor).await
    }

    pub async fn delete_many<D>(&self, delete: D) -> Result<D::Output, Error>
    where
        D: DeleteSpec,
    {
        let executor: &dyn D1Executor = self.database.as_ref();
        delete.execute(executor).await
    }
}

impl From<D1Database> for VitrailClient {
    fn from(database: D1Database) -> Self {
        Self::new(database)
    }
}
