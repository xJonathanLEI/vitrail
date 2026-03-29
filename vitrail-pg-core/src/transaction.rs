use std::fmt;

use futures_util::lock::Mutex;
use sqlx::postgres::{PgArguments, PgQueryResult, PgRow};
use sqlx::{Postgres, Transaction, query::Query as SqlxQuery};

use crate::{BoxFuture, DeleteSpec, InsertSpec, PgExecutor, QuerySpec, UpdateSpec};

/// Options for starting a [`VitrailTransaction`] through
/// `VitrailClient::begin_with_options(...)`.
///
/// The first transaction iteration intentionally keeps this type small and only
/// exposes configuration that is meaningful for Postgres transaction startup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransactionOptions {
    /// Isolation level to apply when the transaction starts.
    ///
    /// When this is `None`, Postgres uses the database default isolation level.
    pub isolation_level: Option<TransactionIsolationLevel>,
}

impl TransactionOptions {
    /// Creates transaction options that use the database default isolation level.
    pub const fn new() -> Self {
        Self {
            isolation_level: None,
        }
    }

    /// Creates transaction options with an explicit isolation level.
    pub const fn with_isolation_level(isolation_level: TransactionIsolationLevel) -> Self {
        Self {
            isolation_level: Some(isolation_level),
        }
    }
}

impl Default for TransactionOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Supported Postgres transaction isolation levels for
/// [`TransactionOptions::isolation_level`].
///
/// These variants map directly to the corresponding `SET TRANSACTION
/// ISOLATION LEVEL ...` SQL statements.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionIsolationLevel {
    /// Postgres `READ COMMITTED`.
    ReadCommitted,
    /// Postgres `REPEATABLE READ`.
    RepeatableRead,
    /// Postgres `SERIALIZABLE`.
    Serializable,
}

impl TransactionIsolationLevel {
    pub(crate) const fn as_sql(self) -> &'static str {
        match self {
            Self::ReadCommitted => "READ COMMITTED",
            Self::RepeatableRead => "REPEATABLE READ",
            Self::Serializable => "SERIALIZABLE",
        }
    }
}

/// Explicit Postgres transaction handle for executing Vitrail operations atomically.
///
/// `VitrailTransaction` mirrors the core query and write methods on
/// [`crate::VitrailClient`], which makes it straightforward to begin a
/// transaction, pass `&VitrailTransaction` through service layers, and then
/// explicitly `commit()` or `rollback()` when the workflow is complete.
///
/// A transaction owns a single database connection. Queries executed through the
/// same transaction therefore run serially on that one connection, even if user
/// code attempts to poll multiple futures concurrently.
///
/// Dropping an open transaction rolls it back through the underlying `sqlx`
/// transaction behavior, but explicit `commit()` or `rollback()` should remain
/// the primary application control flow.
pub struct VitrailTransaction {
    transaction: Mutex<Option<Transaction<'static, Postgres>>>,
}

impl VitrailTransaction {
    pub(crate) fn new(transaction: Transaction<'static, Postgres>) -> Self {
        Self {
            transaction: Mutex::new(Some(transaction)),
        }
    }

    pub(crate) async fn set_isolation_level(
        &self,
        isolation_level: TransactionIsolationLevel,
    ) -> Result<(), sqlx::Error> {
        let sql = format!(
            "SET TRANSACTION ISOLATION LEVEL {}",
            isolation_level.as_sql()
        );

        let executor: &dyn PgExecutor = self;
        executor.execute(sqlx::query(&sql)).await?;
        Ok(())
    }

    pub async fn find_many<Q>(&self, query: Q) -> Result<Vec<Q::Output>, sqlx::Error>
    where
        Q: QuerySpec,
    {
        let executor: &dyn PgExecutor = self;
        query.fetch_many(executor).await
    }

    pub async fn find_optional<Q>(&self, query: Q) -> Result<Option<Q::Output>, sqlx::Error>
    where
        Q: QuerySpec,
    {
        let executor: &dyn PgExecutor = self;
        query.fetch_optional(executor).await
    }

    pub async fn find_first<Q>(&self, query: Q) -> Result<Q::Output, sqlx::Error>
    where
        Q: QuerySpec,
    {
        let executor: &dyn PgExecutor = self;
        query.fetch_first(executor).await
    }

    pub async fn insert<I>(&self, insert: I) -> Result<I::Output, sqlx::Error>
    where
        I: InsertSpec,
    {
        let executor: &dyn PgExecutor = self;
        insert.execute(executor).await
    }

    pub async fn update_many<U>(&self, update: U) -> Result<U::Output, sqlx::Error>
    where
        U: UpdateSpec,
    {
        let executor: &dyn PgExecutor = self;
        update.execute(executor).await
    }

    pub async fn delete_many<D>(&self, delete: D) -> Result<D::Output, sqlx::Error>
    where
        D: DeleteSpec,
    {
        let executor: &dyn PgExecutor = self;
        delete.execute(executor).await
    }

    /// Commits the transaction.
    ///
    /// This consumes the transaction handle so it cannot be reused afterward.
    pub async fn commit(self) -> Result<(), sqlx::Error> {
        let transaction = self.take_transaction("commit")?;
        transaction.commit().await
    }

    /// Rolls the transaction back.
    ///
    /// This consumes the transaction handle so it cannot be reused afterward.
    pub async fn rollback(self) -> Result<(), sqlx::Error> {
        let transaction = self.take_transaction("rollback")?;
        transaction.rollback().await
    }

    fn take_transaction(
        self,
        operation: &'static str,
    ) -> Result<Transaction<'static, Postgres>, sqlx::Error> {
        let mut guard = self
            .transaction
            .try_lock()
            .ok_or_else(|| busy_transaction_close_error(operation))?;

        guard
            .take()
            .ok_or_else(|| closed_transaction_close_error(operation))
    }
}

impl fmt::Debug for VitrailTransaction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VitrailTransaction")
            .finish_non_exhaustive()
    }
}

impl PgExecutor for VitrailTransaction {
    fn fetch_all<'a>(
        &'a self,
        query: SqlxQuery<'a, Postgres, PgArguments>,
    ) -> BoxFuture<'a, Result<Vec<PgRow>, sqlx::Error>> {
        Box::pin(async move {
            let mut guard = self.transaction.lock().await;
            let transaction = guard.as_mut().ok_or_else(closed_transaction_use_error)?;
            query.fetch_all(&mut **transaction).await
        })
    }

    fn fetch_optional<'a>(
        &'a self,
        query: SqlxQuery<'a, Postgres, PgArguments>,
    ) -> BoxFuture<'a, Result<Option<PgRow>, sqlx::Error>> {
        Box::pin(async move {
            let mut guard = self.transaction.lock().await;
            let transaction = guard.as_mut().ok_or_else(closed_transaction_use_error)?;
            query.fetch_optional(&mut **transaction).await
        })
    }

    fn fetch_one<'a>(
        &'a self,
        query: SqlxQuery<'a, Postgres, PgArguments>,
    ) -> BoxFuture<'a, Result<PgRow, sqlx::Error>> {
        Box::pin(async move {
            let mut guard = self.transaction.lock().await;
            let transaction = guard.as_mut().ok_or_else(closed_transaction_use_error)?;
            query.fetch_one(&mut **transaction).await
        })
    }

    fn execute<'a>(
        &'a self,
        query: SqlxQuery<'a, Postgres, PgArguments>,
    ) -> BoxFuture<'a, Result<PgQueryResult, sqlx::Error>> {
        Box::pin(async move {
            let mut guard = self.transaction.lock().await;
            let transaction = guard.as_mut().ok_or_else(closed_transaction_use_error)?;
            query.execute(&mut **transaction).await
        })
    }
}

fn closed_transaction_use_error() -> sqlx::Error {
    sqlx::Error::Protocol(
        "transaction is already closed; it may have already been committed or rolled back".into(),
    )
}

fn closed_transaction_close_error(operation: &'static str) -> sqlx::Error {
    sqlx::Error::Protocol(format!(
        "cannot {operation} transaction because it is already closed"
    ))
}

fn busy_transaction_close_error(operation: &'static str) -> sqlx::Error {
    sqlx::Error::Protocol(format!(
        "cannot {operation} transaction while another transaction operation is still in progress"
    ))
}
