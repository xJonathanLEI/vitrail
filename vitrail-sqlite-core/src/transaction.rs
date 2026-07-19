use std::fmt;

use futures_util::lock::Mutex;
use sqlx::sqlite::{SqliteArguments, SqliteQueryResult, SqliteRow};
use sqlx::{Sqlite, Transaction, query::Query as SqlxQuery};

use crate::{BoxFuture, DeleteSpec, InsertSpec, QuerySpec, SqliteExecutor, UpdateSpec};

/// Options for starting a [`VitrailTransaction`] through
/// `VitrailClient::begin_with_options(...)`.
///
/// SQLite transaction options are intentionally dialect-specific and expose
/// transaction modes rather than PostgreSQL-style isolation levels.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransactionOptions {
    /// Transaction mode to apply when the transaction starts.
    ///
    /// When this is `None`, SQLite uses its default transaction mode.
    pub mode: Option<TransactionMode>,
}

impl TransactionOptions {
    /// Creates transaction options that use SQLite's default transaction mode.
    pub const fn new() -> Self {
        Self { mode: None }
    }

    /// Creates transaction options with an explicit SQLite transaction mode.
    pub const fn with_mode(mode: TransactionMode) -> Self {
        Self { mode: Some(mode) }
    }
}

impl Default for TransactionOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Supported SQLite transaction modes.
///
/// These variants map directly to SQLite's `BEGIN DEFERRED`, `BEGIN IMMEDIATE`,
/// and `BEGIN EXCLUSIVE` statements.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionMode {
    /// Defers acquiring database locks until the transaction first accesses the
    /// database.
    Deferred,
    /// Starts a write transaction immediately.
    Immediate,
    /// Starts an exclusive transaction immediately.
    Exclusive,
}

impl TransactionMode {
    pub(crate) const fn as_sql(self) -> &'static str {
        match self {
            Self::Deferred => "BEGIN DEFERRED",
            Self::Immediate => "BEGIN IMMEDIATE",
            Self::Exclusive => "BEGIN EXCLUSIVE",
        }
    }
}

/// Explicit SQLite transaction handle for executing Vitrail operations atomically.
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
    transaction: Mutex<Option<Transaction<'static, Sqlite>>>,
}

impl VitrailTransaction {
    pub(crate) fn new(transaction: Transaction<'static, Sqlite>) -> Self {
        Self {
            transaction: Mutex::new(Some(transaction)),
        }
    }

    pub async fn find_many<Q>(&self, query: Q) -> Result<Vec<Q::Output>, sqlx::Error>
    where
        Q: QuerySpec,
    {
        let executor: &dyn SqliteExecutor = self;
        query.fetch_many(executor).await
    }

    pub async fn find_optional<Q>(&self, query: Q) -> Result<Option<Q::Output>, sqlx::Error>
    where
        Q: QuerySpec,
    {
        let executor: &dyn SqliteExecutor = self;
        query.fetch_optional(executor).await
    }

    pub async fn find_first<Q>(&self, query: Q) -> Result<Q::Output, sqlx::Error>
    where
        Q: QuerySpec,
    {
        let executor: &dyn SqliteExecutor = self;
        query.fetch_first(executor).await
    }

    pub async fn insert<I>(&self, insert: I) -> Result<I::Output, sqlx::Error>
    where
        I: InsertSpec,
    {
        let executor: &dyn SqliteExecutor = self;
        insert.execute(executor).await
    }

    pub async fn update_many<U>(&self, update: U) -> Result<U::Output, sqlx::Error>
    where
        U: UpdateSpec,
    {
        let executor: &dyn SqliteExecutor = self;
        update.execute(executor).await
    }

    pub async fn delete_many<D>(&self, delete: D) -> Result<D::Output, sqlx::Error>
    where
        D: DeleteSpec,
    {
        let executor: &dyn SqliteExecutor = self;
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
    ) -> Result<Transaction<'static, Sqlite>, sqlx::Error> {
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

impl SqliteExecutor for VitrailTransaction {
    fn fetch_all<'a>(
        &'a self,
        query: SqlxQuery<'a, Sqlite, SqliteArguments<'a>>,
    ) -> BoxFuture<'a, Result<Vec<SqliteRow>, sqlx::Error>> {
        Box::pin(async move {
            let mut guard = self.transaction.lock().await;
            let transaction = guard.as_mut().ok_or_else(closed_transaction_use_error)?;
            query.fetch_all(&mut **transaction).await
        })
    }

    fn fetch_optional<'a>(
        &'a self,
        query: SqlxQuery<'a, Sqlite, SqliteArguments<'a>>,
    ) -> BoxFuture<'a, Result<Option<SqliteRow>, sqlx::Error>> {
        Box::pin(async move {
            let mut guard = self.transaction.lock().await;
            let transaction = guard.as_mut().ok_or_else(closed_transaction_use_error)?;
            query.fetch_optional(&mut **transaction).await
        })
    }

    fn fetch_one<'a>(
        &'a self,
        query: SqlxQuery<'a, Sqlite, SqliteArguments<'a>>,
    ) -> BoxFuture<'a, Result<SqliteRow, sqlx::Error>> {
        Box::pin(async move {
            let mut guard = self.transaction.lock().await;
            let transaction = guard.as_mut().ok_or_else(closed_transaction_use_error)?;
            query.fetch_one(&mut **transaction).await
        })
    }

    fn execute<'a>(
        &'a self,
        query: SqlxQuery<'a, Sqlite, SqliteArguments<'a>>,
    ) -> BoxFuture<'a, Result<SqliteQueryResult, sqlx::Error>> {
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
