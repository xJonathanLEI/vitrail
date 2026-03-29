use sqlx::postgres::{PgPool, PgPoolOptions};

use crate::{
    DeleteSpec, InsertSpec, PgExecutor, QuerySpec, TransactionOptions, UpdateSpec,
    VitrailTransaction,
};

/// Postgres client entry point.
#[derive(Clone, Debug)]
pub struct VitrailClient {
    pool: PgPool,
}

impl VitrailClient {
    pub async fn new(database_url: &str) -> Result<Self, sqlx::Error> {
        Self::with_options(PgPoolOptions::new(), database_url).await
    }

    pub async fn with_options(
        options: PgPoolOptions,
        database_url: &str,
    ) -> Result<Self, sqlx::Error> {
        let pool = options.connect(database_url).await?;
        Ok(Self { pool })
    }

    /// Starts a new transaction using the database default isolation level.
    ///
    /// The returned [`VitrailTransaction`] is an explicit transaction handle that
    /// can be passed through service layers and used with the same core query and
    /// write operations as [`VitrailClient`].
    pub async fn begin(&self) -> Result<VitrailTransaction, sqlx::Error> {
        self.begin_with_options(TransactionOptions::default()).await
    }

    /// Starts a new transaction with explicit transaction options.
    ///
    /// This is the entry point for configuring transaction startup behavior such
    /// as the isolation level.
    pub async fn begin_with_options(
        &self,
        options: TransactionOptions,
    ) -> Result<VitrailTransaction, sqlx::Error> {
        let transaction = self.pool.begin().await?;
        let transaction = VitrailTransaction::new(transaction);

        if let Some(isolation_level) = options.isolation_level {
            transaction.set_isolation_level(isolation_level).await?;
        }

        Ok(transaction)
    }

    pub async fn find_many<Q>(&self, query: Q) -> Result<Vec<Q::Output>, sqlx::Error>
    where
        Q: QuerySpec,
    {
        let executor: &dyn PgExecutor = &self.pool;
        query.fetch_many(executor).await
    }

    pub async fn find_optional<Q>(&self, query: Q) -> Result<Option<Q::Output>, sqlx::Error>
    where
        Q: QuerySpec,
    {
        let executor: &dyn PgExecutor = &self.pool;
        query.fetch_optional(executor).await
    }

    pub async fn find_first<Q>(&self, query: Q) -> Result<Q::Output, sqlx::Error>
    where
        Q: QuerySpec,
    {
        let executor: &dyn PgExecutor = &self.pool;
        query.fetch_first(executor).await
    }

    pub async fn insert<I>(&self, insert: I) -> Result<I::Output, sqlx::Error>
    where
        I: InsertSpec,
    {
        let executor: &dyn PgExecutor = &self.pool;
        insert.execute(executor).await
    }

    pub async fn update_many<U>(&self, update: U) -> Result<U::Output, sqlx::Error>
    where
        U: UpdateSpec,
    {
        let executor: &dyn PgExecutor = &self.pool;
        update.execute(executor).await
    }

    pub async fn delete_many<D>(&self, delete: D) -> Result<D::Output, sqlx::Error>
    where
        D: DeleteSpec,
    {
        let executor: &dyn PgExecutor = &self.pool;
        delete.execute(executor).await
    }

    pub async fn close(&self) {
        self.pool.close().await;
    }
}
