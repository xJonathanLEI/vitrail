use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};

use crate::{InsertSpec, QuerySpec, SqliteExecutor};

/// SQLite client entry point.
#[derive(Clone, Debug)]
pub struct VitrailClient {
    pool: SqlitePool,
}

impl VitrailClient {
    pub async fn new(database_url: &str) -> Result<Self, sqlx::Error> {
        Self::with_options(SqlitePoolOptions::new(), database_url).await
    }

    pub async fn with_options(
        options: SqlitePoolOptions,
        database_url: &str,
    ) -> Result<Self, sqlx::Error> {
        let pool = options.connect(database_url).await?;
        Ok(Self { pool })
    }

    pub async fn find_many<Q>(&self, query: Q) -> Result<Vec<Q::Output>, sqlx::Error>
    where
        Q: QuerySpec,
    {
        let executor: &dyn SqliteExecutor = &self.pool;
        query.fetch_many(executor).await
    }

    pub async fn find_optional<Q>(&self, query: Q) -> Result<Option<Q::Output>, sqlx::Error>
    where
        Q: QuerySpec,
    {
        let executor: &dyn SqliteExecutor = &self.pool;
        query.fetch_optional(executor).await
    }

    pub async fn find_first<Q>(&self, query: Q) -> Result<Q::Output, sqlx::Error>
    where
        Q: QuerySpec,
    {
        let executor: &dyn SqliteExecutor = &self.pool;
        query.fetch_first(executor).await
    }

    pub async fn insert<I>(&self, insert: I) -> Result<I::Output, sqlx::Error>
    where
        I: InsertSpec,
    {
        let executor: &dyn SqliteExecutor = &self.pool;
        insert.execute(executor).await
    }

    pub async fn close(&self) {
        self.pool.close().await;
    }
}
