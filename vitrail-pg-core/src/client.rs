use sqlx::postgres::{PgPool, PgPoolOptions};

use crate::{InsertSpec, QuerySpec, UpdateSpec};

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

    pub async fn find_many<Q>(&self, query: Q) -> Result<Vec<Q::Output>, sqlx::Error>
    where
        Q: QuerySpec,
    {
        query.fetch_many(&self.pool).await
    }

    pub async fn find_optional<Q>(&self, query: Q) -> Result<Option<Q::Output>, sqlx::Error>
    where
        Q: QuerySpec,
    {
        query.fetch_optional(&self.pool).await
    }

    pub async fn find_first<Q>(&self, query: Q) -> Result<Q::Output, sqlx::Error>
    where
        Q: QuerySpec,
    {
        query.fetch_first(&self.pool).await
    }

    pub async fn insert<I>(&self, insert: I) -> Result<I::Output, sqlx::Error>
    where
        I: InsertSpec,
    {
        insert.execute(&self.pool).await
    }

    pub async fn update_many<U>(&self, update: U) -> Result<U::Output, sqlx::Error>
    where
        U: UpdateSpec,
    {
        update.execute(&self.pool).await
    }

    pub async fn close(&self) {
        self.pool.close().await;
    }
}
