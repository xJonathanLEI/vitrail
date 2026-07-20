use std::sync::Arc;

use worker::d1::D1Database;

use crate::{DeleteSpec, Error, InsertSpec, QuerySpec, UpdateSpec};

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

    pub async fn find_many<Q>(&self, query: Q) -> Result<Vec<Q::Output>, Error>
    where
        Q: QuerySpec,
    {
        query.fetch_many(&self.database).await
    }

    pub async fn find_optional<Q>(&self, query: Q) -> Result<Option<Q::Output>, Error>
    where
        Q: QuerySpec,
    {
        query.fetch_optional(&self.database).await
    }

    pub async fn find_first<Q>(&self, query: Q) -> Result<Q::Output, Error>
    where
        Q: QuerySpec,
    {
        query.fetch_first(&self.database).await
    }

    pub async fn insert<I>(&self, insert: I) -> Result<I::Output, Error>
    where
        I: InsertSpec,
    {
        insert.execute(&self.database).await
    }

    pub async fn update_many<U>(&self, update: U) -> Result<U::Output, Error>
    where
        U: UpdateSpec,
    {
        update.execute(&self.database).await
    }

    pub async fn delete_many<D>(&self, delete: D) -> Result<D::Output, Error>
    where
        D: DeleteSpec,
    {
        delete.execute(&self.database).await
    }
}

impl From<D1Database> for VitrailClient {
    fn from(database: D1Database) -> Self {
        Self::new(database)
    }
}
