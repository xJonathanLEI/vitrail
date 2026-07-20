use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use worker::d1::D1DatabaseSession;

use crate::{D1Executor, DeleteSpec, Error, InsertSpec, QuerySpec, UpdateSpec};

/// An opaque, non-empty Cloudflare D1 session bookmark.
///
/// Vitrail does not inspect or interpret bookmark contents. The transparent
/// Serde representation is a JSON string, making bookmarks suitable for
/// propagation through headers, cookies, or application payloads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Bookmark(String);

impl Bookmark {
    /// Returns the opaque bookmark string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(value: String) -> Result<Self, Error> {
        if value.is_empty() {
            return Err(Error::InvalidBookmark(
                "bookmark must not be empty".to_owned(),
            ));
        }

        Ok(Self(value))
    }
}

impl fmt::Display for Bookmark {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for Bookmark {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_from(value)
    }
}

impl TryFrom<String> for Bookmark {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::validate(value)
    }
}

impl TryFrom<&str> for Bookmark {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::validate(value.to_owned())
    }
}

impl Serialize for Bookmark {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Bookmark {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_from(value).map_err(serde::de::Error::custom)
    }
}

/// Initial consistency constraint for a new D1 session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionConstraint {
    /// Starts the session at the current primary state.
    FirstPrimary,
    /// Allows the session's first operation to use any eligible replica.
    FirstUnconstrained,
    /// Starts the session at least as fresh as a previously observed bookmark.
    Bookmark(Bookmark),
}

/// An explicit Cloudflare D1 logical session.
///
/// One Workers SDK session is retained for the lifetime of this value, providing
/// sequential consistency across operations that are awaited in order. Sessions
/// are intentionally not cloneable.
#[derive(Debug)]
pub struct VitrailSession {
    session: D1DatabaseSession,
}

impl VitrailSession {
    pub(crate) fn new(session: D1DatabaseSession) -> Self {
        Self { session }
    }

    pub async fn find_many<Q>(&self, query: Q) -> Result<Vec<Q::Output>, Error>
    where
        Q: QuerySpec,
    {
        let executor: &dyn D1Executor = &self.session;
        query.fetch_many(executor).await
    }

    pub async fn find_optional<Q>(&self, query: Q) -> Result<Option<Q::Output>, Error>
    where
        Q: QuerySpec,
    {
        let executor: &dyn D1Executor = &self.session;
        query.fetch_optional(executor).await
    }

    pub async fn find_first<Q>(&self, query: Q) -> Result<Q::Output, Error>
    where
        Q: QuerySpec,
    {
        let executor: &dyn D1Executor = &self.session;
        query.fetch_first(executor).await
    }

    pub async fn insert<I>(&self, insert: I) -> Result<I::Output, Error>
    where
        I: InsertSpec,
    {
        let executor: &dyn D1Executor = &self.session;
        insert.execute(executor).await
    }

    pub async fn update_many<U>(&self, update: U) -> Result<U::Output, Error>
    where
        U: UpdateSpec,
    {
        let executor: &dyn D1Executor = &self.session;
        update.execute(executor).await
    }

    pub async fn delete_many<D>(&self, delete: D) -> Result<D::Output, Error>
    where
        D: DeleteSpec,
    {
        let executor: &dyn D1Executor = &self.session;
        delete.execute(executor).await
    }

    /// Returns the latest bookmark observed by this session.
    ///
    /// Before the session executes its first operation, D1 may return `None`.
    pub fn latest_bookmark(&self) -> Result<Option<Bookmark>, Error> {
        self.session
            .get_bookmark()?
            .map(Bookmark::try_from)
            .transpose()
    }
}
