use std::error::Error as StdError;
use std::fmt;

use vitrail_sqlite_dialect::{CompileError, ValidationErrors};

type BoxError = Box<dyn StdError + Send + Sync + 'static>;

/// Details about a controlled D1 result-decoding failure.
#[derive(Debug)]
pub struct DecodeError {
    message: String,
    source: Option<BoxError>,
}

impl DecodeError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            source: None,
        }
    }

    pub(crate) fn with_source<E>(message: impl Into<String>, source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    /// Returns the decoding error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for DecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl StdError for DecodeError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn StdError + 'static))
    }
}

/// Error returned by Cloudflare D1 Vitrail operations.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// A query or write could not be compiled for D1.
    Compile(CompileError),
    /// A schema exceeds a Cloudflare D1 platform limit.
    PlatformLimit(ValidationErrors),
    /// The Workers SDK or D1 binding returned an error.
    Worker(worker::Error),
    /// An operation requiring one row returned no rows.
    RowNotFound,
    /// A compiled value could not be converted into a D1 binding.
    Binding(String),
    /// A D1 result could not be decoded safely.
    Decode(DecodeError),
    /// A bookmark was empty or otherwise invalid.
    InvalidBookmark(String),
    /// D1 omitted metadata required to report a write result.
    MissingWriteMetadata {
        /// The operation whose metadata was missing.
        operation: &'static str,
    },
    /// A D1 batch result did not match the submitted batch shape.
    BatchShape(String),
}

impl Error {
    pub(crate) fn binding(message: impl Into<String>) -> Self {
        Self::Binding(message.into())
    }

    pub(crate) fn decode(message: impl Into<String>) -> Self {
        Self::Decode(DecodeError::new(message))
    }

    pub(crate) fn decode_with_source<E>(message: impl Into<String>, source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::Decode(DecodeError::with_source(message, source))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Compile(error) => write!(formatter, "failed to compile D1 statement: {error}"),
            Self::PlatformLimit(error) => {
                write!(formatter, "schema exceeds Cloudflare D1 limits: {error}")
            }
            Self::Worker(error) => write!(formatter, "Cloudflare D1 operation failed: {error}"),
            Self::RowNotFound => formatter.write_str("D1 query returned no rows"),
            Self::Binding(message) => write!(formatter, "failed to bind D1 statement: {message}"),
            Self::Decode(error) => write!(formatter, "failed to decode D1 result: {error}"),
            Self::InvalidBookmark(message) => write!(formatter, "invalid D1 bookmark: {message}"),
            Self::MissingWriteMetadata { operation } => write!(
                formatter,
                "D1 did not return the `changes` metadata required for {operation}"
            ),
            Self::BatchShape(message) => write!(formatter, "invalid D1 batch result: {message}"),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Compile(error) => Some(error),
            Self::PlatformLimit(error) => Some(error),
            Self::Worker(error) => Some(error),
            Self::Decode(error) => Some(error),
            Self::RowNotFound
            | Self::Binding(_)
            | Self::InvalidBookmark(_)
            | Self::MissingWriteMetadata { .. }
            | Self::BatchShape(_) => None,
        }
    }
}

impl From<CompileError> for Error {
    fn from(error: CompileError) -> Self {
        Self::Compile(error)
    }
}

impl From<ValidationErrors> for Error {
    fn from(error: ValidationErrors) -> Self {
        Self::PlatformLimit(error)
    }
}

impl From<worker::Error> for Error {
    fn from(error: worker::Error) -> Self {
        Self::Worker(error)
    }
}

/// Wraps a custom string-type decoding error while retaining it as the source.
///
/// Implementations of [`crate::StringValueType`] can use this helper to map
/// their parser errors into Vitrail's controlled D1 decoding error.
pub fn decode_error<E>(error: E) -> Error
where
    E: StdError + Send + Sync + 'static,
{
    let message = error.to_string();
    Error::decode_with_source(message, error)
}
