use std::fmt;

/// Error produced while validating or compiling a SQLite-family operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompileError {
    message: String,
}

impl CompileError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for CompileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for CompileError {}
