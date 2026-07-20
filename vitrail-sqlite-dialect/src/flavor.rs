use crate::{CompileError, OperationKind, ScalarType};

/// Maximum number of bound parameters accepted by a Cloudflare D1 statement.
pub const D1_MAX_BINDINGS: usize = 100;

/// Maximum UTF-8 size accepted for a Cloudflare D1 statement.
pub const D1_MAX_SQL_BYTES: usize = 100_000;

/// Maximum number of scalar database columns accepted by a Cloudflare D1 table.
pub const D1_MAX_COLUMNS: usize = 100;

/// Compatibility-floor argument count used for D1 JSON function calls.
pub const D1_JSON_FUNCTION_ARGUMENT_LIMIT: usize = 32;

/// Selects the SQLite-family behavior used while compiling an operation.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SqliteFamilyFlavor {
    #[default]
    Native,
    D1,
}

impl SqliteFamilyFlavor {
    pub(crate) const fn capabilities(self) -> SqliteFamilyCapabilities {
        match self {
            Self::Native => SqliteFamilyCapabilities {
                exact_integer_text_transport: false,
                json_function_argument_limit: None,
                statement_limits: None,
            },
            Self::D1 => SqliteFamilyCapabilities {
                exact_integer_text_transport: true,
                json_function_argument_limit: Some(D1_JSON_FUNCTION_ARGUMENT_LIMIT),
                statement_limits: Some(D1StatementLimits {
                    max_bindings: D1_MAX_BINDINGS,
                    max_sql_bytes: D1_MAX_SQL_BYTES,
                }),
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SqliteFamilyCapabilities {
    exact_integer_text_transport: bool,
    json_function_argument_limit: Option<usize>,
    statement_limits: Option<D1StatementLimits>,
}

impl SqliteFamilyCapabilities {
    pub(crate) fn write_parameter_expr(self, placeholder: &str, scalar: ScalarType) -> String {
        match scalar {
            ScalarType::Int | ScalarType::BigInt if self.exact_integer_text_transport => {
                format!("CAST({placeholder} AS INTEGER)")
            }
            ScalarType::Json => format!("json({placeholder})"),
            _ => placeholder.to_owned(),
        }
    }

    pub(crate) fn comparison_parameter_expr(self, placeholder: &str, scalar: ScalarType) -> String {
        match scalar {
            ScalarType::Int | ScalarType::BigInt if self.exact_integer_text_transport => {
                format!("CAST({placeholder} AS INTEGER)")
            }
            ScalarType::DateTime => format!("julianday({placeholder})"),
            ScalarType::Json => format!("json({placeholder})"),
            _ => placeholder.to_owned(),
        }
    }

    pub(crate) fn stored_column_expr(self, column_sql: &str, scalar: ScalarType) -> String {
        match scalar {
            ScalarType::DateTime => format!("julianday({column_sql})"),
            ScalarType::Json => format!("json({column_sql})"),
            _ => column_sql.to_owned(),
        }
    }

    pub(crate) fn result_column_expr(self, column_sql: &str, scalar: ScalarType) -> String {
        match scalar {
            ScalarType::Int | ScalarType::BigInt if self.exact_integer_text_transport => {
                format!("CAST({column_sql} AS TEXT)")
            }
            ScalarType::Json => format!("json({column_sql})"),
            _ => column_sql.to_owned(),
        }
    }

    pub(crate) fn nested_json_column_expr(self, column_sql: &str, scalar: ScalarType) -> String {
        match scalar {
            ScalarType::Int | ScalarType::BigInt if self.exact_integer_text_transport => {
                format!("CAST({column_sql} AS TEXT)")
            }
            ScalarType::Boolean => format!(
                "json(CASE WHEN {column_sql} IS NULL THEN NULL WHEN {column_sql} THEN 'true' ELSE 'false' END)"
            ),
            ScalarType::Bytes => {
                format!("CASE WHEN {column_sql} IS NULL THEN NULL ELSE hex({column_sql}) END")
            }
            ScalarType::Json => format!("json({column_sql})"),
            _ => column_sql.to_owned(),
        }
    }

    pub(crate) fn json_array_expr(self, items: &[String]) -> String {
        let initial_item_count = self
            .json_function_argument_limit
            .map_or(items.len(), |limit| items.len().min(limit));
        let mut expression = format!("json_array({})", items[..initial_item_count].join(", "));

        for item in &items[initial_item_count..] {
            expression = format!("json_insert({expression}, '$[#]', {item})");
        }

        expression
    }

    pub(crate) fn validate_statement(
        self,
        operation: OperationKind,
        sql: &str,
        binding_count: usize,
    ) -> Result<(), CompileError> {
        let Some(limits) = self.statement_limits else {
            return Ok(());
        };

        if binding_count > limits.max_bindings {
            return Err(CompileError::new(format!(
                "Cloudflare D1 {} operation compiled with {binding_count} bound parameters, exceeding the allowed limit of {}",
                operation_name(operation),
                limits.max_bindings,
            )));
        }

        let sql_bytes = sql.len();
        if sql_bytes > limits.max_sql_bytes {
            return Err(CompileError::new(format!(
                "Cloudflare D1 {} operation compiled to {sql_bytes} UTF-8 bytes of SQL, exceeding the allowed limit of {} bytes",
                operation_name(operation),
                limits.max_sql_bytes,
            )));
        }

        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct D1StatementLimits {
    max_bindings: usize,
    max_sql_bytes: usize,
}

fn operation_name(operation: OperationKind) -> &'static str {
    match operation {
        OperationKind::Query => "query",
        OperationKind::Insert => "insert",
        OperationKind::UpdateMany => "update-many",
        OperationKind::DeleteMany => "delete-many",
    }
}
