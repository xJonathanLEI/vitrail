use crate::filter::{FilterBuilder, compile_filter_sql, schema_model as resolve_schema_model};
use crate::flavor::{SqliteFamilyCapabilities, SqliteFamilyFlavor};
use crate::query::{QueryFilter, QueryVariableValue, QueryVariables, quoted_ident};
use crate::schema::{Model, ScalarType, Schema};
use crate::{BindingValue, CompileError, CompiledStatement, OperationKind};

pub fn compile_delete_many(
    schema: &Schema,
    model_name: &str,
    filter: Option<&QueryFilter>,
    variables: &QueryVariables,
) -> Result<CompiledStatement, CompileError> {
    compile_delete_many_with_flavor(
        schema,
        model_name,
        filter,
        variables,
        SqliteFamilyFlavor::Native,
    )
}

#[doc(hidden)]
pub fn compile_delete_many_with_flavor(
    schema: &Schema,
    model_name: &str,
    filter: Option<&QueryFilter>,
    variables: &QueryVariables,
    flavor: SqliteFamilyFlavor,
) -> Result<CompiledStatement, CompileError> {
    let model = resolve_schema_model(schema, model_name, "delete")?;
    let mut builder = DeleteSqlBuilder {
        schema,
        variables,
        capabilities: flavor.capabilities(),
        bindings: Vec::new(),
        next_alias: 1,
    };

    let where_clause = filter
        .map(|filter| builder.filter_sql(model, filter, "t0"))
        .transpose()?;

    let sql = format!(
        r#"DELETE FROM {} AS "t0"{}"#,
        quoted_ident(model.name()),
        where_clause
            .map(|where_clause| format!(" WHERE {where_clause}"))
            .unwrap_or_default(),
    );

    CompiledStatement::new(
        flavor,
        sql,
        builder.bindings,
        Vec::new(),
        OperationKind::DeleteMany,
    )
}

struct DeleteSqlBuilder<'a> {
    schema: &'a Schema,
    variables: &'a QueryVariables,
    capabilities: SqliteFamilyCapabilities,
    bindings: Vec<BindingValue>,
    next_alias: usize,
}

impl<'a> DeleteSqlBuilder<'a> {
    fn filter_sql(
        &mut self,
        model: &'a Model,
        filter: &QueryFilter,
        table_alias: &str,
    ) -> Result<String, CompileError> {
        compile_filter_sql(self, model, filter, table_alias)
    }

    fn push_query_binding(
        &mut self,
        value: QueryVariableValue,
        scalar: ScalarType,
    ) -> Result<String, CompileError> {
        self.bindings.push(match value {
            QueryVariableValue::Null => BindingValue::Null,
            QueryVariableValue::Int(value) => BindingValue::Int(value),
            QueryVariableValue::String(value) => BindingValue::String(value),
            QueryVariableValue::Bool(value) => BindingValue::Bool(value),
            QueryVariableValue::Float(value) => BindingValue::Float(value),
            QueryVariableValue::Bytes(value) => BindingValue::Bytes(value),
            QueryVariableValue::DateTime(value) => BindingValue::DateTime(value),
            QueryVariableValue::Json(value) => BindingValue::Json(value),
            QueryVariableValue::List(_) => {
                unreachable!("SQLite list filters must be expanded before compilation")
            }
        });
        let placeholder = format!("?{}", self.bindings.len());
        Ok(self
            .capabilities
            .comparison_parameter_expr(&placeholder, scalar))
    }
}

impl<'a> FilterBuilder<'a> for DeleteSqlBuilder<'a> {
    fn schema(&self) -> &'a Schema {
        self.schema
    }

    fn variables(&self) -> &'a QueryVariables {
        self.variables
    }

    fn capabilities(&self) -> SqliteFamilyCapabilities {
        self.capabilities
    }

    fn push_filter_binding(
        &mut self,
        value: QueryVariableValue,
        scalar: ScalarType,
    ) -> Result<String, CompileError> {
        self.push_query_binding(value, scalar)
    }

    fn next_filter_alias(&mut self) -> String {
        let alias = format!("t{}", self.next_alias);
        self.next_alias += 1;
        alias
    }

    fn operation_name(&self) -> &'static str {
        "delete"
    }
}
