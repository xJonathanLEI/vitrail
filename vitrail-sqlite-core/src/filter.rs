use crate::query::{
    QueryFilter, QueryFilterValue, QueryVariableValue, QueryVariables, column_expr, quoted_ident,
    schema_error,
};
use crate::schema::{Field, FieldType, Model, Resolution, ScalarType, Schema};

pub(crate) trait FilterBuilder<'a> {
    fn variables(&self) -> &'a QueryVariables;

    fn push_filter_binding(
        &mut self,
        value: QueryVariableValue,
        scalar: ScalarType,
    ) -> Result<String, sqlx::Error>;

    fn operation_name(&self) -> &'static str;
}

pub(crate) fn schema_model<'a>(
    schema: &'a Schema,
    requested: &str,
    operation: &str,
) -> Result<&'a Model, sqlx::Error> {
    match schema.resolve_model(requested) {
        Resolution::Found(model) => Ok(model),
        Resolution::NotFound => Err(schema_error(format!(
            "unknown model `{requested}` in {operation}"
        ))),
        Resolution::Ambiguous(models) => {
            let candidates = models
                .into_iter()
                .map(|model| format!("`{}`", model.name()))
                .collect::<Vec<_>>()
                .join(", ");

            Err(schema_error(format!(
                "ambiguous model `{requested}` in {operation}; matches {candidates}"
            )))
        }
    }
}

pub(crate) fn compile_filter_sql<'a>(
    builder: &mut impl FilterBuilder<'a>,
    model: &Model,
    filter: &QueryFilter,
    table_alias: &str,
) -> Result<String, sqlx::Error> {
    match filter {
        QueryFilter::And(filters) => {
            let parts = filters
                .iter()
                .map(|filter| compile_filter_sql(builder, model, filter, table_alias))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("({})", parts.join(" AND ")))
        }
        QueryFilter::Or(filters) => {
            let parts = filters
                .iter()
                .map(|filter| compile_filter_sql(builder, model, filter, table_alias))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("({})", parts.join(" OR ")))
        }
        QueryFilter::Not(filter) => Ok(format!(
            "NOT ({})",
            compile_filter_sql(builder, model, filter, table_alias)?
        )),
        QueryFilter::Eq { field, value } | QueryFilter::Ne { field, value } => {
            let field = model.field_named(field).ok_or_else(|| {
                schema_error(format!(
                    "unknown field `{}.{}` in {} filter",
                    model.name(),
                    field,
                    builder.operation_name()
                ))
            })?;

            let scalar = match field.ty() {
                FieldType::Scalar(scalar) => scalar.scalar(),
                FieldType::Relation { .. } => {
                    return Err(schema_error(format!(
                        "field `{}.{}` is not scalar and cannot appear in {} `where`",
                        model.name(),
                        field.name(),
                        builder.operation_name()
                    )));
                }
            };

            let binding = resolve_filter_value(builder.variables(), value)?;
            if !query_value_matches_field(&binding, field) {
                return Err(schema_error(format!(
                    "filter value for field `{}.{}` is incompatible with schema type `{}`",
                    model.name(),
                    field.name(),
                    field.ty().name()
                )));
            }

            match filter {
                QueryFilter::Eq { .. } if matches!(binding, QueryVariableValue::Null) => Ok(
                    format!("\"{table_alias}\".{} IS NULL", quoted_ident(field.name())),
                ),
                QueryFilter::Ne { .. } if matches!(binding, QueryVariableValue::Null) => {
                    Ok(format!(
                        "\"{table_alias}\".{} IS NOT NULL",
                        quoted_ident(field.name())
                    ))
                }
                QueryFilter::Eq { .. } => {
                    let placeholder = builder.push_filter_binding(binding, scalar)?;
                    Ok(format!(
                        "{} = {}",
                        column_expr(table_alias, field.name(), scalar),
                        placeholder
                    ))
                }
                QueryFilter::Ne { .. } => {
                    let placeholder = builder.push_filter_binding(binding, scalar)?;
                    Ok(format!(
                        "{} <> {}",
                        column_expr(table_alias, field.name(), scalar),
                        placeholder
                    ))
                }
                _ => unreachable!("handled by outer match"),
            }
        }
        QueryFilter::In { field, .. } => Err(schema_error(format!(
            "`in` filters for field `{}.{field}` are not supported by SQLite read queries yet",
            model.name()
        ))),
        QueryFilter::Relation { field, .. } => Err(schema_error(format!(
            "relation filters through `{}.{field}` are not supported by SQLite read queries yet",
            model.name()
        ))),
    }
}

fn resolve_filter_value(
    variables: &QueryVariables,
    value: &QueryFilterValue,
) -> Result<QueryVariableValue, sqlx::Error> {
    match value {
        QueryFilterValue::Value(value) => Ok(value.clone()),
        QueryFilterValue::Variable(name) => variables
            .get(name)
            .cloned()
            .ok_or_else(|| schema_error(format!("missing query variable `{name}`"))),
    }
}

fn query_value_matches_field(value: &QueryVariableValue, field: &Field) -> bool {
    let FieldType::Scalar(scalar) = field.ty() else {
        return false;
    };

    match value {
        QueryVariableValue::Null => field.ty().is_optional(),
        QueryVariableValue::Int(_) => {
            matches!(scalar.scalar(), ScalarType::Int | ScalarType::BigInt)
        }
        QueryVariableValue::String(_) => scalar.scalar() == ScalarType::String,
        QueryVariableValue::Bool(_) => scalar.scalar() == ScalarType::Boolean,
        QueryVariableValue::Float(_) => scalar.scalar() == ScalarType::Float,
        QueryVariableValue::Bytes(_) => scalar.scalar() == ScalarType::Bytes,
        QueryVariableValue::DateTime(_) => scalar.scalar() == ScalarType::DateTime,
        QueryVariableValue::Json(_) => scalar.scalar() == ScalarType::Json,
        QueryVariableValue::List(_) => false,
    }
}
