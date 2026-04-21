use crate::query::{
    QueryFilter, QueryFilterValue, QueryFilterValues, QueryVariableValue, QueryVariables,
    column_expr, quoted_ident, schema_error,
};
use crate::schema::{Field, FieldType, Model, Resolution, ScalarType, Schema};

pub(crate) trait FilterBuilder<'a> {
    fn schema(&self) -> &'a Schema;
    fn variables(&self) -> &'a QueryVariables;
    fn push_filter_binding(
        &mut self,
        value: QueryVariableValue,
        scalar: ScalarType,
    ) -> Result<String, sqlx::Error>;
    fn next_filter_alias(&mut self) -> String;
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
    model: &'a Model,
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
                QueryFilter::Eq { .. } => {
                    if matches!(binding, QueryVariableValue::Null) {
                        Ok(format!(
                            "\"{table_alias}\".{} IS NULL",
                            quoted_ident(field.name())
                        ))
                    } else {
                        let placeholder = builder.push_filter_binding(binding, scalar)?;
                        Ok(format!(
                            "{} = {}",
                            column_expr(table_alias, field.name(), scalar),
                            placeholder
                        ))
                    }
                }
                QueryFilter::Ne { .. } => {
                    if matches!(binding, QueryVariableValue::Null) {
                        Ok(format!(
                            "\"{table_alias}\".{} IS NOT NULL",
                            quoted_ident(field.name())
                        ))
                    } else {
                        let placeholder = builder.push_filter_binding(binding, scalar)?;
                        Ok(format!(
                            "{} <> {}",
                            column_expr(table_alias, field.name(), scalar),
                            placeholder
                        ))
                    }
                }
                _ => unreachable!("handled by outer match"),
            }
        }
        QueryFilter::In { field, values } => {
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

            let bindings = resolve_filter_values(builder.variables(), values)?;
            if bindings.is_empty() {
                return Err(schema_error(format!(
                    "`in` filter for field `{}.{}` requires at least one value",
                    model.name(),
                    field.name()
                )));
            }

            if !query_values_match_field(&bindings, field) {
                return Err(schema_error(format!(
                    "filter values for field `{}.{}` are incompatible with schema type `{}`",
                    model.name(),
                    field.name(),
                    field.ty().name()
                )));
            }

            if bindings.iter().any(|binding| {
                matches!(
                    binding,
                    QueryVariableValue::Null | QueryVariableValue::List(_)
                )
            }) {
                return Err(schema_error(format!(
                    "`in` filter for field `{}.{}` only supports non-null scalar values",
                    model.name(),
                    field.name()
                )));
            }

            let placeholder =
                builder.push_filter_binding(QueryVariableValue::List(bindings), scalar)?;

            Ok(format!(
                "{} = ANY({})",
                column_expr(table_alias, field.name(), scalar),
                placeholder
            ))
        }
        QueryFilter::Relation { field, filter } => {
            let relation_field = model.field_named(field).ok_or_else(|| {
                schema_error(format!(
                    "unknown relation `{}.{}` in {} filter",
                    model.name(),
                    field,
                    builder.operation_name()
                ))
            })?;

            if relation_field.kind().is_scalar() {
                return Err(schema_error(format!(
                    "field `{}.{}` is not a relation and cannot appear in {} `where`",
                    model.name(),
                    relation_field.name(),
                    builder.operation_name()
                )));
            }

            let target_model = schema_model(
                builder.schema(),
                relation_field.ty().name(),
                builder.operation_name(),
            )?;
            let (nested_fields, parent_fields) =
                relation_fields(model, relation_field, target_model)?;

            let nested_alias = builder.next_filter_alias();
            let nested_filter = compile_filter_sql(builder, target_model, filter, &nested_alias)?;
            let relation_predicate =
                relation_predicates(&nested_alias, &nested_fields, table_alias, &parent_fields);

            Ok(format!(
                "EXISTS (SELECT 1 FROM {} AS \"{}\" WHERE {} AND {})",
                quoted_ident(target_model.name()),
                nested_alias,
                relation_predicate,
                nested_filter,
            ))
        }
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

fn resolve_filter_values(
    variables: &QueryVariables,
    values: &QueryFilterValues,
) -> Result<Vec<QueryVariableValue>, sqlx::Error> {
    match values {
        QueryFilterValues::Values(values) => values
            .iter()
            .map(|value| resolve_filter_value(variables, value))
            .collect(),
        QueryFilterValues::Variable(name) => {
            let value = variables
                .get(name)
                .cloned()
                .ok_or_else(|| schema_error(format!("missing query variable `{name}`")))?;

            match value {
                QueryVariableValue::List(values) => Ok(values),
                value => Err(schema_error(format!(
                    "query variable `{name}` must be a list for `in` filters, got `{}`",
                    match value {
                        QueryVariableValue::Null => "null",
                        QueryVariableValue::Int(_) => "int",
                        QueryVariableValue::String(_) => "string",
                        QueryVariableValue::Bool(_) => "bool",
                        QueryVariableValue::Float(_) => "float",
                        QueryVariableValue::Decimal(_) => "decimal",
                        QueryVariableValue::Bytes(_) => "bytes",
                        QueryVariableValue::DateTime(_) => "datetime",
                        QueryVariableValue::Uuid(_) => "uuid",
                        QueryVariableValue::List(_) => unreachable!(),
                    }
                ))),
            }
        }
    }
}

fn model_names_match(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn infer_relation_fields<'a>(
    model: &'a Model,
    field: &'a Field,
    target_model: &'a Model,
) -> Result<(Vec<&'a str>, Vec<&'a str>), sqlx::Error> {
    let reverse_relation = target_model
        .fields()
        .iter()
        .find(|candidate| {
            model_names_match(candidate.ty().name(), model.name()) && candidate.relation().is_some()
        })
        .ok_or_else(|| {
            schema_error(format!(
                "could not infer relation metadata for `{}.{}`",
                model.name(),
                field.name()
            ))
        })?;

    let reverse_relation = reverse_relation
        .relation()
        .expect("reverse relation existence checked above");

    Ok((
        reverse_relation
            .fields()
            .iter()
            .map(String::as_str)
            .collect(),
        reverse_relation
            .references()
            .iter()
            .map(String::as_str)
            .collect(),
    ))
}

fn relation_fields<'a>(
    model: &'a Model,
    field: &'a Field,
    target_model: &'a Model,
) -> Result<(Vec<&'a str>, Vec<&'a str>), sqlx::Error> {
    match field.relation() {
        Some(relation_info) => Ok((
            relation_info
                .references()
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            relation_info
                .fields()
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
        )),
        None => infer_relation_fields(model, field, target_model),
    }
}

fn relation_predicates(
    nested_alias: &str,
    nested_fields: &[&str],
    parent_alias: &str,
    parent_fields: &[&str],
) -> String {
    nested_fields
        .iter()
        .zip(parent_fields)
        .map(|(nested_field, parent_field)| {
            format!(
                "\"{nested_alias}\".{} = \"{parent_alias}\".{}",
                quoted_ident(nested_field),
                quoted_ident(parent_field),
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ")
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
        QueryVariableValue::String(_) => {
            scalar.scalar() == ScalarType::String && !field.has_db_uuid()
        }
        QueryVariableValue::Bool(_) => scalar.scalar() == ScalarType::Boolean,
        QueryVariableValue::Float(_) => scalar.scalar() == ScalarType::Float,
        QueryVariableValue::Decimal(_) => scalar.scalar() == ScalarType::Decimal,
        QueryVariableValue::Bytes(_) => scalar.scalar() == ScalarType::Bytes,
        QueryVariableValue::DateTime(_) => scalar.scalar() == ScalarType::DateTime,
        QueryVariableValue::Uuid(_) => scalar.scalar() == ScalarType::String && field.has_db_uuid(),
        QueryVariableValue::List(_) => false,
    }
}

fn query_values_match_field(values: &[QueryVariableValue], field: &Field) -> bool {
    let Some(first) = values.first() else {
        return true;
    };

    if matches!(
        first,
        QueryVariableValue::Null | QueryVariableValue::List(_)
    ) {
        return false;
    }

    values.iter().all(|value| {
        matches!(
            (first, value),
            (QueryVariableValue::Int(_), QueryVariableValue::Int(_))
                | (QueryVariableValue::String(_), QueryVariableValue::String(_))
                | (QueryVariableValue::Bool(_), QueryVariableValue::Bool(_))
                | (QueryVariableValue::Float(_), QueryVariableValue::Float(_))
                | (
                    QueryVariableValue::Decimal(_),
                    QueryVariableValue::Decimal(_)
                )
                | (QueryVariableValue::Bytes(_), QueryVariableValue::Bytes(_))
                | (
                    QueryVariableValue::DateTime(_),
                    QueryVariableValue::DateTime(_)
                )
                | (QueryVariableValue::Uuid(_), QueryVariableValue::Uuid(_))
        ) && query_value_matches_field(value, field)
    })
}
