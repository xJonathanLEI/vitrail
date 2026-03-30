use std::marker::PhantomData;

use sqlx::postgres::PgArguments;
use sqlx::{Postgres, query::Query as SqlxQuery};

use crate::PgExecutor;
use crate::query::{
    BoxFuture, QueryFilter, QueryFilterValue, QueryVariableSet, QueryVariableValue, QueryVariables,
    SchemaAccess, column_expr, quoted_ident, schema_error,
};
use crate::schema::{Field, FieldType, Model, Resolution, ScalarType, Schema};

/// Runtime contract implemented by executable delete values.
pub trait DeleteSpec: Send + Sync {
    type Output: Send + 'static;

    #[doc(hidden)]
    fn execute<'a>(
        &'a self,
        executor: &'a dyn PgExecutor,
    ) -> BoxFuture<'a, Result<Self::Output, sqlx::Error>>;
}

/// Runtime contract implemented by bulk delete models.
pub trait DeleteManyModel: Sized + Send + 'static {
    type Schema: SchemaAccess;
    type Variables: QueryVariableSet;

    fn model_name() -> &'static str;

    fn filter() -> Option<QueryFilter> {
        None
    }

    fn filter_with_variables(_variables: &QueryVariables) -> Option<QueryFilter> {
        Self::filter()
    }
}

/// Executable bulk delete returning the number of affected rows.
#[derive(Clone, Debug)]
pub struct DeleteMany<S, T, V = ()> {
    variables: QueryVariables,
    _marker: PhantomData<(S, T, V)>,
}

impl<S, T> DeleteMany<S, T, ()>
where
    T: DeleteManyModel<Variables = ()>,
{
    pub fn new() -> Self {
        Self {
            variables: QueryVariables::new(),
            _marker: PhantomData,
        }
    }
}

impl<S, T> Default for DeleteMany<S, T, ()>
where
    T: DeleteManyModel<Variables = ()>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<S, T> DeleteMany<S, T, ()>
where
    T: DeleteManyModel,
{
    pub fn new_with_variables(variables: T::Variables) -> DeleteMany<S, T, T::Variables> {
        DeleteMany {
            variables: variables.into_query_variables(),
            _marker: PhantomData,
        }
    }

    pub fn with_variables(self, variables: T::Variables) -> DeleteMany<S, T, T::Variables> {
        DeleteMany {
            variables: variables.into_query_variables(),
            _marker: PhantomData,
        }
    }
}

impl<S, T, V> DeleteMany<S, T, V>
where
    S: SchemaAccess,
    T: DeleteManyModel<Schema = S, Variables = V>,
    V: QueryVariableSet,
{
    fn filter(&self) -> Option<QueryFilter> {
        T::filter_with_variables(&self.variables)
    }

    pub fn to_sql(&self) -> Result<String, sqlx::Error> {
        let filter = self.filter();
        let (sql, _) = build_delete_many_sql(
            S::schema(),
            T::model_name(),
            filter.as_ref(),
            &self.variables,
        )?;
        Ok(sql)
    }
}

impl<S, T, V> DeleteSpec for DeleteMany<S, T, V>
where
    S: SchemaAccess,
    T: DeleteManyModel<Schema = S, Variables = V> + Sync,
    V: QueryVariableSet + Sync,
{
    type Output = u64;

    fn execute<'a>(
        &'a self,
        executor: &'a dyn PgExecutor,
    ) -> BoxFuture<'a, Result<Self::Output, sqlx::Error>> {
        Box::pin(async move {
            let filter = self.filter();
            let (sql, bindings) = build_delete_many_sql(
                S::schema(),
                T::model_name(),
                filter.as_ref(),
                &self.variables,
            )?;
            let result = executor
                .execute(bind_delete(sqlx::query(&sql), &bindings))
                .await?;
            Ok(result.rows_affected())
        })
    }
}

fn build_delete_many_sql(
    schema: &Schema,
    model_name: &str,
    filter: Option<&QueryFilter>,
    variables: &QueryVariables,
) -> Result<(String, Vec<QueryVariableValue>), sqlx::Error> {
    let model = schema_model(schema, model_name)?;
    let mut builder = DeleteSqlBuilder {
        schema,
        variables,
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

    Ok((sql, builder.bindings))
}

fn schema_model<'a>(schema: &'a Schema, requested: &str) -> Result<&'a Model, sqlx::Error> {
    match schema.resolve_model(requested) {
        Resolution::Found(model) => Ok(model),
        Resolution::NotFound => Err(schema_error(format!(
            "unknown model `{requested}` in delete"
        ))),
        Resolution::Ambiguous(models) => {
            let candidates = models
                .into_iter()
                .map(|model| format!("`{}`", model.name()))
                .collect::<Vec<_>>()
                .join(", ");

            Err(schema_error(format!(
                "ambiguous model `{requested}` in delete; matches {candidates}"
            )))
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

struct DeleteSqlBuilder<'a> {
    schema: &'a Schema,
    variables: &'a QueryVariables,
    bindings: Vec<QueryVariableValue>,
    next_alias: usize,
}

impl<'a> DeleteSqlBuilder<'a> {
    fn filter_sql(
        &mut self,
        model: &'a Model,
        filter: &QueryFilter,
        table_alias: &str,
    ) -> Result<String, sqlx::Error> {
        match filter {
            QueryFilter::And(filters) => {
                let parts = filters
                    .iter()
                    .map(|filter| self.filter_sql(model, filter, table_alias))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(format!("({})", parts.join(" AND ")))
            }
            QueryFilter::Or(filters) => {
                let parts = filters
                    .iter()
                    .map(|filter| self.filter_sql(model, filter, table_alias))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(format!("({})", parts.join(" OR ")))
            }
            QueryFilter::Not(filter) => Ok(format!(
                "NOT ({})",
                self.filter_sql(model, filter, table_alias)?
            )),
            QueryFilter::Eq { field, value } | QueryFilter::Ne { field, value } => {
                let field = model.field_named(field).ok_or_else(|| {
                    schema_error(format!(
                        "unknown field `{}.{}` in delete filter",
                        model.name(),
                        field
                    ))
                })?;

                let scalar = match field.ty() {
                    FieldType::Scalar(scalar) => scalar.scalar(),
                    FieldType::Relation { .. } => {
                        return Err(schema_error(format!(
                            "field `{}.{}` is not scalar and cannot appear in delete `where`",
                            model.name(),
                            field.name()
                        )));
                    }
                };

                let binding = self.resolve_filter_value(value)?;

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
                            let placeholder = self.push_query_binding(binding, scalar)?;
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
                            let placeholder = self.push_query_binding(binding, scalar)?;
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
            QueryFilter::Relation { field, filter } => {
                let relation_field = model.field_named(field).ok_or_else(|| {
                    schema_error(format!(
                        "unknown relation `{}.{}` in delete filter",
                        model.name(),
                        field
                    ))
                })?;

                if relation_field.kind().is_scalar() {
                    return Err(schema_error(format!(
                        "field `{}.{}` is not a relation and cannot appear as a nested delete filter",
                        model.name(),
                        relation_field.name()
                    )));
                }

                let target_model = schema_model(self.schema, relation_field.ty().name())?;
                let (nested_fields, parent_fields) =
                    self.relation_fields(model, relation_field, target_model)?;

                if nested_fields.len() != 1 || parent_fields.len() != 1 {
                    return Err(schema_error(format!(
                        "relation `{}.{}` currently requires exactly one parent field and one nested field",
                        model.name(),
                        relation_field.name()
                    )));
                }

                let nested_alias = format!("t{}", self.next_alias);
                self.next_alias += 1;
                let nested_filter = self.filter_sql(target_model, filter, &nested_alias)?;

                Ok(format!(
                    "EXISTS (SELECT 1 FROM {} AS \"{}\" WHERE \"{}\".{} = \"{}\".{} AND {})",
                    quoted_ident(target_model.name()),
                    nested_alias,
                    nested_alias,
                    quoted_ident(nested_fields[0]),
                    table_alias,
                    quoted_ident(parent_fields[0]),
                    nested_filter,
                ))
            }
        }
    }

    fn resolve_filter_value(
        &self,
        value: &QueryFilterValue,
    ) -> Result<QueryVariableValue, sqlx::Error> {
        match value {
            QueryFilterValue::Value(value) => Ok(value.clone()),
            QueryFilterValue::Variable(name) => self
                .variables
                .get(name)
                .cloned()
                .ok_or_else(|| schema_error(format!("missing query variable `{name}`"))),
        }
    }

    fn push_query_binding(
        &mut self,
        value: QueryVariableValue,
        _scalar: ScalarType,
    ) -> Result<String, sqlx::Error> {
        self.bindings.push(value);
        Ok(format!("${}", self.bindings.len()))
    }

    fn relation_fields(
        &self,
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
}

fn query_value_matches_field(value: &QueryVariableValue, field: &Field) -> bool {
    let FieldType::Scalar(scalar) = field.ty() else {
        return false;
    };

    match value {
        QueryVariableValue::Null => scalar.optional(),
        QueryVariableValue::Int(_) => scalar.scalar() == ScalarType::Int,
        QueryVariableValue::String(_) => {
            scalar.scalar() == ScalarType::String && !field.has_db_uuid()
        }
        QueryVariableValue::Bool(_) => scalar.scalar() == ScalarType::Boolean,
        QueryVariableValue::Float(_) => scalar.scalar() == ScalarType::Float,
        QueryVariableValue::Decimal(_) => scalar.scalar() == ScalarType::Decimal,
        QueryVariableValue::Bytes(_) => scalar.scalar() == ScalarType::Bytes,
        QueryVariableValue::DateTime(_) => scalar.scalar() == ScalarType::DateTime,
        QueryVariableValue::Uuid(_) => scalar.scalar() == ScalarType::String && field.has_db_uuid(),
    }
}

fn bind_delete<'q>(
    mut query: SqlxQuery<'q, Postgres, PgArguments>,
    bindings: &'q [QueryVariableValue],
) -> SqlxQuery<'q, Postgres, PgArguments> {
    for binding in bindings {
        query = match binding {
            QueryVariableValue::Null => query.bind(Option::<i64>::None),
            QueryVariableValue::Int(value) => query.bind(*value),
            QueryVariableValue::String(value) => query.bind(value),
            QueryVariableValue::Bool(value) => query.bind(*value),
            QueryVariableValue::Float(value) => query.bind(*value),
            QueryVariableValue::Decimal(value) => query.bind(*value),
            QueryVariableValue::Bytes(value) => query.bind(value),
            QueryVariableValue::DateTime(value) => query.bind(*value),
            QueryVariableValue::Uuid(value) => query.bind(*value),
        };
    }

    query
}
