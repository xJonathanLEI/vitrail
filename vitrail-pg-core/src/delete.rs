use std::marker::PhantomData;

use sqlx::postgres::PgArguments;
use sqlx::{Postgres, query::Query as SqlxQuery};

use crate::PgExecutor;
use crate::filter::{FilterBuilder, compile_filter_sql, schema_model as resolve_schema_model};
use crate::query::{
    BoxFuture, QueryFilter, QueryVariableSet, QueryVariableValue, QueryVariables, SchemaAccess,
    quoted_ident,
};
use crate::schema::{Model, ScalarType, Schema};

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
    let model = resolve_schema_model(schema, model_name, "delete")?;
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
        compile_filter_sql(self, model, filter, table_alias)
    }

    fn push_query_binding(
        &mut self,
        value: QueryVariableValue,
        _scalar: ScalarType,
    ) -> Result<String, sqlx::Error> {
        self.bindings.push(value);
        Ok(format!("${}", self.bindings.len()))
    }
}

impl<'a> FilterBuilder<'a> for DeleteSqlBuilder<'a> {
    fn schema(&self) -> &'a Schema {
        self.schema
    }

    fn variables(&self) -> &'a QueryVariables {
        self.variables
    }

    fn push_filter_binding(
        &mut self,
        value: QueryVariableValue,
        scalar: ScalarType,
    ) -> Result<String, sqlx::Error> {
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
            QueryVariableValue::List(values) => {
                let first = values
                    .first()
                    .expect("list-valued query variables must not be empty when bound");

                match first {
                    QueryVariableValue::Null => {
                        unreachable!("list-valued query variables must not contain null items")
                    }
                    QueryVariableValue::Int(_) => query.bind(
                        values
                            .iter()
                            .map(|value| match value {
                                QueryVariableValue::Int(value) => *value,
                                _ => unreachable!("list-valued query variables must be homogenous"),
                            })
                            .collect::<Vec<_>>(),
                    ),
                    QueryVariableValue::String(_) => query.bind(
                        values
                            .iter()
                            .map(|value| match value {
                                QueryVariableValue::String(value) => value.clone(),
                                _ => unreachable!("list-valued query variables must be homogenous"),
                            })
                            .collect::<Vec<_>>(),
                    ),
                    QueryVariableValue::Bool(_) => query.bind(
                        values
                            .iter()
                            .map(|value| match value {
                                QueryVariableValue::Bool(value) => *value,
                                _ => unreachable!("list-valued query variables must be homogenous"),
                            })
                            .collect::<Vec<_>>(),
                    ),
                    QueryVariableValue::Float(_) => query.bind(
                        values
                            .iter()
                            .map(|value| match value {
                                QueryVariableValue::Float(value) => *value,
                                _ => unreachable!("list-valued query variables must be homogenous"),
                            })
                            .collect::<Vec<_>>(),
                    ),
                    QueryVariableValue::Decimal(_) => query.bind(
                        values
                            .iter()
                            .map(|value| match value {
                                QueryVariableValue::Decimal(value) => *value,
                                _ => unreachable!("list-valued query variables must be homogenous"),
                            })
                            .collect::<Vec<_>>(),
                    ),
                    QueryVariableValue::Bytes(_) => query.bind(
                        values
                            .iter()
                            .map(|value| match value {
                                QueryVariableValue::Bytes(value) => value.clone(),
                                _ => unreachable!("list-valued query variables must be homogenous"),
                            })
                            .collect::<Vec<_>>(),
                    ),
                    QueryVariableValue::DateTime(_) => query.bind(
                        values
                            .iter()
                            .map(|value| match value {
                                QueryVariableValue::DateTime(value) => *value,
                                _ => unreachable!("list-valued query variables must be homogenous"),
                            })
                            .collect::<Vec<_>>(),
                    ),
                    QueryVariableValue::Uuid(_) => query.bind(
                        values
                            .iter()
                            .map(|value| match value {
                                QueryVariableValue::Uuid(value) => *value,
                                _ => unreachable!("list-valued query variables must be homogenous"),
                            })
                            .collect::<Vec<_>>(),
                    ),
                    QueryVariableValue::List(_) => {
                        unreachable!("list-valued query variables must not contain nested lists")
                    }
                }
            }
        };
    }

    query
}
