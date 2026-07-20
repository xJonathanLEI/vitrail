use std::marker::PhantomData;

use crate::SqliteExecutor;
use crate::query::{
    BoxFuture, QueryFilter, QueryVariableSet, QueryVariables, dialect_filter, dialect_variables,
};
use crate::schema::{Schema, SchemaAccess};
use crate::statement::{bind_statement, map_compile_error};

/// Runtime contract implemented by executable delete values.
pub trait DeleteSpec: Send + Sync {
    type Output: Send + 'static;

    #[doc(hidden)]
    fn execute<'a>(
        &'a self,
        executor: &'a dyn SqliteExecutor,
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
        executor: &'a dyn SqliteExecutor,
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
                .execute(bind_statement(sqlx::query(&sql), &bindings))
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
) -> Result<(String, Vec<vitrail_sqlite_dialect::BindingValue>), sqlx::Error> {
    let dialect_filter = filter.map(dialect_filter);
    let dialect_variables = dialect_variables(variables)?;
    let statement = vitrail_sqlite_dialect::compile_delete_many(
        schema.as_dialect(),
        model_name,
        dialect_filter.as_ref(),
        &dialect_variables,
    )
    .map_err(map_compile_error)?;
    let (sql, bindings, _, _) = statement.into_parts();
    Ok((sql, bindings))
}
