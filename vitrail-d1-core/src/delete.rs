use std::marker::PhantomData;

use vitrail_sqlite_dialect::{
    CompiledStatement, SqliteFamilyFlavor, compile_delete_many_with_flavor,
};
use worker::d1::D1Database;

use crate::query::{
    BoxFuture, QueryFilter, QueryVariableSet, QueryVariables, dialect_filter, dialect_variables,
};
use crate::statement::execute_changes;
use crate::{Error, Schema, SchemaAccess};

/// Runtime contract implemented by executable D1 delete values.
pub trait DeleteSpec: Send + Sync {
    type Output: Send + 'static;

    #[doc(hidden)]
    fn execute<'a>(
        &'a self,
        database: &'a D1Database,
    ) -> BoxFuture<'a, Result<Self::Output, Error>>;
}

/// Runtime contract implemented by D1 bulk delete models.
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

/// Executable D1 bulk delete returning the number of affected rows.
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

    pub fn to_sql(&self) -> Result<String, Error> {
        let filter = self.filter();

        Ok(compile_delete_statement(
            S::schema(),
            T::model_name(),
            filter.as_ref(),
            &self.variables,
        )?
        .sql()
        .to_owned())
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
        database: &'a D1Database,
    ) -> BoxFuture<'a, Result<Self::Output, Error>> {
        Box::pin(async move {
            let filter = self.filter();
            let statement = compile_delete_statement(
                S::schema(),
                T::model_name(),
                filter.as_ref(),
                &self.variables,
            )?;

            execute_changes(database, &statement).await
        })
    }
}

pub(crate) fn compile_delete_statement(
    schema: &Schema,
    model_name: &str,
    filter: Option<&QueryFilter>,
    variables: &QueryVariables,
) -> Result<CompiledStatement, Error> {
    let dialect_filter = filter.map(dialect_filter);
    let dialect_variables = dialect_variables(variables)?;

    compile_delete_many_with_flavor(
        schema,
        model_name,
        dialect_filter.as_ref(),
        &dialect_variables,
        SqliteFamilyFlavor::D1,
    )
    .map_err(Error::from)
}
