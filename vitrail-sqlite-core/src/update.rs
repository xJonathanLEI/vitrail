use std::collections::HashMap;
use std::marker::PhantomData;

use serde_json::Value as JsonValue;

use crate::SqliteExecutor;
use crate::query::{
    BoxFuture, QueryFilter, QueryVariableSet, QueryVariables, StringValueType, dialect_filter,
    dialect_variables, schema_error,
};
use crate::schema::{Schema, SchemaAccess};
use crate::statement::{bind_statement, map_compile_error};

/// Runtime contract implemented by executable update values.
pub trait UpdateSpec: Send + Sync {
    type Output: Send + 'static;

    #[doc(hidden)]
    fn execute<'a>(
        &'a self,
        executor: &'a dyn SqliteExecutor,
    ) -> BoxFuture<'a, Result<Self::Output, sqlx::Error>>;
}

/// Runtime contract implemented by bulk update models.
pub trait UpdateManyModel: Sized + Send + 'static {
    type Schema: SchemaAccess;
    type Values: UpdateValueSet;
    type Variables: QueryVariableSet;

    fn model_name() -> &'static str;

    fn filter() -> Option<QueryFilter> {
        None
    }

    fn filter_with_variables(_variables: &QueryVariables) -> Option<QueryFilter> {
        Self::filter()
    }
}

/// Converts a user-provided input into executable update values.
pub trait UpdateValueSet: Send + 'static {
    fn into_update_values(self) -> UpdateValues;
}

impl UpdateValueSet for UpdateValues {
    fn into_update_values(self) -> UpdateValues {
        self
    }
}

impl UpdateValueSet for () {
    fn into_update_values(self) -> UpdateValues {
        UpdateValues::new()
    }
}

/// Converts a supported SQLite scalar into an update value.
pub trait UpdateScalar: Send {
    fn into_update_value(self) -> UpdateValue;
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct UpdateValues {
    values: Vec<UpdateFieldValue>,
    value_indices: HashMap<String, usize>,
}

impl UpdateValues {
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            value_indices: HashMap::new(),
        }
    }

    pub fn from_values(values: Vec<(impl Into<String>, UpdateValue)>) -> Self {
        let mut update_values = Self::new();

        for (name, value) in values {
            update_values
                .push(name, value)
                .expect("update field names must be unique");
        }

        update_values
    }

    pub fn push(
        &mut self,
        name: impl Into<String>,
        value: UpdateValue,
    ) -> Result<usize, sqlx::Error> {
        let name = name.into();

        if self.value_indices.contains_key(&name) {
            return Err(schema_error(format!("duplicate update field `{name}`")));
        }

        let index = self.values.len();
        self.values.push(UpdateFieldValue {
            name: name.clone(),
            value,
        });
        self.value_indices.insert(name, index);
        Ok(index)
    }

    pub fn get(&self, name: &str) -> Option<&UpdateValue> {
        self.value_indices
            .get(name)
            .and_then(|index| self.values.get(*index))
            .map(|field| &field.value)
    }

    pub fn iter(&self) -> impl Iterator<Item = &UpdateFieldValue> {
        self.values.iter()
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct UpdateFieldValue {
    pub name: String,
    pub value: UpdateValue,
}

#[derive(Clone, Debug, PartialEq)]
pub enum UpdateValue {
    Null,
    Int(i64),
    String(String),
    Bool(bool),
    Float(f64),
    Bytes(Vec<u8>),
    DateTime(chrono::DateTime<chrono::Utc>),
    Json(JsonValue),
}

impl From<i64> for UpdateValue {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<String> for UpdateValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for UpdateValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<bool> for UpdateValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<f64> for UpdateValue {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<Vec<u8>> for UpdateValue {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value)
    }
}

impl From<&[u8]> for UpdateValue {
    fn from(value: &[u8]) -> Self {
        Self::Bytes(value.to_vec())
    }
}

impl From<chrono::DateTime<chrono::Utc>> for UpdateValue {
    fn from(value: chrono::DateTime<chrono::Utc>) -> Self {
        Self::DateTime(value)
    }
}

impl From<JsonValue> for UpdateValue {
    fn from(value: JsonValue) -> Self {
        Self::Json(value)
    }
}

impl<T> From<Option<T>> for UpdateValue
where
    T: Into<UpdateValue>,
{
    fn from(value: Option<T>) -> Self {
        match value {
            Some(value) => value.into(),
            None => Self::Null,
        }
    }
}

impl UpdateScalar for i64 {
    fn into_update_value(self) -> UpdateValue {
        self.into()
    }
}

impl UpdateScalar for &str {
    fn into_update_value(self) -> UpdateValue {
        self.into()
    }
}

impl UpdateScalar for bool {
    fn into_update_value(self) -> UpdateValue {
        self.into()
    }
}

impl UpdateScalar for f64 {
    fn into_update_value(self) -> UpdateValue {
        self.into()
    }
}

impl UpdateScalar for Vec<u8> {
    fn into_update_value(self) -> UpdateValue {
        self.into()
    }
}

impl UpdateScalar for &[u8] {
    fn into_update_value(self) -> UpdateValue {
        self.into()
    }
}

impl UpdateScalar for chrono::DateTime<chrono::Utc> {
    fn into_update_value(self) -> UpdateValue {
        self.into()
    }
}

impl UpdateScalar for JsonValue {
    fn into_update_value(self) -> UpdateValue {
        self.into()
    }
}

impl<T> UpdateScalar for T
where
    T: StringValueType,
{
    fn into_update_value(self) -> UpdateValue {
        UpdateValue::String(self.into_db_string())
    }
}

impl<T> UpdateScalar for Option<T>
where
    T: UpdateScalar,
{
    fn into_update_value(self) -> UpdateValue {
        match self {
            Some(value) => value.into_update_value(),
            None => UpdateValue::Null,
        }
    }
}

/// Executable scalar bulk update returning the number of affected rows.
#[derive(Clone, Debug)]
pub struct UpdateMany<S, T, V = ()> {
    values: UpdateValues,
    variables: QueryVariables,
    _marker: PhantomData<(S, T, V)>,
}

impl<S, T> UpdateMany<S, T, ()>
where
    T: UpdateManyModel<Variables = ()>,
{
    pub fn new(values: T::Values) -> Self {
        Self {
            values: values.into_update_values(),
            variables: QueryVariables::new(),
            _marker: PhantomData,
        }
    }

    pub fn with_values(values: UpdateValues) -> Self {
        Self {
            values,
            variables: QueryVariables::new(),
            _marker: PhantomData,
        }
    }
}

impl<S, T> UpdateMany<S, T, ()>
where
    T: UpdateManyModel,
{
    pub fn new_with_variables(
        variables: T::Variables,
        values: T::Values,
    ) -> UpdateMany<S, T, T::Variables> {
        UpdateMany {
            values: values.into_update_values(),
            variables: variables.into_query_variables(),
            _marker: PhantomData,
        }
    }

    pub fn with_values_and_variables(
        values: UpdateValues,
        variables: T::Variables,
    ) -> UpdateMany<S, T, T::Variables> {
        UpdateMany {
            values,
            variables: variables.into_query_variables(),
            _marker: PhantomData,
        }
    }

    pub fn with_variables(self, variables: T::Variables) -> UpdateMany<S, T, T::Variables> {
        UpdateMany {
            values: self.values,
            variables: variables.into_query_variables(),
            _marker: PhantomData,
        }
    }
}

impl<S, T, V> UpdateMany<S, T, V>
where
    S: SchemaAccess,
    T: UpdateManyModel<Schema = S, Variables = V>,
    V: QueryVariableSet,
{
    fn filter(&self) -> Option<QueryFilter> {
        T::filter_with_variables(&self.variables)
    }

    pub fn values(&self) -> &UpdateValues {
        &self.values
    }

    pub fn to_sql(&self) -> Result<String, sqlx::Error> {
        let filter = self.filter();
        let (sql, _) = build_update_many_sql(
            S::schema(),
            T::model_name(),
            &self.values,
            filter.as_ref(),
            &self.variables,
        )?;
        Ok(sql)
    }
}

impl<S, T, V> UpdateSpec for UpdateMany<S, T, V>
where
    S: SchemaAccess,
    T: UpdateManyModel<Schema = S, Variables = V> + Sync,
    V: QueryVariableSet + Sync,
{
    type Output = u64;

    fn execute<'a>(
        &'a self,
        executor: &'a dyn SqliteExecutor,
    ) -> BoxFuture<'a, Result<Self::Output, sqlx::Error>> {
        Box::pin(async move {
            let filter = self.filter();
            let (sql, bindings) = build_update_many_sql(
                S::schema(),
                T::model_name(),
                &self.values,
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

fn build_update_many_sql(
    schema: &Schema,
    model_name: &str,
    values: &UpdateValues,
    filter: Option<&QueryFilter>,
    variables: &QueryVariables,
) -> Result<(String, Vec<vitrail_sqlite_dialect::BindingValue>), sqlx::Error> {
    let mut dialect_values = vitrail_sqlite_dialect::UpdateValues::new();
    for field in values.iter() {
        dialect_values
            .push(field.name.clone(), dialect_update_value(&field.value))
            .map_err(map_compile_error)?;
    }

    let dialect_filter = filter.map(dialect_filter);
    let dialect_variables = dialect_variables(variables)?;
    let statement = vitrail_sqlite_dialect::compile_update_many(
        schema.as_dialect(),
        model_name,
        &dialect_values,
        dialect_filter.as_ref(),
        &dialect_variables,
    )
    .map_err(map_compile_error)?;
    let (sql, bindings, _, _) = statement.into_parts();
    Ok((sql, bindings))
}

fn dialect_update_value(value: &UpdateValue) -> vitrail_sqlite_dialect::UpdateValue {
    match value {
        UpdateValue::Null => vitrail_sqlite_dialect::UpdateValue::Null,
        UpdateValue::Int(value) => vitrail_sqlite_dialect::UpdateValue::Int(*value),
        UpdateValue::String(value) => vitrail_sqlite_dialect::UpdateValue::String(value.clone()),
        UpdateValue::Bool(value) => vitrail_sqlite_dialect::UpdateValue::Bool(*value),
        UpdateValue::Float(value) => vitrail_sqlite_dialect::UpdateValue::Float(*value),
        UpdateValue::Bytes(value) => vitrail_sqlite_dialect::UpdateValue::Bytes(value.clone()),
        UpdateValue::DateTime(value) => vitrail_sqlite_dialect::UpdateValue::DateTime(*value),
        UpdateValue::Json(value) => vitrail_sqlite_dialect::UpdateValue::Json(value.clone()),
    }
}
