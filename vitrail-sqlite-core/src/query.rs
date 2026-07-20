use std::collections::HashMap;
use std::marker::PhantomData;

use serde_json::Value as JsonValue;
use sqlx::sqlite::SqliteRow;
use sqlx::{Row as _, ValueRef as _};

pub use futures_util::future::BoxFuture;

use crate::SqliteExecutor;
use crate::schema::{Schema, SchemaAccess};
use crate::statement::{bind_statement, map_compile_error};

/// Runtime contract implemented by executable query values.
pub trait QuerySpec: Send + Sync {
    type Output: Send + 'static;

    #[doc(hidden)]
    fn fetch_many<'a>(
        &'a self,
        executor: &'a dyn SqliteExecutor,
    ) -> BoxFuture<'a, Result<Vec<Self::Output>, sqlx::Error>>;

    #[doc(hidden)]
    fn fetch_optional<'a>(
        &'a self,
        executor: &'a dyn SqliteExecutor,
    ) -> BoxFuture<'a, Result<Option<Self::Output>, sqlx::Error>> {
        Box::pin(async move { Ok(self.fetch_many(executor).await?.into_iter().next()) })
    }

    #[doc(hidden)]
    fn fetch_first<'a>(
        &'a self,
        executor: &'a dyn SqliteExecutor,
    ) -> BoxFuture<'a, Result<Self::Output, sqlx::Error>> {
        Box::pin(async move {
            self.fetch_optional(executor)
                .await?
                .ok_or(sqlx::Error::RowNotFound)
        })
    }
}

pub use vitrail_sqlite_dialect::{QueryOrder, QueryOrderDirection, QueryPagination, alias_name};

pub type QuerySelection = vitrail_sqlite_dialect::QuerySelection<QueryFilter>;
pub type QueryRelationSelection = vitrail_sqlite_dialect::QueryRelationSelection<QueryFilter>;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueryVariables {
    values: Vec<QueryVariableValue>,
    value_indices: HashMap<String, usize>,
}

impl QueryVariables {
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            value_indices: HashMap::new(),
        }
    }

    pub fn from_values(values: Vec<(impl Into<String>, QueryVariableValue)>) -> Self {
        let mut query_variables = Self::new();

        for (name, value) in values {
            query_variables
                .push(name, value)
                .expect("query variable names must be unique");
        }

        query_variables
    }

    pub fn push(
        &mut self,
        name: impl Into<String>,
        value: QueryVariableValue,
    ) -> Result<usize, sqlx::Error> {
        let name = name.into();

        if self.value_indices.contains_key(&name) {
            return Err(schema_error(format!("duplicate query variable `{name}`")));
        }

        let index = self.values.len();
        self.values.push(value);
        self.value_indices.insert(name, index);
        Ok(index)
    }

    pub fn get(&self, name: &str) -> Option<&QueryVariableValue> {
        self.value_indices
            .get(name)
            .and_then(|index| self.values.get(*index))
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

pub trait QueryVariableSet: Send + 'static {
    fn into_query_variables(self) -> QueryVariables;
}

impl QueryVariableSet for QueryVariables {
    fn into_query_variables(self) -> QueryVariables {
        self
    }
}

impl QueryVariableSet for () {
    fn into_query_variables(self) -> QueryVariables {
        QueryVariables::new()
    }
}

pub trait StringValueType: Sized + Send + 'static {
    fn from_db_string(value: String) -> Result<Self, sqlx::Error>;

    fn into_db_string(self) -> String;
}

impl StringValueType for String {
    fn from_db_string(value: String) -> Result<Self, sqlx::Error> {
        Ok(value)
    }

    fn into_db_string(self) -> String {
        self
    }
}

pub trait QueryScalar: Send {
    fn into_query_variable_value(self) -> QueryVariableValue;
}

pub trait QueryResultValue: Sized + Send + 'static {
    fn from_row(row: &SqliteRow, alias: &str) -> Result<Self, sqlx::Error>;

    fn from_json(value: &JsonValue) -> Result<Self, sqlx::Error>;
}

#[derive(Clone, Debug, PartialEq)]
pub enum QueryVariableValue {
    Null,
    Int(i64),
    String(String),
    Bool(bool),
    Float(f64),
    Bytes(Vec<u8>),
    DateTime(chrono::DateTime<chrono::Utc>),
    Json(JsonValue),
    List(Vec<QueryVariableValue>),
}

pub trait QueryListScalar: Send {
    fn into_list_query_variable_value(self) -> QueryVariableValue;
}

impl From<i64> for QueryVariableValue {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl QueryListScalar for i64 {
    fn into_list_query_variable_value(self) -> QueryVariableValue {
        self.into()
    }
}

impl From<String> for QueryVariableValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for QueryVariableValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl QueryListScalar for &str {
    fn into_list_query_variable_value(self) -> QueryVariableValue {
        self.into()
    }
}

impl<T> QueryListScalar for T
where
    T: StringValueType,
{
    fn into_list_query_variable_value(self) -> QueryVariableValue {
        QueryVariableValue::String(self.into_db_string())
    }
}

impl From<bool> for QueryVariableValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl QueryListScalar for bool {
    fn into_list_query_variable_value(self) -> QueryVariableValue {
        self.into()
    }
}

impl From<f64> for QueryVariableValue {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl QueryListScalar for f64 {
    fn into_list_query_variable_value(self) -> QueryVariableValue {
        self.into()
    }
}

impl From<Vec<u8>> for QueryVariableValue {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value)
    }
}

impl QueryListScalar for Vec<u8> {
    fn into_list_query_variable_value(self) -> QueryVariableValue {
        self.into()
    }
}

impl From<&[u8]> for QueryVariableValue {
    fn from(value: &[u8]) -> Self {
        Self::Bytes(value.to_vec())
    }
}

impl From<chrono::DateTime<chrono::Utc>> for QueryVariableValue {
    fn from(value: chrono::DateTime<chrono::Utc>) -> Self {
        Self::DateTime(value)
    }
}

impl QueryListScalar for chrono::DateTime<chrono::Utc> {
    fn into_list_query_variable_value(self) -> QueryVariableValue {
        self.into()
    }
}

impl From<JsonValue> for QueryVariableValue {
    fn from(value: JsonValue) -> Self {
        Self::Json(value)
    }
}

impl QueryListScalar for JsonValue {
    fn into_list_query_variable_value(self) -> QueryVariableValue {
        self.into()
    }
}

impl<T> From<Vec<T>> for QueryVariableValue
where
    T: QueryListScalar,
{
    fn from(values: Vec<T>) -> Self {
        Self::List(
            values
                .into_iter()
                .map(QueryListScalar::into_list_query_variable_value)
                .collect(),
        )
    }
}

impl<T> From<Option<T>> for QueryVariableValue
where
    T: Into<QueryVariableValue>,
{
    fn from(value: Option<T>) -> Self {
        match value {
            Some(value) => value.into(),
            None => Self::Null,
        }
    }
}

impl QueryScalar for i64 {
    fn into_query_variable_value(self) -> QueryVariableValue {
        self.into()
    }
}

impl QueryScalar for &str {
    fn into_query_variable_value(self) -> QueryVariableValue {
        self.into()
    }
}

impl QueryScalar for bool {
    fn into_query_variable_value(self) -> QueryVariableValue {
        self.into()
    }
}

impl QueryScalar for f64 {
    fn into_query_variable_value(self) -> QueryVariableValue {
        self.into()
    }
}

impl QueryScalar for Vec<u8> {
    fn into_query_variable_value(self) -> QueryVariableValue {
        self.into()
    }
}

impl QueryScalar for &[u8] {
    fn into_query_variable_value(self) -> QueryVariableValue {
        self.into()
    }
}

impl QueryScalar for chrono::DateTime<chrono::Utc> {
    fn into_query_variable_value(self) -> QueryVariableValue {
        self.into()
    }
}

impl QueryScalar for JsonValue {
    fn into_query_variable_value(self) -> QueryVariableValue {
        self.into()
    }
}

impl<T> QueryScalar for T
where
    T: StringValueType,
{
    fn into_query_variable_value(self) -> QueryVariableValue {
        QueryVariableValue::String(self.into_db_string())
    }
}

impl<T> QueryScalar for Vec<T>
where
    T: QueryListScalar,
{
    fn into_query_variable_value(self) -> QueryVariableValue {
        self.into()
    }
}

impl<T, const N: usize> QueryScalar for [T; N]
where
    T: QueryListScalar,
{
    fn into_query_variable_value(self) -> QueryVariableValue {
        QueryVariableValue::List(
            self.into_iter()
                .map(QueryListScalar::into_list_query_variable_value)
                .collect(),
        )
    }
}

impl<T> QueryScalar for Option<T>
where
    T: QueryScalar,
{
    fn into_query_variable_value(self) -> QueryVariableValue {
        match self {
            Some(value) => value.into_query_variable_value(),
            None => QueryVariableValue::Null,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum QueryFilterValue {
    Variable(String),
    Value(QueryVariableValue),
}

impl QueryFilterValue {
    pub fn variable(name: impl Into<String>) -> Self {
        Self::Variable(name.into())
    }

    pub fn value<T>(value: T) -> Self
    where
        T: QueryScalar,
    {
        Self::Value(value.into_query_variable_value())
    }
}

impl<T> From<T> for QueryFilterValue
where
    T: QueryScalar,
{
    fn from(value: T) -> Self {
        Self::Value(value.into_query_variable_value())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum QueryFilterValues {
    Variable(String),
    Values(Vec<QueryFilterValue>),
}

impl QueryFilterValues {
    pub fn variable(name: impl Into<String>) -> Self {
        Self::Variable(name.into())
    }

    pub fn values<T>(values: impl IntoIterator<Item = T>) -> Self
    where
        T: QueryListScalar,
    {
        Self::Values(
            values
                .into_iter()
                .map(|value| QueryFilterValue::Value(value.into_list_query_variable_value()))
                .collect(),
        )
    }
}

impl<T> From<Vec<T>> for QueryFilterValues
where
    T: QueryListScalar,
{
    fn from(values: Vec<T>) -> Self {
        Self::values(values)
    }
}

impl<T, const N: usize> From<[T; N]> for QueryFilterValues
where
    T: QueryListScalar,
{
    fn from(values: [T; N]) -> Self {
        Self::values(values)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum QueryFilter {
    And(Vec<QueryFilter>),
    Or(Vec<QueryFilter>),
    Not(Box<QueryFilter>),
    Eq {
        field: &'static str,
        value: QueryFilterValue,
    },
    Ne {
        field: &'static str,
        value: QueryFilterValue,
    },
    In {
        field: &'static str,
        values: QueryFilterValues,
    },
    Relation {
        field: &'static str,
        filter: Box<QueryFilter>,
    },
}

impl QueryFilter {
    pub fn eq(field: &'static str, value: impl Into<QueryFilterValue>) -> Self {
        Self::Eq {
            field,
            value: value.into(),
        }
    }

    pub fn ne(field: &'static str, value: impl Into<QueryFilterValue>) -> Self {
        Self::Ne {
            field,
            value: value.into(),
        }
    }

    pub fn r#in(field: &'static str, values: impl Into<QueryFilterValues>) -> Self {
        Self::In {
            field,
            values: values.into(),
        }
    }

    pub fn is_null(field: &'static str) -> Self {
        Self::Eq {
            field,
            value: QueryFilterValue::Value(QueryVariableValue::Null),
        }
    }

    pub fn is_not_null(field: &'static str) -> Self {
        Self::Ne {
            field,
            value: QueryFilterValue::Value(QueryVariableValue::Null),
        }
    }

    pub fn relation(field: &'static str, filter: QueryFilter) -> Self {
        Self::Relation {
            field,
            filter: Box::new(filter),
        }
    }
}

pub trait QueryValue: Sized + Send + 'static {
    fn from_json(value: &JsonValue) -> Result<Self, sqlx::Error>;
}

pub trait QueryModel: Sized + Send + 'static {
    type Schema: SchemaAccess;
    type Variables: QueryVariableSet;

    fn model_name() -> &'static str;

    fn selection() -> QuerySelection;

    fn selection_with_variables(_variables: &QueryVariables) -> QuerySelection {
        Self::selection()
    }

    fn from_row(row: &SqliteRow, prefix: &str) -> Result<Self, sqlx::Error>;
}

#[derive(Clone, Debug)]
pub struct Query<S, T, V = ()> {
    selection: Option<QuerySelection>,
    variables: QueryVariables,
    _marker: PhantomData<(S, T, V)>,
}

impl<S, T> Query<S, T, ()>
where
    T: QueryModel<Variables = ()>,
{
    pub fn new() -> Self {
        Self {
            selection: None,
            variables: QueryVariables::new(),
            _marker: PhantomData,
        }
    }

    pub fn with_selection(selection: QuerySelection) -> Self {
        Self {
            selection: Some(selection),
            variables: QueryVariables::new(),
            _marker: PhantomData,
        }
    }
}

impl<S, T> Default for Query<S, T, ()>
where
    T: QueryModel<Variables = ()>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<S, T> Query<S, T, ()>
where
    T: QueryModel,
{
    pub fn new_with_variables(variables: T::Variables) -> Query<S, T, T::Variables> {
        Query {
            selection: None,
            variables: variables.into_query_variables(),
            _marker: PhantomData,
        }
    }

    pub fn with_selection_and_variables(
        selection: QuerySelection,
        variables: T::Variables,
    ) -> Query<S, T, T::Variables> {
        Query {
            selection: Some(selection),
            ..Self::new_with_variables(variables)
        }
    }

    pub fn with_variables(self, variables: T::Variables) -> Query<S, T, T::Variables> {
        Query {
            selection: self.selection,
            variables: variables.into_query_variables(),
            _marker: PhantomData,
        }
    }
}

impl<S, T, V> Query<S, T, V>
where
    S: SchemaAccess,
    T: QueryModel<Schema = S, Variables = V>,
    V: QueryVariableSet,
{
    fn selection(&self) -> QuerySelection {
        self.selection
            .clone()
            .unwrap_or_else(|| T::selection_with_variables(&self.variables))
    }

    pub fn to_sql(&self) -> Result<String, sqlx::Error> {
        let selection = self.selection();
        let (sql, _) = build_query_sql(S::schema(), &selection, &self.variables)?;
        Ok(sql)
    }
}

impl<S, T, V> QuerySpec for Query<S, T, V>
where
    S: SchemaAccess,
    T: QueryModel<Schema = S, Variables = V> + Sync,
    V: QueryVariableSet + Sync,
{
    type Output = T;

    fn fetch_many<'a>(
        &'a self,
        executor: &'a dyn SqliteExecutor,
    ) -> BoxFuture<'a, Result<Vec<Self::Output>, sqlx::Error>> {
        Box::pin(async move {
            let selection = self.selection();
            let (sql, bindings) = build_query_sql(S::schema(), &selection, &self.variables)?;
            let rows = executor
                .fetch_all(bind_statement(sqlx::query(&sql), &bindings))
                .await?;
            let mut values = Vec::with_capacity(rows.len());
            let root_prefix = selection.model;

            for row in rows {
                values.push(T::from_row(&row, root_prefix)?);
            }

            Ok(values)
        })
    }

    fn fetch_optional<'a>(
        &'a self,
        executor: &'a dyn SqliteExecutor,
    ) -> BoxFuture<'a, Result<Option<Self::Output>, sqlx::Error>> {
        Box::pin(async move {
            let mut selection = self.selection();
            selection.limit = Some(QueryPagination::value(1));

            let (sql, bindings) = build_query_sql(S::schema(), &selection, &self.variables)?;
            let row = executor
                .fetch_optional(bind_statement(sqlx::query(&sql), &bindings))
                .await?;
            let root_prefix = selection.model;

            row.map(|row| T::from_row(&row, root_prefix)).transpose()
        })
    }
}

pub fn query_model_is_null<T: QueryModel>(
    row: &SqliteRow,
    prefix: &str,
) -> Result<bool, sqlx::Error> {
    selection_is_null(row, prefix, &T::selection())
}

fn selection_is_null(
    row: &SqliteRow,
    prefix: &str,
    selection: &QuerySelection,
) -> Result<bool, sqlx::Error> {
    for field in &selection.scalar_fields {
        let alias = alias_name(prefix, field);
        if !row.try_get_raw(alias.as_str())?.is_null() {
            return Ok(false);
        }
    }

    for relation in &selection.relations {
        let alias = alias_name(prefix, relation.field);
        if !row.try_get_raw(alias.as_str())?.is_null() {
            return Ok(false);
        }
    }

    Ok(true)
}

pub fn json_array_field(value: &JsonValue, index: usize) -> Result<&JsonValue, sqlx::Error> {
    value.get(index).ok_or_else(|| {
        schema_error(format!(
            "missing JSON array index `{index}` in query result"
        ))
    })
}

pub fn json_as_i64(value: &JsonValue) -> Result<i64, sqlx::Error> {
    value
        .as_i64()
        .ok_or_else(|| schema_error("expected JSON integer in query result".to_owned()))
}

pub fn json_as_string(value: &JsonValue) -> Result<String, sqlx::Error> {
    value
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| schema_error("expected JSON string in query result".to_owned()))
}

pub fn json_as_bool(value: &JsonValue) -> Result<bool, sqlx::Error> {
    value
        .as_bool()
        .ok_or_else(|| schema_error("expected JSON boolean in query result".to_owned()))
}

pub fn json_value<T>(value: &JsonValue) -> Result<T, sqlx::Error>
where
    T: QueryResultValue,
{
    T::from_json(value)
}

fn json_string_value<T>(value: &JsonValue) -> Result<T, sqlx::Error>
where
    T: StringValueType,
{
    T::from_db_string(json_as_string(value)?)
}

pub fn json_as_f64(value: &JsonValue) -> Result<f64, sqlx::Error> {
    value
        .as_f64()
        .ok_or_else(|| schema_error("expected JSON float in query result".to_owned()))
}

pub fn json_as_bytes(value: &JsonValue) -> Result<Vec<u8>, sqlx::Error> {
    match value {
        JsonValue::String(value) => decode_hex_bytes(value),
        JsonValue::Array(values) => values
            .iter()
            .map(|value| {
                let byte = value.as_u64().ok_or_else(|| {
                    schema_error("expected JSON byte array in query result".to_owned())
                })?;

                u8::try_from(byte).map_err(|_| {
                    schema_error(format!(
                        "expected JSON byte array values in range 0..=255, got `{byte}`"
                    ))
                })
            })
            .collect(),
        _ => Err(schema_error(
            "expected JSON byte string or byte array in query result".to_owned(),
        )),
    }
}

fn decode_hex_bytes(value: &str) -> Result<Vec<u8>, sqlx::Error> {
    let value = value.strip_prefix("\\x").unwrap_or(value);

    if !value.len().is_multiple_of(2) {
        return Err(schema_error(format!(
            "invalid bytes in query result `{value}`: hex string must have an even length"
        )));
    }

    let mut bytes = Vec::with_capacity(value.len() / 2);
    let mut index = 0;

    while index < value.len() {
        let chunk = &value[index..index + 2];
        let byte = u8::from_str_radix(chunk, 16).map_err(|error| {
            schema_error(format!("invalid bytes in query result `{value}`: {error}"))
        })?;
        bytes.push(byte);
        index += 2;
    }

    Ok(bytes)
}

pub fn json_as_datetime_utc(
    value: &JsonValue,
) -> Result<chrono::DateTime<chrono::Utc>, sqlx::Error> {
    let value = value
        .as_str()
        .ok_or_else(|| schema_error("expected JSON datetime string in query result".to_owned()))?;

    parse_datetime_utc(value).ok_or_else(|| {
        schema_error(format!(
            "invalid JSON datetime in query result: unsupported format `{value}`"
        ))
    })
}

fn parse_datetime_utc(value: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    if let Ok(datetime) = chrono::DateTime::parse_from_rfc3339(value) {
        return Some(datetime.with_timezone(&chrono::Utc));
    }

    for format in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"] {
        if let Ok(datetime) = chrono::NaiveDateTime::parse_from_str(value, format) {
            return Some(datetime.and_utc());
        }
    }

    None
}

pub fn row_as_datetime_utc(
    row: &SqliteRow,
    alias: &str,
) -> Result<chrono::DateTime<chrono::Utc>, sqlx::Error> {
    if let Ok(value) = row.try_get::<chrono::DateTime<chrono::Utc>, _>(alias) {
        return Ok(value);
    }

    if let Ok(value) = row.try_get::<chrono::NaiveDateTime, _>(alias) {
        return Ok(value.and_utc());
    }

    let value: String = row.try_get(alias)?;
    parse_datetime_utc(&value).ok_or_else(|| {
        schema_error(format!(
            "invalid datetime in query result: unsupported format `{value}`"
        ))
    })
}

pub fn row_as_bytes(row: &SqliteRow, alias: &str) -> Result<Vec<u8>, sqlx::Error> {
    row.try_get(alias)
}

fn parse_json_text(value: &str) -> Result<JsonValue, sqlx::Error> {
    serde_json::from_str(value).map_err(|error| sqlx::Error::Decode(Box::new(error)))
}

#[doc(hidden)]
pub fn row_relation_json(row: &SqliteRow, alias: &str) -> Result<JsonValue, sqlx::Error> {
    let value: String = row.try_get(alias)?;
    parse_json_text(&value)
}

#[doc(hidden)]
pub fn row_optional_relation_json(
    row: &SqliteRow,
    alias: &str,
) -> Result<Option<JsonValue>, sqlx::Error> {
    let value: Option<String> = row.try_get(alias)?;
    value.map(|value| parse_json_text(&value)).transpose()
}

pub fn row_value<T>(row: &SqliteRow, alias: &str) -> Result<T, sqlx::Error>
where
    T: QueryResultValue,
{
    T::from_row(row, alias)
}

fn row_string_value<T>(row: &SqliteRow, alias: &str) -> Result<T, sqlx::Error>
where
    T: StringValueType,
{
    T::from_db_string(row.try_get::<String, _>(alias)?)
}

impl QueryResultValue for i64 {
    fn from_row(row: &SqliteRow, alias: &str) -> Result<Self, sqlx::Error> {
        row.try_get(alias)
    }

    fn from_json(value: &JsonValue) -> Result<Self, sqlx::Error> {
        json_as_i64(value)
    }
}

impl QueryResultValue for bool {
    fn from_row(row: &SqliteRow, alias: &str) -> Result<Self, sqlx::Error> {
        row.try_get(alias)
    }

    fn from_json(value: &JsonValue) -> Result<Self, sqlx::Error> {
        json_as_bool(value)
    }
}

impl QueryResultValue for f64 {
    fn from_row(row: &SqliteRow, alias: &str) -> Result<Self, sqlx::Error> {
        row.try_get(alias)
    }

    fn from_json(value: &JsonValue) -> Result<Self, sqlx::Error> {
        json_as_f64(value)
    }
}

impl QueryResultValue for chrono::DateTime<chrono::Utc> {
    fn from_row(row: &SqliteRow, alias: &str) -> Result<Self, sqlx::Error> {
        row_as_datetime_utc(row, alias)
    }

    fn from_json(value: &JsonValue) -> Result<Self, sqlx::Error> {
        json_as_datetime_utc(value)
    }
}

impl QueryResultValue for Vec<u8> {
    fn from_row(row: &SqliteRow, alias: &str) -> Result<Self, sqlx::Error> {
        row_as_bytes(row, alias)
    }

    fn from_json(value: &JsonValue) -> Result<Self, sqlx::Error> {
        json_as_bytes(value)
    }
}

impl QueryResultValue for JsonValue {
    fn from_row(row: &SqliteRow, alias: &str) -> Result<Self, sqlx::Error> {
        let value: String = row.try_get(alias)?;
        parse_json_text(&value)
    }

    fn from_json(value: &JsonValue) -> Result<Self, sqlx::Error> {
        Ok(value.clone())
    }
}

impl<T> QueryResultValue for T
where
    T: StringValueType,
{
    fn from_row(row: &SqliteRow, alias: &str) -> Result<Self, sqlx::Error> {
        row_string_value(row, alias)
    }

    fn from_json(value: &JsonValue) -> Result<Self, sqlx::Error> {
        json_string_value(value)
    }
}

impl<T> QueryResultValue for Option<T>
where
    T: QueryResultValue,
{
    fn from_row(row: &SqliteRow, alias: &str) -> Result<Self, sqlx::Error> {
        if row.try_get_raw(alias)?.is_null() {
            Ok(None)
        } else {
            T::from_row(row, alias).map(Some)
        }
    }

    fn from_json(value: &JsonValue) -> Result<Self, sqlx::Error> {
        if value.is_null() {
            Ok(None)
        } else {
            T::from_json(value).map(Some)
        }
    }
}

fn build_query_sql(
    schema: &Schema,
    selection: &QuerySelection,
    variables: &QueryVariables,
) -> Result<(String, Vec<vitrail_sqlite_dialect::BindingValue>), sqlx::Error> {
    let selection = dialect_selection(selection);
    let variables = dialect_variables(variables)?;
    let statement =
        vitrail_sqlite_dialect::compile_query(schema.as_dialect(), &selection, &variables)
            .map_err(map_compile_error)?;
    let (sql, bindings, _, _) = statement.into_parts();
    Ok((sql, bindings))
}

fn dialect_selection(selection: &QuerySelection) -> vitrail_sqlite_dialect::QuerySelection {
    vitrail_sqlite_dialect::QuerySelection {
        model: selection.model,
        scalar_fields: selection.scalar_fields.clone(),
        relations: selection
            .relations
            .iter()
            .map(|relation| vitrail_sqlite_dialect::QueryRelationSelection {
                field: relation.field,
                selection: dialect_selection(&relation.selection),
            })
            .collect(),
        filter: selection.filter.as_ref().map(dialect_filter),
        order_by: selection.order_by.clone(),
        skip: selection.skip.clone(),
        limit: selection.limit.clone(),
    }
}

pub(crate) fn dialect_filter(filter: &QueryFilter) -> vitrail_sqlite_dialect::QueryFilter {
    match filter {
        QueryFilter::And(filters) => {
            vitrail_sqlite_dialect::QueryFilter::And(filters.iter().map(dialect_filter).collect())
        }
        QueryFilter::Or(filters) => {
            vitrail_sqlite_dialect::QueryFilter::Or(filters.iter().map(dialect_filter).collect())
        }
        QueryFilter::Not(filter) => {
            vitrail_sqlite_dialect::QueryFilter::Not(Box::new(dialect_filter(filter)))
        }
        QueryFilter::Eq { field, value } => vitrail_sqlite_dialect::QueryFilter::Eq {
            field,
            value: dialect_filter_value(value),
        },
        QueryFilter::Ne { field, value } => vitrail_sqlite_dialect::QueryFilter::Ne {
            field,
            value: dialect_filter_value(value),
        },
        QueryFilter::In { field, values } => vitrail_sqlite_dialect::QueryFilter::In {
            field,
            values: dialect_filter_values(values),
        },
        QueryFilter::Relation { field, filter } => vitrail_sqlite_dialect::QueryFilter::Relation {
            field,
            filter: Box::new(dialect_filter(filter)),
        },
    }
}

fn dialect_filter_value(value: &QueryFilterValue) -> vitrail_sqlite_dialect::QueryFilterValue {
    match value {
        QueryFilterValue::Variable(name) => {
            vitrail_sqlite_dialect::QueryFilterValue::Variable(name.clone())
        }
        QueryFilterValue::Value(value) => {
            vitrail_sqlite_dialect::QueryFilterValue::Value(dialect_variable_value(value))
        }
    }
}

fn dialect_filter_values(values: &QueryFilterValues) -> vitrail_sqlite_dialect::QueryFilterValues {
    match values {
        QueryFilterValues::Variable(name) => {
            vitrail_sqlite_dialect::QueryFilterValues::Variable(name.clone())
        }
        QueryFilterValues::Values(values) => vitrail_sqlite_dialect::QueryFilterValues::Values(
            values.iter().map(dialect_filter_value).collect(),
        ),
    }
}

pub(crate) fn dialect_variables(
    variables: &QueryVariables,
) -> Result<vitrail_sqlite_dialect::QueryVariables, sqlx::Error> {
    let mut entries = variables.value_indices.iter().collect::<Vec<_>>();
    entries.sort_by_key(|(_, index)| **index);

    let mut dialect_variables = vitrail_sqlite_dialect::QueryVariables::new();
    for (name, index) in entries {
        dialect_variables
            .push(
                name.clone(),
                dialect_variable_value(&variables.values[*index]),
            )
            .map_err(map_compile_error)?;
    }

    Ok(dialect_variables)
}

fn dialect_variable_value(
    value: &QueryVariableValue,
) -> vitrail_sqlite_dialect::QueryVariableValue {
    match value {
        QueryVariableValue::Null => vitrail_sqlite_dialect::QueryVariableValue::Null,
        QueryVariableValue::Int(value) => vitrail_sqlite_dialect::QueryVariableValue::Int(*value),
        QueryVariableValue::String(value) => {
            vitrail_sqlite_dialect::QueryVariableValue::String(value.clone())
        }
        QueryVariableValue::Bool(value) => vitrail_sqlite_dialect::QueryVariableValue::Bool(*value),
        QueryVariableValue::Float(value) => {
            vitrail_sqlite_dialect::QueryVariableValue::Float(*value)
        }
        QueryVariableValue::Bytes(value) => {
            vitrail_sqlite_dialect::QueryVariableValue::Bytes(value.clone())
        }
        QueryVariableValue::DateTime(value) => {
            vitrail_sqlite_dialect::QueryVariableValue::DateTime(*value)
        }
        QueryVariableValue::Json(value) => {
            vitrail_sqlite_dialect::QueryVariableValue::Json(value.clone())
        }
        QueryVariableValue::List(values) => vitrail_sqlite_dialect::QueryVariableValue::List(
            values.iter().map(dialect_variable_value).collect(),
        ),
    }
}

pub fn schema_error(message: String) -> sqlx::Error {
    sqlx::Error::Protocol(message)
}
