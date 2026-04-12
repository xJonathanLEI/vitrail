use std::collections::HashMap;
use std::marker::PhantomData;

use serde_json::Value as JsonValue;
use sqlx::postgres::{PgArguments, PgRow};
use sqlx::{Postgres, Row as _, ValueRef as _};

pub use futures_util::future::BoxFuture;
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::PgExecutor;
use crate::filter::{FilterBuilder, compile_filter_sql, schema_model as resolve_schema_model};
use crate::schema::{FieldType, Model, ScalarType, Schema};

/// Runtime contract implemented by executable query values.
pub trait QuerySpec: Send + Sync {
    type Output: Send + 'static;

    #[doc(hidden)]
    fn fetch_many<'a>(
        &'a self,
        executor: &'a dyn PgExecutor,
    ) -> BoxFuture<'a, Result<Vec<Self::Output>, sqlx::Error>>;

    #[doc(hidden)]
    fn fetch_optional<'a>(
        &'a self,
        executor: &'a dyn PgExecutor,
    ) -> BoxFuture<'a, Result<Option<Self::Output>, sqlx::Error>> {
        Box::pin(async move { Ok(self.fetch_many(executor).await?.into_iter().next()) })
    }

    #[doc(hidden)]
    fn fetch_first<'a>(
        &'a self,
        executor: &'a dyn PgExecutor,
    ) -> BoxFuture<'a, Result<Self::Output, sqlx::Error>> {
        Box::pin(async move {
            self.fetch_optional(executor)
                .await?
                .ok_or(sqlx::Error::RowNotFound)
        })
    }
}

pub trait SchemaAccess: Send + Sync + 'static {
    fn schema() -> &'static Schema;
}

#[derive(Clone, Debug, PartialEq)]
pub struct QuerySelection {
    pub model: &'static str,
    pub scalar_fields: Vec<&'static str>,
    pub relations: Vec<QueryRelationSelection>,
    pub filter: Option<QueryFilter>,
    pub order_by: Vec<QueryOrder>,
    pub skip: Option<QueryPagination>,
    pub limit: Option<QueryPagination>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryRelationSelection {
    pub field: &'static str,
    pub selection: QuerySelection,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryOrderDirection {
    Asc,
    Desc,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryOrder {
    Scalar {
        field: &'static str,
        direction: QueryOrderDirection,
    },
    Relation {
        field: &'static str,
        orders: Vec<QueryOrder>,
    },
}

impl QueryOrder {
    pub fn scalar(field: &'static str, direction: QueryOrderDirection) -> Self {
        Self::Scalar { field, direction }
    }

    pub fn relation(field: &'static str, orders: Vec<QueryOrder>) -> Self {
        Self::Relation { field, orders }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryPagination {
    Value(i64),
    Variable(&'static str),
}

impl QueryPagination {
    pub fn value(value: i64) -> Self {
        Self::Value(value)
    }

    pub fn variable(name: &'static str) -> Self {
        Self::Variable(name)
    }
}

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
    fn from_row(row: &PgRow, alias: &str) -> Result<Self, sqlx::Error>;

    fn from_json(value: &JsonValue) -> Result<Self, sqlx::Error>;
}

#[derive(Clone, Debug, PartialEq)]
pub enum QueryVariableValue {
    Null,
    Int(i64),
    String(String),
    Bool(bool),
    Float(f64),
    Decimal(Decimal),
    Bytes(Vec<u8>),
    DateTime(chrono::DateTime<chrono::Utc>),
    Uuid(Uuid),
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

impl From<Decimal> for QueryVariableValue {
    fn from(value: Decimal) -> Self {
        Self::Decimal(value)
    }
}

impl QueryListScalar for Decimal {
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

impl From<Uuid> for QueryVariableValue {
    fn from(value: Uuid) -> Self {
        Self::Uuid(value)
    }
}

impl QueryListScalar for Uuid {
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

impl QueryScalar for Decimal {
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

impl QueryScalar for Uuid {
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
                .collect::<Vec<_>>(),
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

    fn from_row(row: &PgRow, prefix: &str) -> Result<Self, sqlx::Error>;
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
        executor: &'a dyn PgExecutor,
    ) -> BoxFuture<'a, Result<Vec<Self::Output>, sqlx::Error>> {
        Box::pin(async move {
            let selection = self.selection();
            let (sql, bindings) = build_query_sql(S::schema(), &selection, &self.variables)?;
            let rows = executor
                .fetch_all(bind_query(sqlx::query(&sql), &bindings))
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
        executor: &'a dyn PgExecutor,
    ) -> BoxFuture<'a, Result<Option<Self::Output>, sqlx::Error>> {
        Box::pin(async move {
            let mut selection = self.selection();
            selection.limit = Some(QueryPagination::value(1));

            let (sql, bindings) = build_query_sql(S::schema(), &selection, &self.variables)?;
            let row = executor
                .fetch_optional(bind_query(sqlx::query(&sql), &bindings))
                .await?;
            let root_prefix = selection.model;

            row.map(|row| T::from_row(&row, root_prefix)).transpose()
        })
    }
}

pub fn query_model_is_null<T: QueryModel>(row: &PgRow, prefix: &str) -> Result<bool, sqlx::Error> {
    selection_is_null(row, prefix, &T::selection())
}

fn selection_is_null(
    row: &PgRow,
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

pub fn alias_name(prefix: &str, field: &str) -> String {
    format!("{prefix}__{field}")
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

pub fn json_string_value<T>(value: &JsonValue) -> Result<T, sqlx::Error>
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

pub fn json_as_decimal(value: &JsonValue) -> Result<Decimal, sqlx::Error> {
    match value {
        JsonValue::String(value) => parse_decimal(value),
        JsonValue::Number(value) => parse_decimal(&value.to_string()),
        _ => Err(schema_error(
            "expected JSON decimal string or number in query result".to_owned(),
        )),
    }
}

pub fn parse_decimal(value: &str) -> Result<Decimal, sqlx::Error> {
    Decimal::from_str_exact(value)
        .or_else(|_| Decimal::from_scientific(value))
        .or_else(|_| {
            let normalized = normalize_decimal_string(value);
            Decimal::from_str_exact(&normalized).or_else(|_| Decimal::from_scientific(&normalized))
        })
        .map_err(|error| {
            schema_error(format!(
                "invalid decimal in query result `{value}`: {error}"
            ))
        })
}

fn normalize_decimal_string(value: &str) -> String {
    if let Some((integer, fractional)) = value.split_once('.') {
        let fractional = fractional.trim_end_matches('0');
        if fractional.is_empty() {
            integer.to_owned()
        } else {
            format!("{integer}.{fractional}")
        }
    } else {
        value.to_owned()
    }
}

pub fn row_as_decimal(row: &PgRow, alias: &str) -> Result<Decimal, sqlx::Error> {
    row.try_get(alias)
}

pub fn json_as_datetime_utc(
    value: &JsonValue,
) -> Result<chrono::DateTime<chrono::Utc>, sqlx::Error> {
    let value = value
        .as_str()
        .ok_or_else(|| schema_error("expected JSON datetime string in query result".to_owned()))?;

    if let Ok(datetime) = chrono::DateTime::parse_from_rfc3339(value) {
        return Ok(datetime.with_timezone(&chrono::Utc));
    }

    for format in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"] {
        if let Ok(datetime) = chrono::NaiveDateTime::parse_from_str(value, format) {
            return Ok(datetime.and_utc());
        }
    }

    Err(schema_error(format!(
        "invalid JSON datetime in query result: unsupported format `{value}`"
    )))
}

pub fn row_as_datetime_utc(
    row: &PgRow,
    alias: &str,
) -> Result<chrono::DateTime<chrono::Utc>, sqlx::Error> {
    if let Ok(value) = row.try_get::<chrono::DateTime<chrono::Utc>, _>(alias) {
        return Ok(value);
    }

    let value: chrono::NaiveDateTime = row.try_get(alias)?;
    Ok(value.and_utc())
}

pub fn row_as_bytes(row: &PgRow, alias: &str) -> Result<Vec<u8>, sqlx::Error> {
    row.try_get(alias)
}

pub fn row_value<T>(row: &PgRow, alias: &str) -> Result<T, sqlx::Error>
where
    T: QueryResultValue,
{
    T::from_row(row, alias)
}

pub fn row_string_value<T>(row: &PgRow, alias: &str) -> Result<T, sqlx::Error>
where
    T: StringValueType,
{
    T::from_db_string(row.try_get::<String, _>(alias)?)
}

impl QueryResultValue for i64 {
    fn from_row(row: &PgRow, alias: &str) -> Result<Self, sqlx::Error> {
        row.try_get(alias)
    }

    fn from_json(value: &JsonValue) -> Result<Self, sqlx::Error> {
        json_as_i64(value)
    }
}

impl QueryResultValue for bool {
    fn from_row(row: &PgRow, alias: &str) -> Result<Self, sqlx::Error> {
        row.try_get(alias)
    }

    fn from_json(value: &JsonValue) -> Result<Self, sqlx::Error> {
        json_as_bool(value)
    }
}

impl QueryResultValue for f64 {
    fn from_row(row: &PgRow, alias: &str) -> Result<Self, sqlx::Error> {
        row.try_get(alias)
    }

    fn from_json(value: &JsonValue) -> Result<Self, sqlx::Error> {
        json_as_f64(value)
    }
}

impl QueryResultValue for Decimal {
    fn from_row(row: &PgRow, alias: &str) -> Result<Self, sqlx::Error> {
        row_as_decimal(row, alias)
    }

    fn from_json(value: &JsonValue) -> Result<Self, sqlx::Error> {
        json_as_decimal(value)
    }
}

impl QueryResultValue for chrono::DateTime<chrono::Utc> {
    fn from_row(row: &PgRow, alias: &str) -> Result<Self, sqlx::Error> {
        row_as_datetime_utc(row, alias)
    }

    fn from_json(value: &JsonValue) -> Result<Self, sqlx::Error> {
        json_as_datetime_utc(value)
    }
}

impl QueryResultValue for Vec<u8> {
    fn from_row(row: &PgRow, alias: &str) -> Result<Self, sqlx::Error> {
        row_as_bytes(row, alias)
    }

    fn from_json(value: &JsonValue) -> Result<Self, sqlx::Error> {
        json_as_bytes(value)
    }
}

impl QueryResultValue for Uuid {
    fn from_row(row: &PgRow, alias: &str) -> Result<Self, sqlx::Error> {
        row.try_get(alias)
    }

    fn from_json(value: &JsonValue) -> Result<Self, sqlx::Error> {
        let value = json_as_string(value)?;
        Uuid::parse_str(&value)
            .map_err(|error| schema_error(format!("invalid JSON UUID in query result: {error}")))
    }
}

impl<T> QueryResultValue for T
where
    T: StringValueType,
{
    fn from_row(row: &PgRow, alias: &str) -> Result<Self, sqlx::Error> {
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
    fn from_row(row: &PgRow, alias: &str) -> Result<Self, sqlx::Error> {
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
) -> Result<(String, Vec<QueryVariableValue>), sqlx::Error> {
    let root_model = resolve_schema_model(schema, selection.model, "query")?;

    let mut builder = SqlBuilder {
        schema,
        variables,
        bindings: Vec::new(),
        joins: Vec::new(),
        next_alias: 1,
    };

    let selects = builder.root_selects(root_model, selection, selection.model, "t0")?;
    let where_clause = selection
        .filter
        .as_ref()
        .map(|filter| builder.filter_sql(root_model, filter, "t0"))
        .transpose()?;

    let mut order_joins = Vec::new();
    let order_by_clause =
        builder.order_by_sql(root_model, &selection.order_by, "t0", &mut order_joins)?;
    builder.joins.extend(order_joins);

    let pagination_clause =
        builder.pagination_clause(selection.skip.as_ref(), selection.limit.as_ref())?;

    let sql = format!(
        "SELECT {} FROM {} AS \"t0\"{}{}{}{}",
        selects.join(", "),
        quoted_ident(root_model.name()),
        if builder.joins.is_empty() {
            String::new()
        } else {
            format!(" {}", builder.joins.join(" "))
        },
        where_clause
            .map(|where_clause| format!(" WHERE {where_clause}"))
            .unwrap_or_default(),
        order_by_clause
            .map(|order_by_clause| format!(" ORDER BY {order_by_clause}"))
            .unwrap_or_default(),
        pagination_clause,
    );

    Ok((sql, builder.bindings))
}

fn model_names_match(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn infer_relation_fields<'a>(
    model: &'a Model,
    field: &'a crate::Field,
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

struct SqlBuilder<'a> {
    schema: &'a Schema,
    variables: &'a QueryVariables,
    bindings: Vec<QueryVariableValue>,
    joins: Vec<String>,
    next_alias: usize,
}

struct RelationSql<'a> {
    many: bool,
    source_model_name: &'a str,
    relation_field_name: &'a str,
    target_model: &'a Model,
    selection: QuerySelection,
    parent_table_alias: String,
    nested_alias: String,
    nested_fields: Vec<&'a str>,
    parent_fields: Vec<&'a str>,
}

impl<'a> SqlBuilder<'a> {
    fn root_selects(
        &mut self,
        model: &'a Model,
        selection: &QuerySelection,
        prefix: &str,
        table_alias: &str,
    ) -> Result<Vec<String>, sqlx::Error> {
        let mut selects = Vec::new();

        for field_name in &selection.scalar_fields {
            let field = model.field_named(field_name).ok_or_else(|| {
                schema_error(format!(
                    "unknown field `{}.{}` in query selection",
                    model.name(),
                    field_name
                ))
            })?;

            let scalar = match field.ty() {
                FieldType::Scalar(scalar) => scalar.scalar(),
                FieldType::Relation { .. } => {
                    return Err(schema_error(format!(
                        "field `{}.{}` is not scalar and cannot appear in `select`",
                        model.name(),
                        field_name
                    )));
                }
            };

            selects.push(select_expr(
                table_alias,
                field.name(),
                scalar,
                &alias_name(prefix, field.name()),
            ));
        }

        for relation in &selection.relations {
            selects.push(self.relation_select(model, relation, prefix, table_alias)?);
        }

        Ok(selects)
    }

    fn relation_select(
        &mut self,
        model: &'a Model,
        relation: &QueryRelationSelection,
        prefix: &str,
        table_alias: &str,
    ) -> Result<String, sqlx::Error> {
        let field = model.field_named(relation.field).ok_or_else(|| {
            schema_error(format!(
                "unknown relation `{}.{}` in query include",
                model.name(),
                relation.field
            ))
        })?;

        if field.kind().is_scalar() {
            return Err(schema_error(format!(
                "field `{}.{}` is not a relation and cannot appear in `include`",
                model.name(),
                relation.field
            )));
        }

        let target_model =
            resolve_schema_model(self.schema, field.ty().name(), "query").map_err(|_| {
                schema_error(format!(
                    "relation `{}.{}` points at unknown model `{}`",
                    model.name(),
                    relation.field,
                    field.ty().name()
                ))
            })?;

        let (nested_fields, parent_fields) = self.relation_fields(model, field, target_model)?;

        let join_alias = format!("t{}", self.next_alias);
        self.next_alias += 1;

        let nested_alias = format!("t{}", self.next_alias);
        self.next_alias += 1;

        let subquery = self.relation_subquery_sql(RelationSql {
            many: field.ty().is_many(),
            source_model_name: model.name(),
            relation_field_name: relation.field,
            target_model,
            selection: relation.selection.clone(),
            parent_table_alias: table_alias.to_owned(),
            nested_alias: nested_alias.clone(),
            nested_fields,
            parent_fields,
        })?;

        self.joins.push(format!(
            "LEFT JOIN LATERAL ({subquery}) AS \"{join_alias}\" ON TRUE"
        ));

        let alias = alias_name(prefix, relation.field);
        Ok(format!("\"{join_alias}\".\"data\" AS \"{alias}\""))
    }

    fn relation_subquery_sql(&mut self, relation: RelationSql<'a>) -> Result<String, sqlx::Error> {
        let mut where_clauses = vec![relation_predicates(
            &relation.nested_alias,
            &relation.nested_fields,
            &relation.parent_table_alias,
            &relation.parent_fields,
        )];
        let mut joins = Vec::new();
        let row_expr = self.json_row_expr(
            relation.target_model,
            &relation.selection,
            &relation.nested_alias,
            &mut joins,
        )?;
        let order_by_clause = self.order_by_sql(
            relation.target_model,
            &relation.selection.order_by,
            &relation.nested_alias,
            &mut joins,
        )?;
        let joins_sql = if joins.is_empty() {
            String::new()
        } else {
            format!(" {}", joins.join(" "))
        };

        if let Some(filter) = relation.selection.filter.as_ref() {
            where_clauses.push(self.filter_sql(
                relation.target_model,
                filter,
                &relation.nested_alias,
            )?);
        }

        let where_clause = where_clauses.join(" AND ");
        let select_order_by_clause = order_by_clause
            .clone()
            .map(|order_by_clause| format!(" ORDER BY {order_by_clause}"))
            .unwrap_or_default();

        if relation.many {
            if relation.selection.skip.is_some() || relation.selection.limit.is_some() {
                let pagination_clause = self.pagination_clause(
                    relation.selection.skip.as_ref(),
                    relation.selection.limit.as_ref(),
                )?;
                let aggregate_table_alias = "__vitrail_nested_rows";
                let aggregate_order_column = "__vitrail_nested_order";
                let select_order_by_clause = if select_order_by_clause.is_empty() {
                    aggregate_order_by(relation.target_model, &relation.nested_alias)
                } else {
                    select_order_by_clause
                };

                Ok(format!(
                    "SELECT COALESCE(json_agg(\"{aggregate_table_alias}\".\"data\" ORDER BY \"{aggregate_table_alias}\".\"{aggregate_order_column}\"), '[]'::json) AS \"data\" FROM (SELECT {row_expr} AS \"data\", row_number() OVER ({select_order_by_clause}) AS \"{aggregate_order_column}\" FROM {} AS \"{}\"{} WHERE {where_clause}{select_order_by_clause}{pagination_clause}) AS \"{aggregate_table_alias}\"",
                    quoted_ident(relation.target_model.name()),
                    relation.nested_alias,
                    joins_sql,
                ))
            } else {
                let aggregate_order_by_clause =
                    if let Some(order_by_clause) = order_by_clause.as_ref() {
                        format!(" ORDER BY {order_by_clause}")
                    } else {
                        aggregate_order_by(relation.target_model, &relation.nested_alias)
                    };

                Ok(format!(
                    "SELECT COALESCE(json_agg({row_expr}{aggregate_order_by_clause}), '[]'::json) AS \"data\" FROM {} AS \"{}\"{} WHERE {where_clause}",
                    quoted_ident(relation.target_model.name()),
                    relation.nested_alias,
                    joins_sql,
                ))
            }
        } else {
            if relation.selection.skip.is_some() || relation.selection.limit.is_some() {
                return Err(schema_error(format!(
                    "relation `{}.{}` is to-one and cannot use `skip` or `limit`",
                    relation.source_model_name, relation.relation_field_name
                )));
            }

            Ok(format!(
                "SELECT {row_expr} AS \"data\" FROM {} AS \"{}\"{} WHERE {where_clause}{select_order_by_clause} LIMIT 1",
                quoted_ident(relation.target_model.name()),
                relation.nested_alias,
                joins_sql,
            ))
        }
    }

    fn filter_sql(
        &mut self,
        model: &'a Model,
        filter: &QueryFilter,
        table_alias: &str,
    ) -> Result<String, sqlx::Error> {
        compile_filter_sql(self, model, filter, table_alias)
    }

    fn pagination_clause(
        &mut self,
        skip: Option<&QueryPagination>,
        limit: Option<&QueryPagination>,
    ) -> Result<String, sqlx::Error> {
        let mut clause = String::new();

        if let Some(limit) = limit {
            let limit = self.pagination_placeholder(limit, "limit")?;
            clause.push_str(&format!(" LIMIT {limit}"));
        }

        if let Some(skip) = skip {
            let skip = self.pagination_placeholder(skip, "skip")?;
            clause.push_str(&format!(" OFFSET {skip}"));
        }

        Ok(clause)
    }

    fn pagination_placeholder(
        &mut self,
        pagination: &QueryPagination,
        kind: &str,
    ) -> Result<String, sqlx::Error> {
        let value = match pagination {
            QueryPagination::Value(value) => QueryVariableValue::Int(*value),
            QueryPagination::Variable(name) => {
                let value = self.variables.get(name).ok_or_else(|| {
                    schema_error(format!("missing query variable `{name}` for `{kind}`"))
                })?;

                match value {
                    QueryVariableValue::Int(value) => QueryVariableValue::Int(*value),
                    other => {
                        return Err(schema_error(format!(
                            "query `{kind}` variable `{name}` must be an integer, got {other:?}"
                        )));
                    }
                }
            }
        };

        let QueryVariableValue::Int(value) = value else {
            unreachable!("pagination values must be integers")
        };

        if value < 0 {
            return Err(schema_error(format!(
                "query `{kind}` must be greater than or equal to 0"
            )));
        }

        self.push_binding(QueryVariableValue::Int(value), ScalarType::Int)
    }

    fn order_by_sql(
        &mut self,
        model: &'a Model,
        orders: &[QueryOrder],
        table_alias: &str,
        joins: &mut Vec<String>,
    ) -> Result<Option<String>, sqlx::Error> {
        if orders.is_empty() {
            return Ok(None);
        }

        let mut items = Vec::new();
        let mut relation_join_aliases = HashMap::new();

        for order in orders {
            self.push_order_sql(
                model,
                order,
                table_alias,
                joins,
                &mut relation_join_aliases,
                &mut items,
            )?;
        }

        Ok(Some(items.join(", ")))
    }

    fn push_order_sql(
        &mut self,
        model: &'a Model,
        order: &QueryOrder,
        table_alias: &str,
        joins: &mut Vec<String>,
        relation_join_aliases: &mut HashMap<String, String>,
        items: &mut Vec<String>,
    ) -> Result<(), sqlx::Error> {
        match order {
            QueryOrder::Scalar { field, direction } => {
                let field = model.field_named(field).ok_or_else(|| {
                    schema_error(format!(
                        "unknown field `{}.{}` in query ordering",
                        model.name(),
                        field
                    ))
                })?;

                let scalar = match field.ty() {
                    FieldType::Scalar(scalar) => scalar.scalar(),
                    FieldType::Relation { .. } => {
                        return Err(schema_error(format!(
                            "field `{}.{}` is not scalar and cannot terminate `order_by`",
                            model.name(),
                            field.name()
                        )));
                    }
                };

                items.push(format!(
                    "{} {}",
                    column_expr(table_alias, field.name(), scalar),
                    match direction {
                        QueryOrderDirection::Asc => "ASC",
                        QueryOrderDirection::Desc => "DESC",
                    }
                ));
                Ok(())
            }
            QueryOrder::Relation { field, orders } => {
                let field = model.field_named(field).ok_or_else(|| {
                    schema_error(format!(
                        "unknown relation `{}.{}` in query ordering",
                        model.name(),
                        field
                    ))
                })?;

                if field.kind().is_scalar() {
                    return Err(schema_error(format!(
                        "field `{}.{}` is not a relation and cannot be traversed in `order_by`",
                        model.name(),
                        field.name()
                    )));
                }

                if field.ty().is_many() {
                    return Err(schema_error(format!(
                        "relation `{}.{}` is to-many and cannot be used in `order_by`",
                        model.name(),
                        field.name()
                    )));
                }

                if orders.is_empty() {
                    return Err(schema_error(format!(
                        "relation `{}.{}` must contain at least one nested `order_by` entry",
                        model.name(),
                        field.name()
                    )));
                }

                let target_model = resolve_schema_model(self.schema, field.ty().name(), "query")
                    .map_err(|_| {
                        schema_error(format!(
                            "relation `{}.{}` points at unknown model `{}`",
                            model.name(),
                            field.name(),
                            field.ty().name()
                        ))
                    })?;

                let (nested_fields, parent_fields) =
                    self.relation_fields(model, field, target_model)?;
                let predicate_template = relation_predicates(
                    "__vitrail_order_join__",
                    &nested_fields,
                    table_alias,
                    &parent_fields,
                );
                let join_key = format!("{}::{predicate_template}", target_model.name());
                let join_alias = if let Some(join_alias) = relation_join_aliases.get(&join_key) {
                    join_alias.clone()
                } else {
                    let join_alias = format!("t{}", self.next_alias);
                    self.next_alias += 1;
                    joins.push(format!(
                        "LEFT JOIN {} AS \"{join_alias}\" ON {}",
                        quoted_ident(target_model.name()),
                        relation_predicates(
                            &join_alias,
                            &nested_fields,
                            table_alias,
                            &parent_fields,
                        ),
                    ));
                    relation_join_aliases.insert(join_key, join_alias.clone());
                    join_alias
                };

                for nested_order in orders {
                    self.push_order_sql(
                        target_model,
                        nested_order,
                        &join_alias,
                        joins,
                        relation_join_aliases,
                        items,
                    )?;
                }

                Ok(())
            }
        }
    }

    fn push_binding(
        &mut self,
        value: QueryVariableValue,
        _scalar: ScalarType,
    ) -> Result<String, sqlx::Error> {
        self.bindings.push(value);
        Ok(format!("${}", self.bindings.len()))
    }

    fn json_row_expr(
        &mut self,
        model: &'a Model,
        selection: &QuerySelection,
        table_alias: &str,
        joins: &mut Vec<String>,
    ) -> Result<String, sqlx::Error> {
        let mut items = Vec::new();

        for field_name in &selection.scalar_fields {
            let field = model.field_named(field_name).ok_or_else(|| {
                schema_error(format!(
                    "unknown field `{}.{}` in query selection",
                    model.name(),
                    field_name
                ))
            })?;

            let scalar = match field.ty() {
                FieldType::Scalar(scalar) => scalar.scalar(),
                FieldType::Relation { .. } => {
                    return Err(schema_error(format!(
                        "field `{}.{}` is not scalar and cannot appear in `select`",
                        model.name(),
                        field_name
                    )));
                }
            };

            items.push(json_column_expr(table_alias, field.name(), scalar));
        }

        for relation in &selection.relations {
            items.push(self.nested_relation_json_expr(model, relation, table_alias, joins)?);
        }

        Ok(format!("json_build_array({})", items.join(", ")))
    }

    fn nested_relation_json_expr(
        &mut self,
        model: &'a Model,
        relation: &QueryRelationSelection,
        table_alias: &str,
        joins: &mut Vec<String>,
    ) -> Result<String, sqlx::Error> {
        let field = model.field_named(relation.field).ok_or_else(|| {
            schema_error(format!(
                "unknown relation `{}.{}` in query include",
                model.name(),
                relation.field
            ))
        })?;

        if field.kind().is_scalar() {
            return Err(schema_error(format!(
                "field `{}.{}` is not a relation and cannot appear in `include`",
                model.name(),
                relation.field
            )));
        }

        let target_model =
            resolve_schema_model(self.schema, field.ty().name(), "query").map_err(|_| {
                schema_error(format!(
                    "relation `{}.{}` points at unknown model `{}`",
                    model.name(),
                    relation.field,
                    field.ty().name()
                ))
            })?;

        let (nested_fields, parent_fields) = self.relation_fields(model, field, target_model)?;

        let join_alias = format!("t{}", self.next_alias);
        self.next_alias += 1;

        let nested_alias = format!("t{}", self.next_alias);
        self.next_alias += 1;

        let subquery = self.relation_subquery_sql(RelationSql {
            many: field.ty().is_many(),
            source_model_name: model.name(),
            relation_field_name: relation.field,
            target_model,
            selection: relation.selection.clone(),
            parent_table_alias: table_alias.to_owned(),
            nested_alias: nested_alias.clone(),
            nested_fields,
            parent_fields,
        })?;

        joins.push(format!(
            "LEFT JOIN LATERAL ({subquery}) AS \"{join_alias}\" ON TRUE"
        ));

        Ok(format!("\"{join_alias}\".\"data\""))
    }

    fn relation_fields(
        &self,
        model: &'a Model,
        field: &'a crate::Field,
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

impl<'a> FilterBuilder<'a> for SqlBuilder<'a> {
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
        self.push_binding(value, scalar)
    }

    fn next_filter_alias(&mut self) -> String {
        let alias = format!("t{}", self.next_alias);
        self.next_alias += 1;
        alias
    }

    fn operation_name(&self) -> &'static str {
        "query"
    }
}

fn aggregate_order_by(model: &Model, table_alias: &str) -> String {
    let primary_key_columns = model.primary_key_columns();
    let field_names = if primary_key_columns.is_empty() {
        model
            .field_named("id")
            .map(|field| vec![field.name()])
            .or_else(|| {
                model
                    .fields()
                    .iter()
                    .find(|field| field.kind().is_scalar())
                    .map(|field| vec![field.name()])
            })
            .unwrap_or_else(|| vec!["id"])
    } else {
        primary_key_columns
    };

    format!(
        " ORDER BY {}",
        field_names
            .into_iter()
            .map(|field_name| format!("\"{table_alias}\".{}", quoted_ident(field_name)))
            .collect::<Vec<_>>()
            .join(", ")
    )
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

pub(crate) fn quoted_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

pub(crate) fn column_expr(table_alias: &str, field_name: &str, scalar: ScalarType) -> String {
    let column_sql = format!("\"{table_alias}\".{}", quoted_ident(field_name));
    match scalar {
        ScalarType::Int => format!("({column_sql})::bigint"),
        ScalarType::DateTime => format!("({column_sql} AT TIME ZONE 'UTC')"),
        _ => column_sql,
    }
}

pub(crate) fn json_column_expr(table_alias: &str, field_name: &str, scalar: ScalarType) -> String {
    let column_sql = format!("\"{table_alias}\".{}", quoted_ident(field_name));
    match scalar {
        ScalarType::Int => format!("({column_sql})::bigint"),
        ScalarType::DateTime => format!("({column_sql} AT TIME ZONE 'UTC')"),
        ScalarType::Decimal => format!("({column_sql})::text"),
        ScalarType::Bytes => format!("encode({column_sql}, 'hex')"),
        _ => column_sql,
    }
}

pub(crate) fn select_expr(
    table_alias: &str,
    field_name: &str,
    scalar: ScalarType,
    alias: &str,
) -> String {
    let expr = column_expr(table_alias, field_name, scalar);
    format!("{expr} AS \"{alias}\"")
}

fn bind_query<'q>(
    mut query: sqlx::query::Query<'q, Postgres, PgArguments>,
    bindings: &'q [QueryVariableValue],
) -> sqlx::query::Query<'q, Postgres, PgArguments> {
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

pub fn schema_error(message: String) -> sqlx::Error {
    sqlx::Error::Protocol(message)
}
