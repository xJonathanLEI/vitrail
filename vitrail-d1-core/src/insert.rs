use std::collections::HashMap;
use std::marker::PhantomData;

use serde_json::Value as JsonValue;
use vitrail_sqlite_dialect::{CompiledStatement, SqliteFamilyFlavor, compile_insert_with_flavor};

use crate::query::{BoxFuture, StringValueType, schema_error};
use crate::statement::execute_rows;
use crate::{D1Executor, D1Row, Error, Schema, SchemaAccess};

/// Runtime contract implemented by executable D1 insert values.
pub trait InsertSpec: Send + Sync {
    type Output: Send + 'static;

    #[doc(hidden)]
    fn execute<'a>(
        &'a self,
        executor: &'a dyn D1Executor,
    ) -> BoxFuture<'a, Result<Self::Output, Error>>;
}

/// Runtime contract implemented by D1 insert result models.
pub trait InsertModel: Sized + Send + 'static {
    type Schema: SchemaAccess;
    type Values: InsertValueSet;

    /// Schema model name being inserted into.
    fn model_name() -> &'static str;

    /// Scalar fields returned by the insert statement.
    fn returning_fields() -> &'static [&'static str];

    /// Decodes the inserted row returned by `RETURNING`.
    fn from_row(row: &D1Row, prefix: &str) -> Result<Self, Error>;
}

/// Converts a user-provided input into executable insert values.
pub trait InsertValueSet: Send + 'static {
    fn into_insert_values(self) -> InsertValues;
}

impl InsertValueSet for InsertValues {
    fn into_insert_values(self) -> InsertValues {
        self
    }
}

impl InsertValueSet for () {
    fn into_insert_values(self) -> InsertValues {
        InsertValues::new()
    }
}

/// Converts a supported D1 scalar into an insert value.
pub trait InsertScalar: Send {
    fn into_insert_value(self) -> InsertValue;
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct InsertValues {
    values: Vec<InsertFieldValue>,
    value_indices: HashMap<String, usize>,
}

impl InsertValues {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_values(values: Vec<(impl Into<String>, InsertValue)>) -> Self {
        let mut insert_values = Self::new();

        for (name, value) in values {
            insert_values
                .push(name, value)
                .expect("insert field names must be unique");
        }

        insert_values
    }

    pub fn push(&mut self, name: impl Into<String>, value: InsertValue) -> Result<usize, Error> {
        let name = name.into();

        if self.value_indices.contains_key(&name) {
            return Err(schema_error(format!("duplicate insert field `{name}`")));
        }

        let index = self.values.len();
        self.values.push(InsertFieldValue {
            name: name.clone(),
            value,
        });
        self.value_indices.insert(name, index);
        Ok(index)
    }

    pub fn get(&self, name: &str) -> Option<&InsertValue> {
        self.value_indices
            .get(name)
            .and_then(|index| self.values.get(*index))
            .map(|field| &field.value)
    }

    pub fn iter(&self) -> impl Iterator<Item = &InsertFieldValue> {
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
pub struct InsertFieldValue {
    pub name: String,
    pub value: InsertValue,
}

#[derive(Clone, Debug, PartialEq)]
pub enum InsertValue {
    Null,
    Int(i64),
    String(String),
    Bool(bool),
    Float(f64),
    Bytes(Vec<u8>),
    DateTime(chrono::DateTime<chrono::Utc>),
    Json(JsonValue),
}

impl From<i64> for InsertValue {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<String> for InsertValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for InsertValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<bool> for InsertValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<f64> for InsertValue {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<Vec<u8>> for InsertValue {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value)
    }
}

impl From<&[u8]> for InsertValue {
    fn from(value: &[u8]) -> Self {
        Self::Bytes(value.to_vec())
    }
}

impl From<chrono::DateTime<chrono::Utc>> for InsertValue {
    fn from(value: chrono::DateTime<chrono::Utc>) -> Self {
        Self::DateTime(value)
    }
}

impl From<JsonValue> for InsertValue {
    fn from(value: JsonValue) -> Self {
        Self::Json(value)
    }
}

impl<T> From<Option<T>> for InsertValue
where
    T: Into<InsertValue>,
{
    fn from(value: Option<T>) -> Self {
        match value {
            Some(value) => value.into(),
            None => Self::Null,
        }
    }
}

impl InsertScalar for i64 {
    fn into_insert_value(self) -> InsertValue {
        self.into()
    }
}

impl InsertScalar for &str {
    fn into_insert_value(self) -> InsertValue {
        self.into()
    }
}

impl InsertScalar for bool {
    fn into_insert_value(self) -> InsertValue {
        self.into()
    }
}

impl InsertScalar for f64 {
    fn into_insert_value(self) -> InsertValue {
        self.into()
    }
}

impl InsertScalar for Vec<u8> {
    fn into_insert_value(self) -> InsertValue {
        self.into()
    }
}

impl InsertScalar for &[u8] {
    fn into_insert_value(self) -> InsertValue {
        self.into()
    }
}

impl InsertScalar for chrono::DateTime<chrono::Utc> {
    fn into_insert_value(self) -> InsertValue {
        self.into()
    }
}

impl InsertScalar for JsonValue {
    fn into_insert_value(self) -> InsertValue {
        self.into()
    }
}

impl<T> InsertScalar for T
where
    T: StringValueType,
{
    fn into_insert_value(self) -> InsertValue {
        InsertValue::String(self.into_db_string())
    }
}

impl<T> InsertScalar for Option<T>
where
    T: InsertScalar,
{
    fn into_insert_value(self) -> InsertValue {
        match self {
            Some(value) => value.into_insert_value(),
            None => InsertValue::Null,
        }
    }
}

/// Executable scalar D1 insert for exactly one row.
#[derive(Clone, Debug)]
pub struct Insert<S, T> {
    values: InsertValues,
    _marker: PhantomData<(S, T)>,
}

impl<S, T> Insert<S, T>
where
    T: InsertModel<Schema = S>,
{
    pub fn new(values: T::Values) -> Self {
        Self {
            values: values.into_insert_values(),
            _marker: PhantomData,
        }
    }

    pub fn with_values(values: InsertValues) -> Self {
        Self {
            values,
            _marker: PhantomData,
        }
    }

    pub fn values(&self) -> &InsertValues {
        &self.values
    }
}

impl<S, T> Insert<S, T>
where
    S: SchemaAccess,
    T: InsertModel<Schema = S>,
{
    pub fn to_sql(&self) -> Result<String, Error> {
        Ok(compile_insert_statement(
            S::schema(),
            T::model_name(),
            &self.values,
            T::returning_fields(),
        )?
        .sql()
        .to_owned())
    }
}

impl<S, T> InsertSpec for Insert<S, T>
where
    S: SchemaAccess,
    T: InsertModel<Schema = S> + Sync,
{
    type Output = T;

    fn execute<'a>(
        &'a self,
        executor: &'a dyn D1Executor,
    ) -> BoxFuture<'a, Result<Self::Output, Error>> {
        Box::pin(async move {
            let statement = compile_insert_statement(
                S::schema(),
                T::model_name(),
                &self.values,
                T::returning_fields(),
            )?;
            let rows = execute_rows(executor, &statement).await?;

            if rows.len() != 1 {
                return Err(Error::decode(format!(
                    "D1 insert returning result contains {} rows; expected exactly one",
                    rows.len(),
                )));
            }

            let row = rows.into_iter().next().ok_or(Error::RowNotFound)?;
            T::from_row(&row, T::model_name())
        })
    }
}

pub(crate) fn compile_insert_statement(
    schema: &Schema,
    model_name: &str,
    values: &InsertValues,
    returning_fields: &[&'static str],
) -> Result<CompiledStatement, Error> {
    let mut dialect_values = vitrail_sqlite_dialect::InsertValues::new();

    for field in values.iter() {
        dialect_values.push(field.name.clone(), dialect_insert_value(&field.value))?;
    }

    compile_insert_with_flavor(
        schema,
        model_name,
        &dialect_values,
        returning_fields,
        SqliteFamilyFlavor::D1,
    )
    .map_err(Error::from)
}

fn dialect_insert_value(value: &InsertValue) -> vitrail_sqlite_dialect::InsertValue {
    match value {
        InsertValue::Null => vitrail_sqlite_dialect::InsertValue::Null,
        InsertValue::Int(value) => vitrail_sqlite_dialect::InsertValue::Int(*value),
        InsertValue::String(value) => vitrail_sqlite_dialect::InsertValue::String(value.clone()),
        InsertValue::Bool(value) => vitrail_sqlite_dialect::InsertValue::Bool(*value),
        InsertValue::Float(value) => vitrail_sqlite_dialect::InsertValue::Float(*value),
        InsertValue::Bytes(value) => vitrail_sqlite_dialect::InsertValue::Bytes(value.clone()),
        InsertValue::DateTime(value) => vitrail_sqlite_dialect::InsertValue::DateTime(*value),
        InsertValue::Json(value) => vitrail_sqlite_dialect::InsertValue::Json(value.clone()),
    }
}
