use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;

use rust_decimal::Decimal;
use sqlx::postgres::{PgArguments, PgRow};
use sqlx::{Postgres, query::Query as SqlxQuery};

use crate::PgExecutor;
use crate::query::{
    BoxFuture, SchemaAccess, StringValueType, alias_name, quoted_ident, schema_error, select_expr,
};
use crate::schema::{
    Attribute, DefaultFunction, Field, FieldType, Model, Resolution, ScalarType, Schema,
};

/// Runtime contract implemented by executable insert values.
pub trait InsertSpec: Send + Sync {
    type Output: Send + 'static;

    #[doc(hidden)]
    fn execute<'a>(
        &'a self,
        executor: &'a dyn PgExecutor,
    ) -> BoxFuture<'a, Result<Self::Output, sqlx::Error>>;
}

/// Runtime contract implemented by insert result models.
pub trait InsertModel: Sized + Send + 'static {
    type Schema: SchemaAccess;
    type Values: InsertValueSet;

    /// Schema model name being inserted into.
    fn model_name() -> &'static str;

    /// Scalar fields returned by the insert statement.
    fn returning_fields() -> &'static [&'static str];

    /// Decodes the inserted row returned by `RETURNING`.
    fn from_row(row: &PgRow, prefix: &str) -> Result<Self, sqlx::Error>;
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
        Self {
            values: Vec::new(),
            value_indices: HashMap::new(),
        }
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

    pub fn push(
        &mut self,
        name: impl Into<String>,
        value: InsertValue,
    ) -> Result<usize, sqlx::Error> {
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
    Decimal(Decimal),
    Bytes(Vec<u8>),
    DateTime(chrono::DateTime<chrono::Utc>),
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

impl From<Decimal> for InsertValue {
    fn from(value: Decimal) -> Self {
        Self::Decimal(value)
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

impl InsertScalar for Decimal {
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

/// Executable scalar insert for exactly one row.
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
    pub fn to_sql(&self) -> Result<String, sqlx::Error> {
        let (sql, _) = build_insert_sql(
            S::schema(),
            T::model_name(),
            &self.values,
            T::returning_fields(),
        )?;
        Ok(sql)
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
        executor: &'a dyn PgExecutor,
    ) -> BoxFuture<'a, Result<Self::Output, sqlx::Error>> {
        Box::pin(async move {
            let (sql, bindings) = build_insert_sql(
                S::schema(),
                T::model_name(),
                &self.values,
                T::returning_fields(),
            )?;
            let row = executor
                .fetch_one(bind_insert(sqlx::query(&sql), &bindings))
                .await?;

            T::from_row(&row, T::model_name())
        })
    }
}

fn build_insert_sql(
    schema: &Schema,
    model_name: &str,
    values: &InsertValues,
    returning_fields: &[&'static str],
) -> Result<(String, Vec<BoundInsertValue>), sqlx::Error> {
    let model = schema_model(schema, model_name)?;

    validate_insert_values(model, values)?;
    validate_returning_fields(model, returning_fields)?;

    let ordered_values = ordered_insert_values(model, values);
    let returning_clause = build_returning_clause(model, returning_fields, model_name)?;

    let sql = if ordered_values.is_empty() {
        format!(
            "INSERT INTO {} DEFAULT VALUES RETURNING {}",
            quoted_ident(model.name()),
            returning_clause.join(", "),
        )
    } else {
        let columns = ordered_values
            .iter()
            .map(|(field, _)| quoted_ident(field.name()))
            .collect::<Vec<_>>();
        let placeholders = ordered_values
            .iter()
            .enumerate()
            .map(|(index, (field, value))| {
                let placeholder = format!("${}", index + 1);
                match (field.ty(), value) {
                    (FieldType::Scalar(scalar), InsertValue::Null)
                        if scalar.scalar() == ScalarType::Bytes =>
                    {
                        format!("{placeholder}::bytea")
                    }
                    _ => placeholder,
                }
            })
            .collect::<Vec<_>>();

        format!(
            "INSERT INTO {} ({}) VALUES ({}) RETURNING {}",
            quoted_ident(model.name()),
            columns.join(", "),
            placeholders.join(", "),
            returning_clause.join(", "),
        )
    };

    let bindings = ordered_values
        .into_iter()
        .map(|(field, value)| match (field.ty(), value.clone()) {
            (FieldType::Scalar(scalar), InsertValue::Null)
                if scalar.scalar() == ScalarType::Bytes =>
            {
                BoundInsertValue::NullBytes
            }
            (_, value) => value.into(),
        })
        .collect();

    Ok((sql, bindings))
}

#[derive(Clone, Debug, PartialEq)]
enum BoundInsertValue {
    Null,
    NullBytes,
    Int(i64),
    String(String),
    Bool(bool),
    Float(f64),
    Decimal(Decimal),
    Bytes(Vec<u8>),
    DateTime(chrono::DateTime<chrono::Utc>),
}

impl From<InsertValue> for BoundInsertValue {
    fn from(value: InsertValue) -> Self {
        match value {
            InsertValue::Null => Self::Null,
            InsertValue::Int(value) => Self::Int(value),
            InsertValue::String(value) => Self::String(value),
            InsertValue::Bool(value) => Self::Bool(value),
            InsertValue::Float(value) => Self::Float(value),
            InsertValue::Decimal(value) => Self::Decimal(value),
            InsertValue::Bytes(value) => Self::Bytes(value),
            InsertValue::DateTime(value) => Self::DateTime(value),
        }
    }
}

fn validate_insert_values(model: &Model, values: &InsertValues) -> Result<(), sqlx::Error> {
    for provided in values.iter() {
        let field = model.field_named(&provided.name).ok_or_else(|| {
            schema_error(format!(
                "unknown field `{}` in insert for model `{}`",
                provided.name,
                model.name()
            ))
        })?;

        if field.kind().is_relation() {
            return Err(schema_error(format!(
                "relation field `{}` cannot be written in insert for model `{}`",
                field.name(),
                model.name()
            )));
        }

        if !insert_value_matches_field(&provided.value, field) {
            return Err(schema_error(format!(
                "insert value for field `{}` is incompatible with schema type `{}` on model `{}`",
                field.name(),
                field.ty().name(),
                model.name()
            )));
        }
    }

    for field in model.fields() {
        if field.kind().is_relation() {
            continue;
        }

        if values.get(field.name()).is_none() && !field_can_be_omitted(field) {
            return Err(schema_error(format!(
                "missing required scalar field `{}` in insert for model `{}`",
                field.name(),
                model.name()
            )));
        }
    }

    Ok(())
}

fn validate_returning_fields(
    model: &Model,
    returning_fields: &[&'static str],
) -> Result<(), sqlx::Error> {
    if returning_fields.is_empty() {
        return Err(schema_error(format!(
            "insert on model `{}` must return at least one scalar field",
            model.name()
        )));
    }

    let mut seen = HashSet::new();

    for field_name in returning_fields {
        if !seen.insert(*field_name) {
            return Err(schema_error(format!(
                "duplicate returning field `{field_name}` in insert for model `{}`",
                model.name()
            )));
        }

        let field = model.field_named(field_name).ok_or_else(|| {
            schema_error(format!(
                "unknown returning field `{field_name}` in insert for model `{}`",
                model.name()
            ))
        })?;

        if field.kind().is_relation() {
            return Err(schema_error(format!(
                "relation field `{field_name}` cannot be returned from scalar insert for model `{}`",
                model.name()
            )));
        }
    }

    Ok(())
}

fn ordered_insert_values<'a>(
    model: &'a Model,
    values: &'a InsertValues,
) -> Vec<(&'a Field, &'a InsertValue)> {
    let mut ordered = Vec::new();

    for field in model.fields() {
        if field.kind().is_relation() {
            continue;
        }

        if let Some(value) = values.get(field.name()) {
            ordered.push((field, value));
        }
    }

    ordered
}

fn build_returning_clause(
    model: &Model,
    returning_fields: &[&'static str],
    prefix: &str,
) -> Result<Vec<String>, sqlx::Error> {
    let mut selections = Vec::with_capacity(returning_fields.len());

    for field_name in returning_fields {
        let field = model.field_named(field_name).ok_or_else(|| {
            schema_error(format!(
                "unknown returning field `{field_name}` in insert for model `{}`",
                model.name()
            ))
        })?;

        let scalar = scalar_field_type(field).ok_or_else(|| {
            schema_error(format!(
                "relation field `{field_name}` cannot be returned from scalar insert for model `{}`",
                model.name()
            ))
        })?;

        let alias = alias_name(prefix, field_name);
        selections.push(select_expr(model.name(), field_name, scalar, &alias));
    }

    Ok(selections)
}

fn field_can_be_omitted(field: &Field) -> bool {
    field.ty().is_optional() || field_has_supported_default(field)
}

fn field_has_supported_default(field: &Field) -> bool {
    field.attributes().iter().any(|attribute| {
        matches!(
            attribute,
            Attribute::Default(default)
                if matches!(
                    default.function(),
                    DefaultFunction::Autoincrement | DefaultFunction::Now
                )
        )
    })
}

fn scalar_field_type(field: &Field) -> Option<ScalarType> {
    match field.ty() {
        FieldType::Scalar(scalar) => Some(scalar.scalar()),
        FieldType::Relation { .. } => None,
    }
}

fn insert_value_matches_field(value: &InsertValue, field: &Field) -> bool {
    let FieldType::Scalar(scalar) = field.ty() else {
        return false;
    };

    match value {
        InsertValue::Null => scalar.optional(),
        InsertValue::Int(_) => scalar.scalar() == ScalarType::Int,
        InsertValue::String(_) => scalar.scalar() == ScalarType::String,
        InsertValue::Bool(_) => scalar.scalar() == ScalarType::Boolean,
        InsertValue::Float(_) => scalar.scalar() == ScalarType::Float,
        InsertValue::Decimal(_) => scalar.scalar() == ScalarType::Decimal,
        InsertValue::Bytes(_) => scalar.scalar() == ScalarType::Bytes,
        InsertValue::DateTime(_) => scalar.scalar() == ScalarType::DateTime,
    }
}

fn schema_model<'a>(schema: &'a Schema, requested: &str) -> Result<&'a Model, sqlx::Error> {
    match schema.resolve_model(requested) {
        Resolution::Found(model) => Ok(model),
        Resolution::NotFound => Err(schema_error(format!(
            "unknown model `{requested}` in insert"
        ))),
        Resolution::Ambiguous(models) => {
            let candidates = models
                .into_iter()
                .map(|model| format!("`{}`", model.name()))
                .collect::<Vec<_>>()
                .join(", ");

            Err(schema_error(format!(
                "ambiguous model `{requested}` in insert; matches {candidates}"
            )))
        }
    }
}

fn bind_insert<'q>(
    mut query: SqlxQuery<'q, Postgres, PgArguments>,
    bindings: &'q [BoundInsertValue],
) -> SqlxQuery<'q, Postgres, PgArguments> {
    for binding in bindings {
        query = match binding {
            BoundInsertValue::Null => query.bind(Option::<i64>::None),
            BoundInsertValue::NullBytes => query.bind(Option::<Vec<u8>>::None),
            BoundInsertValue::Int(value) => query.bind(*value),
            BoundInsertValue::String(value) => query.bind(value),
            BoundInsertValue::Bool(value) => query.bind(*value),
            BoundInsertValue::Float(value) => query.bind(*value),
            BoundInsertValue::Decimal(value) => query.bind(*value),
            BoundInsertValue::Bytes(value) => query.bind(value),
            BoundInsertValue::DateTime(value) => query.bind(*value),
        };
    }

    query
}
