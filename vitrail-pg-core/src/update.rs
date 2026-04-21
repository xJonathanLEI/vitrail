use std::collections::HashMap;
use std::marker::PhantomData;

use rust_decimal::Decimal;
use sqlx::postgres::PgArguments;
use sqlx::{Postgres, query::Query as SqlxQuery};
use uuid::Uuid;

use crate::PgExecutor;
use crate::filter::{FilterBuilder, compile_filter_sql, schema_model as resolve_schema_model};
use crate::query::{
    BoxFuture, QueryFilter, QueryVariableSet, QueryVariableValue, QueryVariables, SchemaAccess,
    StringValueType, quoted_ident, schema_error,
};
use crate::schema::{Field, FieldType, Model, ScalarType, Schema};

/// Runtime contract implemented by executable update values.
pub trait UpdateSpec: Send + Sync {
    type Output: Send + 'static;

    #[doc(hidden)]
    fn execute<'a>(
        &'a self,
        executor: &'a dyn PgExecutor,
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
    Decimal(Decimal),
    Bytes(Vec<u8>),
    DateTime(chrono::DateTime<chrono::Utc>),
    Uuid(Uuid),
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

impl From<Decimal> for UpdateValue {
    fn from(value: Decimal) -> Self {
        Self::Decimal(value)
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

impl From<Uuid> for UpdateValue {
    fn from(value: Uuid) -> Self {
        Self::Uuid(value)
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

impl UpdateScalar for Decimal {
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

impl UpdateScalar for Uuid {
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
        executor: &'a dyn PgExecutor,
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
                .execute(bind_update(sqlx::query(&sql), &bindings))
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
) -> Result<(String, Vec<BoundValue>), sqlx::Error> {
    let model = resolve_schema_model(schema, model_name, "update")?;

    validate_update_values(model, values)?;

    let ordered_values = ordered_update_values(model, values);
    let mut builder = UpdateSqlBuilder {
        schema,
        variables,
        bindings: Vec::new(),
        next_alias: 1,
    };

    let assignments = ordered_values
        .iter()
        .map(|(field, value)| {
            let scalar = match field.ty() {
                FieldType::Scalar(scalar) => scalar.scalar(),
                FieldType::Relation { .. } => {
                    return Err(schema_error(format!(
                        "field `{}.{}` is not scalar and cannot appear in `data`",
                        model.name(),
                        field.name()
                    )));
                }
            };
            let placeholder =
                builder.push_update_binding((*value).clone(), scalar, field.has_db_uuid())?;
            Ok(format!(
                r#"{} = {}"#,
                quoted_ident(field.name()),
                placeholder
            ))
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()?;

    let where_clause = filter
        .map(|filter| builder.filter_sql(model, filter, "t0"))
        .transpose()?;

    let sql = format!(
        r#"UPDATE {} AS "t0" SET {}{}"#,
        quoted_ident(model.name()),
        assignments.join(", "),
        where_clause
            .map(|where_clause| format!(" WHERE {where_clause}"))
            .unwrap_or_default(),
    );

    Ok((sql, builder.bindings))
}

fn validate_update_values(model: &Model, values: &UpdateValues) -> Result<(), sqlx::Error> {
    if values.is_empty() {
        return Err(schema_error(format!(
            "update on model `{}` must write at least one scalar field",
            model.name()
        )));
    }

    for provided in values.iter() {
        let field = model.field_named(&provided.name).ok_or_else(|| {
            schema_error(format!(
                "unknown field `{}` in update for model `{}`",
                provided.name,
                model.name()
            ))
        })?;

        if field.kind().is_relation() {
            return Err(schema_error(format!(
                "relation field `{}` cannot be written in update for model `{}`",
                field.name(),
                model.name()
            )));
        }

        if !update_value_matches_field(&provided.value, field) {
            return Err(schema_error(format!(
                "update value for field `{}` is incompatible with schema type `{}` on model `{}`",
                field.name(),
                field.ty().name(),
                model.name()
            )));
        }
    }

    Ok(())
}

fn ordered_update_values<'a>(
    model: &'a Model,
    values: &'a UpdateValues,
) -> Vec<(&'a Field, &'a UpdateValue)> {
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

struct UpdateSqlBuilder<'a> {
    schema: &'a Schema,
    variables: &'a QueryVariables,
    bindings: Vec<BoundValue>,
    next_alias: usize,
}

impl<'a> UpdateSqlBuilder<'a> {
    fn filter_sql(
        &mut self,
        model: &'a Model,
        filter: &QueryFilter,
        table_alias: &str,
    ) -> Result<String, sqlx::Error> {
        compile_filter_sql(self, model, filter, table_alias)
    }

    fn push_update_binding(
        &mut self,
        value: UpdateValue,
        scalar: ScalarType,
        is_db_uuid: bool,
    ) -> Result<String, sqlx::Error> {
        let binding = match (value, scalar, is_db_uuid) {
            (UpdateValue::Null, ScalarType::Bytes, _) => BoundValue::NullBytes,
            (UpdateValue::Null, ScalarType::String, true) => BoundValue::NullUuid,
            (value, _, _) => value.into(),
        };

        self.bindings.push(binding);
        Ok(format!("${}", self.bindings.len()))
    }

    fn push_query_binding(
        &mut self,
        value: QueryVariableValue,
        _scalar: ScalarType,
    ) -> Result<String, sqlx::Error> {
        self.bindings.push(value.into());
        Ok(format!("${}", self.bindings.len()))
    }
}

impl<'a> FilterBuilder<'a> for UpdateSqlBuilder<'a> {
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
        "update"
    }
}

#[derive(Clone, Debug, PartialEq)]
enum BoundValue {
    Null,
    NullBytes,
    NullUuid,
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

impl From<UpdateValue> for BoundValue {
    fn from(value: UpdateValue) -> Self {
        match value {
            UpdateValue::Null => Self::Null,
            UpdateValue::Int(value) => Self::Int(value),
            UpdateValue::String(value) => Self::String(value),
            UpdateValue::Bool(value) => Self::Bool(value),
            UpdateValue::Float(value) => Self::Float(value),
            UpdateValue::Decimal(value) => Self::Decimal(value),
            UpdateValue::Bytes(value) => Self::Bytes(value),
            UpdateValue::DateTime(value) => Self::DateTime(value),
            UpdateValue::Uuid(value) => Self::Uuid(value),
        }
    }
}

impl From<QueryVariableValue> for BoundValue {
    fn from(value: QueryVariableValue) -> Self {
        match value {
            QueryVariableValue::Null => Self::Null,
            QueryVariableValue::Int(value) => Self::Int(value),
            QueryVariableValue::String(value) => Self::String(value),
            QueryVariableValue::Bool(value) => Self::Bool(value),
            QueryVariableValue::Float(value) => Self::Float(value),
            QueryVariableValue::Decimal(value) => Self::Decimal(value),
            QueryVariableValue::Bytes(value) => Self::Bytes(value),
            QueryVariableValue::DateTime(value) => Self::DateTime(value),
            QueryVariableValue::Uuid(value) => Self::Uuid(value),
            QueryVariableValue::List(values) => Self::List(values),
        }
    }
}

fn update_value_matches_field(value: &UpdateValue, field: &Field) -> bool {
    let FieldType::Scalar(scalar) = field.ty() else {
        return false;
    };

    match value {
        UpdateValue::Null => scalar.optional(),
        UpdateValue::Int(_) => {
            matches!(scalar.scalar(), ScalarType::Int | ScalarType::BigInt)
        }
        UpdateValue::String(_) => scalar.scalar() == ScalarType::String && !field.has_db_uuid(),
        UpdateValue::Bool(_) => scalar.scalar() == ScalarType::Boolean,
        UpdateValue::Float(_) => scalar.scalar() == ScalarType::Float,
        UpdateValue::Decimal(_) => scalar.scalar() == ScalarType::Decimal,
        UpdateValue::Bytes(_) => scalar.scalar() == ScalarType::Bytes,
        UpdateValue::DateTime(_) => scalar.scalar() == ScalarType::DateTime,
        UpdateValue::Uuid(_) => scalar.scalar() == ScalarType::String && field.has_db_uuid(),
    }
}

fn bind_update<'q>(
    mut query: SqlxQuery<'q, Postgres, PgArguments>,
    bindings: &'q [BoundValue],
) -> SqlxQuery<'q, Postgres, PgArguments> {
    for binding in bindings {
        query = match binding {
            BoundValue::Null => query.bind(Option::<i64>::None),
            BoundValue::NullBytes => query.bind(Option::<Vec<u8>>::None),
            BoundValue::NullUuid => query.bind(Option::<Uuid>::None),
            BoundValue::Int(value) => query.bind(*value),
            BoundValue::String(value) => query.bind(value),
            BoundValue::Bool(value) => query.bind(*value),
            BoundValue::Float(value) => query.bind(*value),
            BoundValue::Decimal(value) => query.bind(*value),
            BoundValue::Bytes(value) => query.bind(value),
            BoundValue::DateTime(value) => query.bind(*value),
            BoundValue::Uuid(value) => query.bind(*value),
            BoundValue::List(values) => {
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
