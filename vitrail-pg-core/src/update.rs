use std::collections::HashMap;
use std::marker::PhantomData;

use rust_decimal::Decimal;
use sqlx::postgres::PgArguments;
use sqlx::{Postgres, query::Query as SqlxQuery};
use uuid::Uuid;

use crate::PgExecutor;
use crate::query::{
    BoxFuture, QueryFilter, QueryFilterValue, QueryVariableSet, QueryVariableValue, QueryVariables,
    SchemaAccess, StringValueType, column_expr, quoted_ident, schema_error,
};
use crate::schema::{Field, FieldType, Model, Resolution, ScalarType, Schema};

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
    let model = schema_model(schema, model_name)?;

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

fn schema_model<'a>(schema: &'a Schema, requested: &str) -> Result<&'a Model, sqlx::Error> {
    match schema.resolve_model(requested) {
        Resolution::Found(model) => Ok(model),
        Resolution::NotFound => Err(schema_error(format!(
            "unknown model `{requested}` in update"
        ))),
        Resolution::Ambiguous(models) => {
            let candidates = models
                .into_iter()
                .map(|model| format!("`{}`", model.name()))
                .collect::<Vec<_>>()
                .join(", ");

            Err(schema_error(format!(
                "ambiguous model `{requested}` in update; matches {candidates}"
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
                        "unknown field `{}.{}` in update filter",
                        model.name(),
                        field
                    ))
                })?;

                let scalar = match field.ty() {
                    FieldType::Scalar(scalar) => scalar.scalar(),
                    FieldType::Relation { .. } => {
                        return Err(schema_error(format!(
                            "field `{}.{}` is not scalar and cannot appear in update `where`",
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
                        "unknown relation `{}.{}` in update filter",
                        model.name(),
                        field
                    ))
                })?;

                if relation_field.kind().is_scalar() {
                    return Err(schema_error(format!(
                        "field `{}.{}` is not a relation and cannot appear as a nested update filter",
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
        }
    }
}

fn update_value_matches_field(value: &UpdateValue, field: &Field) -> bool {
    let FieldType::Scalar(scalar) = field.ty() else {
        return false;
    };

    match value {
        UpdateValue::Null => scalar.optional(),
        UpdateValue::Int(_) => scalar.scalar() == ScalarType::Int,
        UpdateValue::String(_) => scalar.scalar() == ScalarType::String && !field.has_db_uuid(),
        UpdateValue::Bool(_) => scalar.scalar() == ScalarType::Boolean,
        UpdateValue::Float(_) => scalar.scalar() == ScalarType::Float,
        UpdateValue::Decimal(_) => scalar.scalar() == ScalarType::Decimal,
        UpdateValue::Bytes(_) => scalar.scalar() == ScalarType::Bytes,
        UpdateValue::DateTime(_) => scalar.scalar() == ScalarType::DateTime,
        UpdateValue::Uuid(_) => scalar.scalar() == ScalarType::String && field.has_db_uuid(),
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
        };
    }

    query
}
