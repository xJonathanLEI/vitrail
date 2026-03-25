use std::collections::HashMap;
use std::marker::PhantomData;

use heck::ToUpperCamelCase;
use serde_json::Value as JsonValue;
use sqlx::postgres::{PgArguments, PgPool, PgRow};
use sqlx::{Postgres, Row as _, ValueRef as _};

pub use futures_util::future::BoxFuture;

use crate::schema::{FieldType, Model, ScalarType, Schema};

/// Runtime contract implemented by executable query values.
pub trait QuerySpec: Send + Sync {
    type Output: Send + 'static;

    fn fetch_many<'a>(
        &'a self,
        pool: &'a PgPool,
    ) -> BoxFuture<'a, Result<Vec<Self::Output>, sqlx::Error>>;

    fn fetch_optional<'a>(
        &'a self,
        pool: &'a PgPool,
    ) -> BoxFuture<'a, Result<Option<Self::Output>, sqlx::Error>> {
        Box::pin(async move { Ok(self.fetch_many(pool).await?.into_iter().next()) })
    }

    fn fetch_first<'a>(
        &'a self,
        pool: &'a PgPool,
    ) -> BoxFuture<'a, Result<Self::Output, sqlx::Error>> {
        Box::pin(async move {
            self.fetch_optional(pool)
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
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryRelationSelection {
    pub field: &'static str,
    pub selection: QuerySelection,
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

#[derive(Clone, Debug, PartialEq)]
pub enum QueryVariableValue {
    Null,
    Int(i64),
    String(String),
    Bool(bool),
    Float(f64),
    DateTime(chrono::DateTime<chrono::Utc>),
}

impl From<i64> for QueryVariableValue {
    fn from(value: i64) -> Self {
        Self::Int(value)
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

impl From<bool> for QueryVariableValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<f64> for QueryVariableValue {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<chrono::DateTime<chrono::Utc>> for QueryVariableValue {
    fn from(value: chrono::DateTime<chrono::Utc>) -> Self {
        Self::DateTime(value)
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

#[derive(Clone, Debug, PartialEq)]
pub enum QueryFilterValue {
    Variable(String),
    Value(QueryVariableValue),
}

impl QueryFilterValue {
    pub fn variable(name: impl Into<String>) -> Self {
        Self::Variable(name.into())
    }

    pub fn value(value: impl Into<QueryVariableValue>) -> Self {
        Self::Value(value.into())
    }
}

impl<T> From<T> for QueryFilterValue
where
    T: Into<QueryVariableValue>,
{
    fn from(value: T) -> Self {
        Self::Value(value.into())
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
}

impl QueryFilter {
    pub fn eq(field: &'static str, value: impl Into<QueryFilterValue>) -> Self {
        Self::Eq {
            field,
            value: value.into(),
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
        pool: &'a PgPool,
    ) -> BoxFuture<'a, Result<Vec<Self::Output>, sqlx::Error>> {
        Box::pin(async move {
            let selection = self.selection();
            let (sql, bindings) = build_query_sql(S::schema(), &selection, &self.variables)?;
            let rows = bind_query(sqlx::query(&sql), &bindings)
                .fetch_all(pool)
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
        pool: &'a PgPool,
    ) -> BoxFuture<'a, Result<Option<Self::Output>, sqlx::Error>> {
        Box::pin(async move {
            let selection = self.selection();
            let (sql, bindings) = build_query_sql(S::schema(), &selection, &self.variables)?;
            let sql = format!("{sql} LIMIT 1");
            let row = bind_query(sqlx::query(&sql), &bindings)
                .fetch_optional(pool)
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

pub fn json_as_f64(value: &JsonValue) -> Result<f64, sqlx::Error> {
    value
        .as_f64()
        .ok_or_else(|| schema_error("expected JSON float in query result".to_owned()))
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

fn build_query_sql(
    schema: &Schema,
    selection: &QuerySelection,
    variables: &QueryVariables,
) -> Result<(String, Vec<QueryVariableValue>), sqlx::Error> {
    let root_model = schema_model(schema, selection.model)
        .ok_or_else(|| schema_error(format!("unknown model `{}`", selection.model)))?;

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

    let sql = format!(
        "SELECT {} FROM {} AS \"t0\"{}{}",
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
    );

    Ok((sql, builder.bindings))
}

fn schema_model<'a>(schema: &'a Schema, requested: &str) -> Option<&'a Model> {
    schema
        .models()
        .iter()
        .find(|model| requested == model.name() || requested == model.name().to_upper_camel_case())
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
    target_model: &'a Model,
    selection: QuerySelection,
    parent_table_alias: String,
    nested_alias: String,
    nested_field: String,
    parent_field: String,
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

        let target_model = schema_model(self.schema, field.ty().name()).ok_or_else(|| {
            schema_error(format!(
                "relation `{}.{}` points at unknown model `{}`",
                model.name(),
                relation.field,
                field.ty().name()
            ))
        })?;

        let (nested_fields, parent_fields) = self.relation_fields(model, field, target_model)?;

        if nested_fields.len() != 1 || parent_fields.len() != 1 {
            return Err(schema_error(format!(
                "relation `{}.{}` currently requires exactly one parent field and one nested field",
                model.name(),
                relation.field
            )));
        }

        let join_alias = format!("t{}", self.next_alias);
        self.next_alias += 1;

        let nested_alias = format!("t{}", self.next_alias);
        self.next_alias += 1;

        let subquery = self.relation_subquery_sql(RelationSql {
            many: field.ty().is_many(),
            target_model,
            selection: relation.selection.clone(),
            parent_table_alias: table_alias.to_owned(),
            nested_alias: nested_alias.clone(),
            nested_field: nested_fields[0].to_owned(),
            parent_field: parent_fields[0].to_owned(),
        })?;

        self.joins.push(format!(
            "LEFT JOIN LATERAL ({subquery}) AS \"{join_alias}\" ON TRUE"
        ));

        let alias = alias_name(prefix, relation.field);
        Ok(format!("\"{join_alias}\".\"data\" AS \"{alias}\""))
    }

    fn relation_subquery_sql(&mut self, relation: RelationSql<'a>) -> Result<String, sqlx::Error> {
        let mut where_clauses = vec![format!(
            "\"{}\".{} = \"{}\".{}",
            relation.nested_alias,
            quoted_ident(&relation.nested_field),
            relation.parent_table_alias,
            quoted_ident(&relation.parent_field),
        )];
        let mut joins = Vec::new();
        let row_expr = self.json_row_expr(
            relation.target_model,
            &relation.selection,
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

        if relation.many {
            Ok(format!(
                "SELECT COALESCE(json_agg({row_expr}{}), '[]'::json) AS \"data\" FROM {} AS \"{}\"{} WHERE {where_clause}",
                aggregate_order_by(relation.target_model, &relation.nested_alias),
                quoted_ident(relation.target_model.name()),
                relation.nested_alias,
                joins_sql,
            ))
        } else {
            Ok(format!(
                "SELECT {row_expr} AS \"data\" FROM {} AS \"{}\"{} WHERE {where_clause} LIMIT 1",
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
            QueryFilter::Eq { field, value } => {
                let field = model.field_named(field).ok_or_else(|| {
                    schema_error(format!(
                        "unknown field `{}.{}` in query filter",
                        model.name(),
                        field
                    ))
                })?;

                let scalar = match field.ty() {
                    FieldType::Scalar(scalar) => scalar.scalar(),
                    FieldType::Relation { .. } => {
                        return Err(schema_error(format!(
                            "field `{}.{}` is not scalar and cannot appear in `where`",
                            model.name(),
                            field.name()
                        )));
                    }
                };

                let binding = self.resolve_filter_value(value)?;
                if matches!(binding, QueryVariableValue::Null) {
                    Ok(format!(
                        "\"{table_alias}\".{} IS NULL",
                        quoted_ident(field.name())
                    ))
                } else {
                    let placeholder = self.push_binding(binding, scalar)?;
                    Ok(format!(
                        "{} = {}",
                        column_expr(table_alias, field.name(), scalar),
                        placeholder
                    ))
                }
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

            items.push(column_expr(table_alias, field.name(), scalar));
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

        let target_model = schema_model(self.schema, field.ty().name()).ok_or_else(|| {
            schema_error(format!(
                "relation `{}.{}` points at unknown model `{}`",
                model.name(),
                relation.field,
                field.ty().name()
            ))
        })?;

        let (nested_fields, parent_fields) = self.relation_fields(model, field, target_model)?;

        if nested_fields.len() != 1 || parent_fields.len() != 1 {
            return Err(schema_error(format!(
                "relation `{}.{}` currently requires exactly one parent field and one nested field",
                model.name(),
                relation.field
            )));
        }

        let join_alias = format!("t{}", self.next_alias);
        self.next_alias += 1;

        let nested_alias = format!("t{}", self.next_alias);
        self.next_alias += 1;

        let subquery = self.relation_subquery_sql(RelationSql {
            many: field.ty().is_many(),
            target_model,
            selection: relation.selection.clone(),
            parent_table_alias: table_alias.to_owned(),
            nested_alias: nested_alias.clone(),
            nested_field: nested_fields[0].to_owned(),
            parent_field: parent_fields[0].to_owned(),
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

fn aggregate_order_by(model: &Model, table_alias: &str) -> String {
    let field_name = model
        .field_named("id")
        .map(|field| field.name())
        .or_else(|| {
            model
                .fields()
                .iter()
                .find(|field| field.kind().is_scalar())
                .map(|field| field.name())
        })
        .unwrap_or("id");

    format!(" ORDER BY \"{table_alias}\".{}", quoted_ident(field_name))
}

fn quoted_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn column_expr(table_alias: &str, field_name: &str, scalar: ScalarType) -> String {
    let column_sql = format!("\"{table_alias}\".{}", quoted_ident(field_name));
    match scalar {
        ScalarType::Int => format!("({column_sql})::bigint"),
        ScalarType::DateTime => format!("({column_sql} AT TIME ZONE 'UTC')"),
        _ => column_sql,
    }
}

fn select_expr(table_alias: &str, field_name: &str, scalar: ScalarType, alias: &str) -> String {
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
            QueryVariableValue::DateTime(value) => query.bind(*value),
        };
    }

    query
}

pub fn schema_error(message: String) -> sqlx::Error {
    sqlx::Error::Protocol(message)
}
