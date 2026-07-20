use std::collections::HashMap;

use serde_json::Value as JsonValue;

use crate::filter::{
    FilterBuilder, compile_filter_sql, filter_binding_expr, schema_model as resolve_schema_model,
};
use crate::query::{QueryFilter, QueryVariableValue, QueryVariables, quoted_ident, schema_error};
use crate::schema::{Field, FieldType, Model, ScalarType, Schema};
use crate::{BindingValue, CompileError, CompiledStatement, OperationKind};

#[derive(Clone, Debug, Default, PartialEq)]
pub struct UpdateValues {
    values: Vec<UpdateFieldValue>,
    value_indices: HashMap<String, usize>,
}

impl UpdateValues {
    pub fn new() -> Self {
        Self::default()
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
    ) -> Result<usize, CompileError> {
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

pub fn compile_update_many(
    schema: &Schema,
    model_name: &str,
    values: &UpdateValues,
    filter: Option<&QueryFilter>,
    variables: &QueryVariables,
) -> Result<CompiledStatement, CompileError> {
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

            let placeholder = builder.push_update_binding((*value).clone(), scalar)?;

            Ok(format!(
                r#"{} = {}"#,
                quoted_ident(field.name()),
                placeholder
            ))
        })
        .collect::<Result<Vec<_>, CompileError>>()?;

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

    Ok(CompiledStatement::new(
        sql,
        builder.bindings,
        Vec::new(),
        OperationKind::UpdateMany,
    ))
}

fn validate_update_values(model: &Model, values: &UpdateValues) -> Result<(), CompileError> {
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
    bindings: Vec<BindingValue>,
    next_alias: usize,
}

impl<'a> UpdateSqlBuilder<'a> {
    fn filter_sql(
        &mut self,
        model: &'a Model,
        filter: &QueryFilter,
        table_alias: &str,
    ) -> Result<String, CompileError> {
        compile_filter_sql(self, model, filter, table_alias)
    }

    fn push_update_binding(
        &mut self,
        value: UpdateValue,
        scalar: ScalarType,
    ) -> Result<String, CompileError> {
        self.bindings.push(binding_from_update_value(value));

        let placeholder = format!("?{}", self.bindings.len());

        match scalar {
            ScalarType::Json => Ok(format!("json({placeholder})")),
            _ => Ok(placeholder),
        }
    }

    fn push_query_binding(
        &mut self,
        value: QueryVariableValue,
        scalar: ScalarType,
    ) -> Result<String, CompileError> {
        self.bindings.push(binding_from_query_value(value));

        let placeholder = format!("?{}", self.bindings.len());

        Ok(filter_binding_expr(&placeholder, scalar))
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
    ) -> Result<String, CompileError> {
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

fn binding_from_update_value(value: UpdateValue) -> BindingValue {
    match value {
        UpdateValue::Null => BindingValue::Null,
        UpdateValue::Int(value) => BindingValue::Int(value),
        UpdateValue::String(value) => BindingValue::String(value),
        UpdateValue::Bool(value) => BindingValue::Bool(value),
        UpdateValue::Float(value) => BindingValue::Float(value),
        UpdateValue::Bytes(value) => BindingValue::Bytes(value),
        UpdateValue::DateTime(value) => BindingValue::DateTime(value),
        UpdateValue::Json(value) => BindingValue::Json(value),
    }
}

fn binding_from_query_value(value: QueryVariableValue) -> BindingValue {
    match value {
        QueryVariableValue::Null => BindingValue::Null,
        QueryVariableValue::Int(value) => BindingValue::Int(value),
        QueryVariableValue::String(value) => BindingValue::String(value),
        QueryVariableValue::Bool(value) => BindingValue::Bool(value),
        QueryVariableValue::Float(value) => BindingValue::Float(value),
        QueryVariableValue::Bytes(value) => BindingValue::Bytes(value),
        QueryVariableValue::DateTime(value) => BindingValue::DateTime(value),
        QueryVariableValue::Json(value) => BindingValue::Json(value),
        QueryVariableValue::List(_) => {
            unreachable!("SQLite list filters must be expanded before compilation")
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
        UpdateValue::String(_) => scalar.scalar() == ScalarType::String,
        UpdateValue::Bool(_) => scalar.scalar() == ScalarType::Boolean,
        UpdateValue::Float(_) => scalar.scalar() == ScalarType::Float,
        UpdateValue::Bytes(_) => scalar.scalar() == ScalarType::Bytes,
        UpdateValue::DateTime(_) => scalar.scalar() == ScalarType::DateTime,
        UpdateValue::Json(_) => scalar.scalar() == ScalarType::Json,
    }
}
