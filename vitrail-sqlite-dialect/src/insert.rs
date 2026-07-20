use std::collections::{HashMap, HashSet};

use serde_json::Value as JsonValue;

use crate::flavor::{SqliteFamilyCapabilities, SqliteFamilyFlavor};
use crate::query::{alias_name, quoted_ident, schema_error};
use crate::schema::{
    Attribute, DefaultFunction, Field, FieldType, Model, Resolution, ScalarType, Schema,
};
use crate::{BindingValue, CompileError, CompiledStatement, OperationKind, ResultColumn};

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

    pub fn push(
        &mut self,
        name: impl Into<String>,
        value: InsertValue,
    ) -> Result<usize, CompileError> {
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

pub fn compile_insert(
    schema: &Schema,
    model_name: &str,
    values: &InsertValues,
    returning_fields: &[&'static str],
) -> Result<CompiledStatement, CompileError> {
    compile_insert_with_flavor(
        schema,
        model_name,
        values,
        returning_fields,
        SqliteFamilyFlavor::Native,
    )
}

#[doc(hidden)]
pub fn compile_insert_with_flavor(
    schema: &Schema,
    model_name: &str,
    values: &InsertValues,
    returning_fields: &[&'static str],
    flavor: SqliteFamilyFlavor,
) -> Result<CompiledStatement, CompileError> {
    let capabilities = flavor.capabilities();
    let model = schema_model(schema, model_name)?;

    validate_insert_values(model, values)?;
    validate_returning_fields(model, returning_fields)?;

    let ordered_values = ordered_insert_values(model, values);
    let (returning_clause, result_columns) =
        build_returning_clause(model, returning_fields, model_name, capabilities)?;

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
            .map(|(index, (field, _))| {
                let placeholder = format!("?{}", index + 1);
                let FieldType::Scalar(scalar) = field.ty() else {
                    unreachable!("validated insert values cannot contain relation fields")
                };

                capabilities.write_parameter_expr(&placeholder, scalar.scalar())
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
        .map(|(_, value)| match value {
            InsertValue::Null => BindingValue::Null,
            InsertValue::Int(value) => BindingValue::Int(*value),
            InsertValue::String(value) => BindingValue::String(value.clone()),
            InsertValue::Bool(value) => BindingValue::Bool(*value),
            InsertValue::Float(value) => BindingValue::Float(*value),
            InsertValue::Bytes(value) => BindingValue::Bytes(value.clone()),
            InsertValue::DateTime(value) => BindingValue::DateTime(*value),
            InsertValue::Json(value) => BindingValue::Json(value.clone()),
        })
        .collect();

    CompiledStatement::new(flavor, sql, bindings, result_columns, OperationKind::Insert)
}

fn validate_insert_values(model: &Model, values: &InsertValues) -> Result<(), CompileError> {
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
) -> Result<(), CompileError> {
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
    capabilities: SqliteFamilyCapabilities,
) -> Result<(Vec<String>, Vec<ResultColumn>), CompileError> {
    let mut selections = Vec::with_capacity(returning_fields.len());
    let mut result_columns = Vec::with_capacity(returning_fields.len());

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
        result_columns.push(ResultColumn::scalar(
            alias.clone(),
            scalar,
            field.ty().is_optional(),
        ));
        let column_sql = format!("\"{}\".{}", model.name(), quoted_ident(field_name));
        let expression = capabilities.result_column_expr(&column_sql, scalar);
        selections.push(format!("{expression} AS \"{alias}\""));
    }

    Ok((selections, result_columns))
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
        InsertValue::Int(_) => {
            matches!(scalar.scalar(), ScalarType::Int | ScalarType::BigInt)
        }
        InsertValue::String(_) => scalar.scalar() == ScalarType::String,
        InsertValue::Bool(_) => scalar.scalar() == ScalarType::Boolean,
        InsertValue::Float(_) => scalar.scalar() == ScalarType::Float,
        InsertValue::Bytes(_) => scalar.scalar() == ScalarType::Bytes,
        InsertValue::DateTime(_) => scalar.scalar() == ScalarType::DateTime,
        InsertValue::Json(_) => scalar.scalar() == ScalarType::Json,
    }
}

fn schema_model<'a>(schema: &'a Schema, requested: &str) -> Result<&'a Model, CompileError> {
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
