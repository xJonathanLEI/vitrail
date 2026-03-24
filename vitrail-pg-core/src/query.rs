use std::marker::PhantomData;

use heck::ToUpperCamelCase;
use serde_json::Value as JsonValue;
use sqlx::postgres::{PgPool, PgRow};
use sqlx::{Row as _, ValueRef as _};

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
        Box::pin(async move {
            let mut rows = self.fetch_many(pool).await?;
            Ok(rows.drain(..).next())
        })
    }
}

pub trait SchemaAccess: Send + Sync + 'static {
    fn schema() -> &'static Schema;
}

#[derive(Clone, Debug)]
pub struct QuerySelection {
    pub model: &'static str,
    pub scalar_fields: Vec<&'static str>,
    pub relations: Vec<QueryRelationSelection>,
}

#[derive(Clone, Debug)]
pub struct QueryRelationSelection {
    pub field: &'static str,
    pub selection: QuerySelection,
}

pub trait QueryValue: Sized + Send + 'static {
    fn from_json(value: &JsonValue) -> Result<Self, sqlx::Error>;
}

pub trait QueryModel: Sized + Send + 'static {
    type Schema: SchemaAccess;

    fn model_name() -> &'static str;

    fn selection() -> QuerySelection;

    fn from_row(row: &PgRow, prefix: &str) -> Result<Self, sqlx::Error>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Query<S, T> {
    _marker: PhantomData<(S, T)>,
}

impl<S, T> Query<S, T> {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<S, T> Query<S, T>
where
    S: SchemaAccess,
    T: QueryModel<Schema = S>,
{
    pub fn to_sql(&self) -> Result<String, sqlx::Error> {
        let selection = T::selection();
        build_query_sql(S::schema(), &selection)
    }
}

impl<S, T> QuerySpec for Query<S, T>
where
    S: SchemaAccess,
    T: QueryModel<Schema = S> + Sync,
{
    type Output = T;

    fn fetch_many<'a>(
        &'a self,
        pool: &'a PgPool,
    ) -> BoxFuture<'a, Result<Vec<Self::Output>, sqlx::Error>> {
        Box::pin(async move {
            let selection = T::selection();
            let sql = self.to_sql()?;
            let rows = sqlx::query(&sql).fetch_all(pool).await?;
            let mut values = Vec::with_capacity(rows.len());
            let root_prefix = selection.model;

            for row in rows {
                values.push(T::from_row(&row, root_prefix)?);
            }

            Ok(values)
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

pub fn json_object_field<'a>(
    value: &'a JsonValue,
    field: &str,
) -> Result<&'a JsonValue, sqlx::Error> {
    value
        .get(field)
        .ok_or_else(|| schema_error(format!("missing JSON field `{field}` in query result")))
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

    chrono::DateTime::parse_from_rfc3339(value)
        .map(|datetime| datetime.with_timezone(&chrono::Utc))
        .map_err(|error| schema_error(format!("invalid JSON datetime in query result: {error}")))
}

fn build_query_sql(schema: &Schema, selection: &QuerySelection) -> Result<String, sqlx::Error> {
    let root_model = schema_model(schema, selection.model)
        .ok_or_else(|| schema_error(format!("unknown model `{}`", selection.model)))?;

    let mut builder = SqlBuilder {
        schema,
        joins: Vec::new(),
        next_alias: 1,
    };

    let selects = builder.root_selects(root_model, selection, selection.model, "t0")?;

    Ok(format!(
        "SELECT {} FROM {} AS \"t0\"{}",
        selects.join(", "),
        quoted_ident(root_model.name()),
        if builder.joins.is_empty() {
            String::new()
        } else {
            format!(" {}", builder.joins.join(" "))
        },
    ))
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
        let where_clause = format!(
            "\"{}\".{} = \"{}\".{}",
            relation.nested_alias,
            quoted_ident(&relation.nested_field),
            relation.parent_table_alias,
            quoted_ident(&relation.parent_field),
        );
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

            items.push(column_expr(
                table_alias,
                field.name(),
                matches!(scalar, ScalarType::Int),
            ));
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

fn column_expr(table_alias: &str, field_name: &str, cast_int: bool) -> String {
    let column_sql = format!("\"{table_alias}\".{}", quoted_ident(field_name));
    if cast_int {
        format!("({column_sql})::bigint")
    } else {
        column_sql
    }
}

fn select_expr(table_alias: &str, field_name: &str, scalar: ScalarType, alias: &str) -> String {
    let expr = column_expr(table_alias, field_name, matches!(scalar, ScalarType::Int));
    format!("{expr} AS \"{alias}\"")
}

pub fn schema_error(message: String) -> sqlx::Error {
    sqlx::Error::Protocol(message)
}
