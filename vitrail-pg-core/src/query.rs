use std::marker::PhantomData;

use heck::ToUpperCamelCase;
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
        let nested_prefix = alias_name(prefix, relation.field);
        if !selection_is_null(row, &nested_prefix, &relation.selection)? {
            return Ok(false);
        }
    }

    Ok(true)
}

pub fn alias_name(prefix: &str, field: &str) -> String {
    format!("{prefix}__{field}")
}

fn build_query_sql(schema: &Schema, selection: &QuerySelection) -> Result<String, sqlx::Error> {
    let root_model = schema_model(schema, selection.model)
        .ok_or_else(|| schema_error(format!("unknown model `{}`", selection.model)))?;

    let mut builder = SqlBuilder {
        schema,
        selects: Vec::new(),
        joins: Vec::new(),
        next_alias: 1,
    };

    builder.push_selection(root_model, selection, selection.model, "t0")?;

    Ok(format!(
        "SELECT {} FROM {} AS \"t0\"{}",
        builder.selects.join(", "),
        quoted_ident(root_model.name()),
        if builder.joins.is_empty() {
            String::new()
        } else {
            format!(" {}", builder.joins.join(" "))
        }
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
            .references()
            .iter()
            .map(String::as_str)
            .collect(),
        reverse_relation
            .fields()
            .iter()
            .map(String::as_str)
            .collect(),
    ))
}

struct SqlBuilder<'a> {
    schema: &'a Schema,
    selects: Vec<String>,
    joins: Vec<String>,
    next_alias: usize,
}

impl<'a> SqlBuilder<'a> {
    fn push_selection(
        &mut self,
        model: &'a Model,
        selection: &QuerySelection,
        prefix: &str,
        table_alias: &str,
    ) -> Result<(), sqlx::Error> {
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

            self.selects.push(select_expr(
                table_alias,
                field.name(),
                scalar,
                &alias_name(prefix, field.name()),
            ));
        }

        for relation in &selection.relations {
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

            let (local_fields, referenced_fields) = match field.relation() {
                Some(relation_info) => (
                    relation_info
                        .fields()
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>(),
                    relation_info
                        .references()
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>(),
                ),
                None => infer_relation_fields(model, field, target_model)?,
            };

            if local_fields.len() != 1 || referenced_fields.len() != 1 {
                return Err(schema_error(format!(
                    "relation `{}.{}` currently requires exactly one local field and one referenced field",
                    model.name(),
                    relation.field
                )));
            }

            let join_alias = format!("t{}", self.next_alias);
            self.next_alias += 1;
            let join_kind = if field.ty().is_optional() {
                "LEFT JOIN"
            } else {
                "INNER JOIN"
            };

            self.joins.push(format!(
                "{join_kind} {} AS \"{}\" ON \"{}\".{} = \"{}\".{}",
                quoted_ident(target_model.name()),
                join_alias,
                table_alias,
                quoted_ident(local_fields[0]),
                join_alias,
                quoted_ident(referenced_fields[0]),
            ));

            let nested_prefix = alias_name(prefix, relation.field);
            self.push_selection(
                target_model,
                &relation.selection,
                &nested_prefix,
                &join_alias,
            )?;
        }

        Ok(())
    }
}

fn quoted_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn select_expr(table_alias: &str, field_name: &str, scalar: ScalarType, alias: &str) -> String {
    let column_sql = format!("\"{table_alias}\".{}", quoted_ident(field_name));
    let expr = match scalar {
        ScalarType::Int => format!("({column_sql})::bigint"),
        ScalarType::String
        | ScalarType::Boolean
        | ScalarType::DateTime
        | ScalarType::Float
        | ScalarType::Decimal
        | ScalarType::Bytes
        | ScalarType::Json => column_sql,
    };

    format!("{expr} AS \"{alias}\"")
}

const fn schema_error(message: String) -> sqlx::Error {
    sqlx::Error::Protocol(message)
}
