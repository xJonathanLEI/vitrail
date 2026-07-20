use std::collections::HashMap;

use serde_json::Value as JsonValue;

use crate::filter::{FilterBuilder, compile_filter_sql, schema_model as resolve_schema_model};
use crate::flavor::{SqliteFamilyCapabilities, SqliteFamilyFlavor};
use crate::schema::{Field, FieldType, Model, ScalarType, Schema};
use crate::{BindingValue, CompileError, CompiledStatement, OperationKind, ResultColumn};

#[derive(Clone, Debug, PartialEq)]
pub struct QuerySelection<F = QueryFilter> {
    pub model: &'static str,
    pub scalar_fields: Vec<&'static str>,
    pub relations: Vec<QueryRelationSelection<F>>,
    pub filter: Option<F>,
    pub order_by: Vec<QueryOrder>,
    pub skip: Option<QueryPagination>,
    pub limit: Option<QueryPagination>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryRelationSelection<F = QueryFilter> {
    pub field: &'static str,
    pub selection: QuerySelection<F>,
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
        Self::default()
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
    ) -> Result<usize, CompileError> {
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

#[derive(Clone, Debug, PartialEq)]
pub enum QueryVariableValue {
    Null,
    Int(i64),
    String(String),
    Bool(bool),
    Float(f64),
    Bytes(Vec<u8>),
    DateTime(chrono::DateTime<chrono::Utc>),
    Json(JsonValue),
    List(Vec<QueryVariableValue>),
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

impl From<Vec<u8>> for QueryVariableValue {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value)
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

impl From<JsonValue> for QueryVariableValue {
    fn from(value: JsonValue) -> Self {
        Self::Json(value)
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

#[derive(Clone, Debug, PartialEq)]
pub enum QueryFilterValues {
    Variable(String),
    Values(Vec<QueryFilterValue>),
}

impl QueryFilterValues {
    pub fn variable(name: impl Into<String>) -> Self {
        Self::Variable(name.into())
    }

    pub fn values(values: impl IntoIterator<Item = QueryVariableValue>) -> Self {
        Self::Values(values.into_iter().map(QueryFilterValue::Value).collect())
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

    pub fn r#in(field: &'static str, values: QueryFilterValues) -> Self {
        Self::In { field, values }
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

pub fn alias_name(prefix: &str, field: &str) -> String {
    format!("{prefix}__{field}")
}

pub fn compile_query(
    schema: &Schema,
    selection: &QuerySelection,
    variables: &QueryVariables,
) -> Result<CompiledStatement, CompileError> {
    compile_query_with_flavor(schema, selection, variables, SqliteFamilyFlavor::Native)
}

#[doc(hidden)]
pub fn compile_query_with_flavor(
    schema: &Schema,
    selection: &QuerySelection,
    variables: &QueryVariables,
    flavor: SqliteFamilyFlavor,
) -> Result<CompiledStatement, CompileError> {
    let root_model = resolve_schema_model(schema, selection.model, "query")?;

    let mut builder = SqlBuilder {
        schema,
        variables,
        capabilities: flavor.capabilities(),
        bindings: Vec::new(),
        result_columns: Vec::new(),
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
    let pagination_clause =
        builder.pagination_clause(selection.skip.as_ref(), selection.limit.as_ref())?;

    let sql = format!(
        "SELECT {} FROM {} AS \"t0\"{}{}{}{}",
        selects.join(", "),
        quoted_ident(root_model.name()),
        if order_joins.is_empty() {
            String::new()
        } else {
            format!(" {}", order_joins.join(" "))
        },
        where_clause
            .map(|where_clause| format!(" WHERE {where_clause}"))
            .unwrap_or_default(),
        order_by_clause
            .map(|order_by_clause| format!(" ORDER BY {order_by_clause}"))
            .unwrap_or_default(),
        pagination_clause,
    );

    CompiledStatement::new(
        flavor,
        sql,
        builder.bindings,
        builder.result_columns,
        OperationKind::Query,
    )
}

fn model_names_match(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn infer_relation_fields<'a>(
    model: &'a Model,
    field: &'a Field,
    target_model: &'a Model,
) -> Result<(Vec<&'a str>, Vec<&'a str>), CompileError> {
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
    capabilities: SqliteFamilyCapabilities,
    bindings: Vec<BindingValue>,
    result_columns: Vec<ResultColumn>,
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
    ) -> Result<Vec<String>, CompileError> {
        let mut selects = Vec::with_capacity(selection.scalar_fields.len());

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

            let alias = alias_name(prefix, field.name());
            self.result_columns.push(ResultColumn::scalar(
                alias.clone(),
                scalar,
                field.ty().is_optional(),
            ));
            let column_sql = format!("\"{table_alias}\".{}", quoted_ident(field.name()));
            let expression = self.capabilities.result_column_expr(&column_sql, scalar);
            selects.push(format!("{expression} AS \"{alias}\""));
        }

        for relation in &selection.relations {
            selects.push(self.relation_select(model, relation, prefix, table_alias)?);
        }

        if selects.is_empty() {
            return Err(schema_error(format!(
                "query selection for model `{}` must contain at least one field",
                model.name()
            )));
        }

        Ok(selects)
    }

    fn relation_select(
        &mut self,
        model: &'a Model,
        relation: &QueryRelationSelection,
        prefix: &str,
        table_alias: &str,
    ) -> Result<String, CompileError> {
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
        let nested_alias = format!("t{}", self.next_alias);
        self.next_alias += 1;

        let subquery = self.relation_subquery_sql(RelationSql {
            many: field.ty().is_many(),
            source_model_name: model.name(),
            relation_field_name: relation.field,
            target_model,
            selection: relation.selection.clone(),
            parent_table_alias: table_alias.to_owned(),
            nested_alias,
            nested_fields,
            parent_fields,
        })?;

        let alias = alias_name(prefix, relation.field);
        self.result_columns.push(ResultColumn::relation(
            alias.clone(),
            field.ty().is_many(),
            field.ty().is_optional(),
        ));
        Ok(format!("({subquery}) AS \"{alias}\""))
    }

    fn relation_subquery_sql(&mut self, relation: RelationSql<'a>) -> Result<String, CompileError> {
        let mut where_clauses = vec![relation_predicates(
            &relation.nested_alias,
            &relation.nested_fields,
            &relation.parent_table_alias,
            &relation.parent_fields,
        )];
        let row_expr = self.json_row_expr(
            relation.target_model,
            &relation.selection,
            &relation.nested_alias,
        )?;

        let mut order_joins = Vec::new();
        let order_by_clause = self.order_by_sql(
            relation.target_model,
            &relation.selection.order_by,
            &relation.nested_alias,
            &mut order_joins,
        )?;
        let joins_sql = if order_joins.is_empty() {
            String::new()
        } else {
            format!(" {}", order_joins.join(" "))
        };

        if let Some(filter) = relation.selection.filter.as_ref() {
            where_clauses.push(self.filter_sql(
                relation.target_model,
                filter,
                &relation.nested_alias,
            )?);
        }

        let where_clause = where_clauses.join(" AND ");
        let explicit_order_by_clause = order_by_clause
            .map(|order_by_clause| format!(" ORDER BY {order_by_clause}"))
            .unwrap_or_default();

        if relation.many {
            let aggregate_table_alias = "__vitrail_nested_rows";
            let select_order_by_clause = if explicit_order_by_clause.is_empty() {
                aggregate_order_by(relation.target_model, &relation.nested_alias)
            } else {
                explicit_order_by_clause
            };
            let pagination_clause = self.pagination_clause(
                relation.selection.skip.as_ref(),
                relation.selection.limit.as_ref(),
            )?;

            Ok(format!(
                "SELECT COALESCE(json_group_array(json(\"{aggregate_table_alias}\".\"data\")), json('[]')) AS \"data\" FROM (SELECT {row_expr} AS \"data\" FROM {} AS \"{}\"{} WHERE {where_clause}{select_order_by_clause}{pagination_clause}) AS \"{aggregate_table_alias}\"",
                quoted_ident(relation.target_model.name()),
                relation.nested_alias,
                joins_sql,
            ))
        } else {
            if relation.selection.skip.is_some() || relation.selection.limit.is_some() {
                return Err(schema_error(format!(
                    "relation `{}.{}` is to-one and cannot use `skip` or `limit`",
                    relation.source_model_name, relation.relation_field_name
                )));
            }

            Ok(format!(
                "SELECT {row_expr} AS \"data\" FROM {} AS \"{}\"{} WHERE {where_clause}{explicit_order_by_clause} LIMIT 1",
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
    ) -> Result<String, CompileError> {
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

            let column_sql = format!("\"{table_alias}\".{}", quoted_ident(field.name()));
            items.push(
                self.capabilities
                    .nested_json_column_expr(&column_sql, scalar),
            );
        }

        for relation in &selection.relations {
            items.push(self.nested_relation_json_expr(model, relation, table_alias)?);
        }

        if items.is_empty() {
            return Err(schema_error(format!(
                "query selection for model `{}` must contain at least one field",
                model.name()
            )));
        }

        Ok(self.capabilities.json_array_expr(&items))
    }

    fn nested_relation_json_expr(
        &mut self,
        model: &'a Model,
        relation: &QueryRelationSelection,
        table_alias: &str,
    ) -> Result<String, CompileError> {
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
        let nested_alias = format!("t{}", self.next_alias);
        self.next_alias += 1;

        let subquery = self.relation_subquery_sql(RelationSql {
            many: field.ty().is_many(),
            source_model_name: model.name(),
            relation_field_name: relation.field,
            target_model,
            selection: relation.selection.clone(),
            parent_table_alias: table_alias.to_owned(),
            nested_alias,
            nested_fields,
            parent_fields,
        })?;

        Ok(format!("json(({subquery}))"))
    }

    fn relation_fields(
        &self,
        model: &'a Model,
        field: &'a Field,
        target_model: &'a Model,
    ) -> Result<(Vec<&'a str>, Vec<&'a str>), CompileError> {
        match field.relation() {
            Some(relation_info) => Ok((
                relation_info
                    .references()
                    .iter()
                    .map(String::as_str)
                    .collect(),
                relation_info.fields().iter().map(String::as_str).collect(),
            )),
            None => infer_relation_fields(model, field, target_model),
        }
    }

    fn filter_sql(
        &mut self,
        model: &'a Model,
        filter: &QueryFilter,
        table_alias: &str,
    ) -> Result<String, CompileError> {
        compile_filter_sql(self, model, filter, table_alias)
    }

    fn pagination_clause(
        &mut self,
        skip: Option<&QueryPagination>,
        limit: Option<&QueryPagination>,
    ) -> Result<String, CompileError> {
        let mut clause = String::new();

        if let Some(limit) = limit {
            let limit = self.pagination_placeholder(limit, "limit")?;
            clause.push_str(&format!(" LIMIT {limit}"));
        } else if skip.is_some() {
            clause.push_str(" LIMIT -1");
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
    ) -> Result<String, CompileError> {
        let value = match pagination {
            QueryPagination::Value(value) => *value,
            QueryPagination::Variable(name) => {
                let value = self.variables.get(name).ok_or_else(|| {
                    schema_error(format!("missing query variable `{name}` for `{kind}`"))
                })?;

                match value {
                    QueryVariableValue::Int(value) => *value,
                    other => {
                        return Err(schema_error(format!(
                            "query `{kind}` variable `{name}` must be an integer, got {other:?}"
                        )));
                    }
                }
            }
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
    ) -> Result<Option<String>, CompileError> {
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
    ) -> Result<(), CompileError> {
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

                let column_sql = format!("\"{table_alias}\".{}", quoted_ident(field.name()));
                items.push(format!(
                    "{} {}",
                    self.capabilities.stored_column_expr(&column_sql, scalar),
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
        scalar: ScalarType,
    ) -> Result<String, CompileError> {
        self.bindings.push(match value {
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
        });
        let placeholder = format!("?{}", self.bindings.len());

        Ok(self
            .capabilities
            .comparison_parameter_expr(&placeholder, scalar))
    }
}

impl<'a> FilterBuilder<'a> for SqlBuilder<'a> {
    fn schema(&self) -> &'a Schema {
        self.schema
    }

    fn variables(&self) -> &'a QueryVariables {
        self.variables
    }

    fn capabilities(&self) -> SqliteFamilyCapabilities {
        self.capabilities
    }

    fn push_filter_binding(
        &mut self,
        value: QueryVariableValue,
        scalar: ScalarType,
    ) -> Result<String, CompileError> {
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

pub(crate) fn schema_error(message: impl Into<String>) -> CompileError {
    CompileError::new(message)
}
