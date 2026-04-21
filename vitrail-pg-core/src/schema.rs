use std::collections::{HashMap, HashSet};

use heck::ToUpperCamelCase;

use crate::validation::{ValidationError, ValidationErrors, ValidationLocation};

/// Schema definition for `vitrail-pg`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Schema {
    models: Vec<Model>,
    external_tables: Vec<String>,
}

impl Schema {
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::new()
    }

    pub fn models(&self) -> &[Model] {
        &self.models
    }

    pub fn external_tables(&self) -> &[String] {
        &self.external_tables
    }

    pub fn model(&self, name: &str) -> Option<&Model> {
        self.models.iter().find(|model| model.name == name)
    }

    pub(crate) fn validate(&self) -> Result<(), ValidationErrors> {
        let mut errors = Vec::new();

        if self.models.is_empty() {
            errors.push(ValidationError::new(
                ValidationLocation::Schema,
                "schema must declare at least one model",
            ));
            return Err(ValidationErrors::from(errors));
        }

        let mut seen_models = HashMap::<&str, usize>::new();

        for (index, model) in self.models.iter().enumerate() {
            if let Some(previous_index) = seen_models.insert(model.name.as_str(), index) {
                errors.push(ValidationError::new(
                    ValidationLocation::Model {
                        model: model.name.clone(),
                    },
                    format!("duplicate model `{}`", model.name),
                ));
                errors.push(ValidationError::new(
                    ValidationLocation::Model {
                        model: self.models[previous_index].name.clone(),
                    },
                    "first declaration of this model",
                ));
            }
        }

        for model in &self.models {
            model.validate_shallow(&mut errors);
        }

        for model in &self.models {
            model.validate_types(self, &mut errors);
        }

        for model in &self.models {
            model.validate_relations(self, &mut errors);
        }

        let mut seen_external_tables = HashSet::new();
        for table in &self.external_tables {
            let normalized = match normalize_external_table_name(table) {
                Ok(normalized) => normalized,
                Err(message) => {
                    errors.push(ValidationError::new(
                        ValidationLocation::ExternalTable {
                            table: table.clone(),
                        },
                        message,
                    ));
                    continue;
                }
            };

            if !seen_external_tables.insert(normalized.clone()) {
                errors.push(ValidationError::new(
                    ValidationLocation::ExternalTable {
                        table: table.clone(),
                    },
                    format!("duplicate external table `{}`", table),
                ));
            }

            if self.model(&normalized).is_some() {
                errors.push(ValidationError::new(
                    ValidationLocation::ExternalTable {
                        table: table.clone(),
                    },
                    format!(
                        "external table `{}` conflicts with managed model `{}`",
                        table, normalized
                    ),
                ));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(ValidationErrors::from(errors))
        }
    }

    pub(crate) fn resolve_model(&self, requested: &str) -> Resolution<'_> {
        let mut matches = Vec::new();

        for model in &self.models {
            if requested == model.name || requested == model.name.to_upper_camel_case() {
                matches.push(model);
            }
        }

        match matches.len() {
            0 => Resolution::NotFound,
            1 => Resolution::Found(matches[0]),
            _ => Resolution::Ambiguous(matches),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SchemaBuilder {
    models: Vec<Model>,
    external_tables: Vec<String>,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn model(mut self, model: Model) -> Self {
        self.models.push(model);
        self
    }

    pub fn models(mut self, models: Vec<Model>) -> Self {
        self.models = models;
        self
    }

    pub fn external_table(mut self, table: impl Into<String>) -> Self {
        self.external_tables.push(table.into());
        self
    }

    pub fn external_tables(mut self, tables: Vec<String>) -> Self {
        self.external_tables = tables;
        self
    }

    pub fn build(self) -> Result<Schema, ValidationErrors> {
        let schema = Schema {
            models: self.models,
            external_tables: self.external_tables,
        };
        schema.validate()?;
        Ok(schema)
    }
}

fn normalize_external_table_name(table: &str) -> Result<String, String> {
    if table.is_empty() {
        return Err("external table name must not be empty".to_owned());
    }

    if let Some((schema, table_name)) = table.split_once('.') {
        if schema != "public" {
            return Err(format!(
                "external table `{}` must target the `public` schema",
                table
            ));
        }

        if table_name.is_empty() {
            return Err(format!(
                "external table `{}` must include a table name",
                table
            ));
        }

        return Ok(table_name.to_owned());
    }

    Ok(table.to_owned())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Model {
    name: String,
    fields: Vec<Field>,
    attributes: Vec<ModelAttribute>,
}

impl Model {
    pub fn builder(name: impl Into<String>) -> ModelBuilder {
        ModelBuilder::new(name)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    pub fn attributes(&self) -> &[ModelAttribute] {
        &self.attributes
    }

    pub fn primary_key_columns(&self) -> Vec<&str> {
        if let Some(primary_key) = self.primary_key_attribute() {
            return primary_key.fields.iter().map(String::as_str).collect();
        }

        self.fields
            .iter()
            .filter(|field| field.has_id())
            .map(|field| field.name.as_str())
            .collect()
    }

    pub fn unique_column_sets(&self) -> Vec<Vec<&str>> {
        self.attributes
            .iter()
            .filter_map(|attribute| match attribute {
                ModelAttribute::Unique(unique) => {
                    Some(unique.fields.iter().map(String::as_str).collect::<Vec<_>>())
                }
                ModelAttribute::Id(_) | ModelAttribute::Index(_) => None,
            })
            .collect()
    }

    pub fn index_column_sets(&self) -> Vec<Vec<&str>> {
        self.attributes
            .iter()
            .filter_map(|attribute| match attribute {
                ModelAttribute::Index(index) => {
                    Some(index.fields.iter().map(String::as_str).collect::<Vec<_>>())
                }
                ModelAttribute::Id(_) | ModelAttribute::Unique(_) => None,
            })
            .collect()
    }

    fn primary_key_attribute(&self) -> Option<&ModelPrimaryKeyAttribute> {
        self.attributes
            .iter()
            .find_map(|attribute| match attribute {
                ModelAttribute::Id(primary_key) => Some(primary_key),
                ModelAttribute::Unique(_) | ModelAttribute::Index(_) => None,
            })
    }

    fn validate_shallow(&self, errors: &mut Vec<ValidationError>) {
        if self.fields.is_empty() {
            errors.push(ValidationError::new(
                ValidationLocation::Model {
                    model: self.name.clone(),
                },
                "model must declare at least one field",
            ));
            return;
        }

        let mut seen_fields = HashMap::<&str, usize>::new();
        let mut id_fields = Vec::new();
        let mut seen_model_id = false;

        for attribute in &self.attributes {
            match attribute {
                ModelAttribute::Id(primary_key) => {
                    if seen_model_id {
                        errors.push(ValidationError::new(
                            ValidationLocation::ModelAttribute {
                                model: self.name.clone(),
                                attribute: "@@id".to_owned(),
                            },
                            "duplicate `@@id` attribute",
                        ));
                    } else {
                        seen_model_id = true;
                    }

                    primary_key.validate(self, errors);
                }
                ModelAttribute::Unique(unique) => unique.validate(self, errors),
                ModelAttribute::Index(index) => index.validate(self, errors),
            }
        }

        for (index, field) in self.fields.iter().enumerate() {
            if let Some(previous_index) = seen_fields.insert(field.name.as_str(), index) {
                errors.push(ValidationError::new(
                    ValidationLocation::Field {
                        model: self.name.clone(),
                        field: field.name.clone(),
                    },
                    format!("duplicate field `{}` in model `{}`", field.name, self.name),
                ));
                errors.push(ValidationError::new(
                    ValidationLocation::Field {
                        model: self.name.clone(),
                        field: self.fields[previous_index].name.clone(),
                    },
                    "first declaration of this field",
                ));
            }

            field.validate_attributes(&self.name, errors);

            if field.has_id() {
                id_fields.push(field);
            }
        }

        let has_model_id = self.primary_key_attribute().is_some();

        match (id_fields.len(), has_model_id) {
            (0, false) => errors.push(ValidationError::new(
                ValidationLocation::Model {
                    model: self.name.clone(),
                },
                format!(
                    "model `{}` must declare exactly one primary key using `@id` or `@@id`",
                    self.name
                ),
            )),
            (0, true) | (1, false) => {}
            (_, true) if !id_fields.is_empty() => {
                errors.push(ValidationError::new(
                    ValidationLocation::Model {
                        model: self.name.clone(),
                    },
                    format!(
                        "model `{}` cannot mix field-level `@id` with model-level `@@id`",
                        self.name
                    ),
                ));

                for field in id_fields {
                    errors.push(ValidationError::new(
                        ValidationLocation::Field {
                            model: self.name.clone(),
                            field: field.name.clone(),
                        },
                        "remove this `@id` because the model already declares `@@id`",
                    ));
                }
            }
            _ => {
                errors.push(ValidationError::new(
                    ValidationLocation::Model {
                        model: self.name.clone(),
                    },
                    format!(
                        "model `{}` declares multiple `@id` fields; use `@@id([...])` for a compound primary key",
                        self.name
                    ),
                ));

                for field in id_fields.into_iter().skip(1) {
                    errors.push(ValidationError::new(
                        ValidationLocation::Field {
                            model: self.name.clone(),
                            field: field.name.clone(),
                        },
                        "extra `@id` field declared here",
                    ));
                }
            }
        }
    }

    fn validate_types(&self, schema: &Schema, errors: &mut Vec<ValidationError>) {
        for field in &self.fields {
            field.validate_type(schema, &self.name, errors);
        }
    }

    fn validate_relations(&self, schema: &Schema, errors: &mut Vec<ValidationError>) {
        for field in &self.fields {
            if field.kind().is_scalar() {
                continue;
            }

            let Some(relation) = field.relation() else {
                continue;
            };

            let target_model = match schema.resolve_model(field.ty.name()) {
                Resolution::Found(model) => model,
                Resolution::NotFound => {
                    errors.push(ValidationError::new(
                        ValidationLocation::FieldType {
                            model: self.name.clone(),
                            field: field.name.clone(),
                            ty: field.ty.name().to_owned(),
                        },
                        format!(
                            "unknown relation target model `{}` for field `{}`",
                            field.ty.name(),
                            field.name
                        ),
                    ));
                    continue;
                }
                Resolution::Ambiguous(models) => {
                    let candidates = models
                        .into_iter()
                        .map(|model| format!("`{}`", model.name))
                        .collect::<Vec<_>>()
                        .join(", ");

                    errors.push(ValidationError::new(
                        ValidationLocation::FieldType {
                            model: self.name.clone(),
                            field: field.name.clone(),
                            ty: field.ty.name().to_owned(),
                        },
                        format!(
                            "ambiguous relation target `{}` for field `{}`; matches {}",
                            field.ty.name(),
                            field.name,
                            candidates
                        ),
                    ));
                    continue;
                }
            };

            if relation.fields.is_empty() {
                errors.push(ValidationError::new(
                    ValidationLocation::RelationAttribute {
                        model: self.name.clone(),
                        field: field.name.clone(),
                    },
                    "`@relation(fields: [...])` cannot be empty",
                ));
            }

            if relation.references.is_empty() {
                errors.push(ValidationError::new(
                    ValidationLocation::RelationAttribute {
                        model: self.name.clone(),
                        field: field.name.clone(),
                    },
                    "`@relation(references: [...])` cannot be empty",
                ));
            }

            if relation.fields.len() != relation.references.len() {
                errors.push(ValidationError::new(
                    ValidationLocation::RelationAttribute {
                        model: self.name.clone(),
                        field: field.name.clone(),
                    },
                    "`@relation(fields: [...], references: [...])` must declare the same number of local and referenced fields",
                ));
            }

            let mut local_seen = HashSet::new();
            for local in &relation.fields {
                if !local_seen.insert(local.as_str()) {
                    errors.push(ValidationError::new(
                        ValidationLocation::RelationField {
                            model: self.name.clone(),
                            field: field.name.clone(),
                            relation_field: local.clone(),
                        },
                        format!("duplicate relation field `{}`", local),
                    ));
                }

                match self.field_named(local) {
                    Some(local_field) => {
                        if !local_field.kind().is_scalar() {
                            errors.push(ValidationError::new(
                                ValidationLocation::RelationField {
                                    model: self.name.clone(),
                                    field: field.name.clone(),
                                    relation_field: local.clone(),
                                },
                                format!(
                                    "relation field list can only reference scalar fields, but `{}` is a relation field",
                                    local
                                ),
                            ));
                        }
                    }
                    None => errors.push(ValidationError::new(
                        ValidationLocation::RelationField {
                            model: self.name.clone(),
                            field: field.name.clone(),
                            relation_field: local.clone(),
                        },
                        format!(
                            "unknown local field `{}` referenced by relation field `{}`",
                            local, field.name
                        ),
                    )),
                }
            }

            let mut remote_seen = HashSet::new();
            for referenced in &relation.references {
                if !remote_seen.insert(referenced.as_str()) {
                    errors.push(ValidationError::new(
                        ValidationLocation::RelationReference {
                            model: self.name.clone(),
                            field: field.name.clone(),
                            referenced_field: referenced.clone(),
                            target_model: target_model.name.clone(),
                        },
                        format!("duplicate referenced field `{}`", referenced),
                    ));
                }

                match target_model.field_named(referenced) {
                    Some(target_field) => {
                        if !target_field.kind().is_scalar() {
                            errors.push(ValidationError::new(
                                ValidationLocation::RelationReference {
                                    model: self.name.clone(),
                                    field: field.name.clone(),
                                    referenced_field: referenced.clone(),
                                    target_model: target_model.name.clone(),
                                },
                                format!(
                                    "relation references can only target scalar fields, but `{}` on model `{}` is a relation field",
                                    referenced, target_model.name
                                ),
                            ));
                        }
                    }
                    None => errors.push(ValidationError::new(
                        ValidationLocation::RelationReference {
                            model: self.name.clone(),
                            field: field.name.clone(),
                            referenced_field: referenced.clone(),
                            target_model: target_model.name.clone(),
                        },
                        format!(
                            "unknown referenced field `{}` on model `{}`",
                            referenced, target_model.name
                        ),
                    )),
                }
            }
        }
    }

    pub fn field_named(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|field| field.name == name)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelBuilder {
    name: String,
    fields: Vec<Field>,
    attributes: Vec<ModelAttribute>,
}

impl ModelBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            fields: Vec::new(),
            attributes: Vec::new(),
        }
    }

    pub fn field(mut self, field: Field) -> Self {
        self.fields.push(field);
        self
    }

    pub fn fields(mut self, fields: Vec<Field>) -> Self {
        self.fields = fields;
        self
    }

    pub fn attribute(mut self, attribute: ModelAttribute) -> Self {
        self.attributes.push(attribute);
        self
    }

    pub fn attributes(mut self, attributes: Vec<ModelAttribute>) -> Self {
        self.attributes = attributes;
        self
    }

    pub fn build(self) -> Result<Model, ValidationErrors> {
        let model = Model {
            name: self.name,
            fields: self.fields,
            attributes: self.attributes,
        };

        let mut errors = Vec::new();
        model.validate_shallow(&mut errors);

        if errors.is_empty() {
            Ok(model)
        } else {
            Err(ValidationErrors::from(errors))
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Field {
    name: String,
    ty: FieldType,
    attributes: Vec<Attribute>,
}

impl Field {
    pub fn builder(name: impl Into<String>, ty: FieldType) -> FieldBuilder {
        FieldBuilder::new(name, ty)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn ty(&self) -> &FieldType {
        &self.ty
    }

    pub fn attributes(&self) -> &[Attribute] {
        &self.attributes
    }

    pub fn kind(&self) -> FieldKind {
        match &self.ty {
            FieldType::Scalar(_) => FieldKind::Scalar,
            FieldType::Relation { .. } => FieldKind::Relation,
        }
    }

    pub fn has_id(&self) -> bool {
        self.attributes
            .iter()
            .any(|attribute| matches!(attribute, Attribute::Id))
    }

    pub fn relation(&self) -> Option<&RelationAttribute> {
        self.attributes
            .iter()
            .find_map(|attribute| match attribute {
                Attribute::Relation(relation) => Some(relation),
                _ => None,
            })
    }

    pub fn rust_type(&self) -> Option<&RustTypeAttribute> {
        self.attributes
            .iter()
            .find_map(|attribute| match attribute {
                Attribute::RustType(rust_type) => Some(rust_type),
                _ => None,
            })
    }

    pub fn has_db_uuid(&self) -> bool {
        self.attributes
            .iter()
            .any(|attribute| matches!(attribute, Attribute::DbUuid))
    }

    fn validate_attributes(&self, model_name: &str, errors: &mut Vec<ValidationError>) {
        let mut seen_id = false;
        let mut seen_unique = false;
        let mut seen_index = false;
        let mut seen_default = false;
        let mut seen_relation = false;
        let mut seen_db_uuid = false;
        let mut seen_rust_type = false;

        for attribute in &self.attributes {
            match attribute {
                Attribute::Id => {
                    if seen_id {
                        errors.push(ValidationError::new(
                            ValidationLocation::Attribute {
                                model: model_name.to_owned(),
                                field: self.name.clone(),
                                attribute: "@id".to_owned(),
                            },
                            "duplicate `@id` attribute",
                        ));
                    } else {
                        seen_id = true;
                    }

                    if !self.kind().is_scalar() {
                        errors.push(ValidationError::new(
                            ValidationLocation::Attribute {
                                model: model_name.to_owned(),
                                field: self.name.clone(),
                                attribute: "@id".to_owned(),
                            },
                            "`@id` can only be used on scalar fields",
                        ));
                    }
                }
                Attribute::Unique => {
                    if seen_unique {
                        errors.push(ValidationError::new(
                            ValidationLocation::Attribute {
                                model: model_name.to_owned(),
                                field: self.name.clone(),
                                attribute: "@unique".to_owned(),
                            },
                            "duplicate `@unique` attribute",
                        ));
                    } else {
                        seen_unique = true;
                    }

                    if !self.kind().is_scalar() {
                        errors.push(ValidationError::new(
                            ValidationLocation::Attribute {
                                model: model_name.to_owned(),
                                field: self.name.clone(),
                                attribute: "@unique".to_owned(),
                            },
                            "`@unique` can only be used on scalar fields",
                        ));
                    }
                }
                Attribute::Index => {
                    if seen_index {
                        errors.push(ValidationError::new(
                            ValidationLocation::Attribute {
                                model: model_name.to_owned(),
                                field: self.name.clone(),
                                attribute: "@index".to_owned(),
                            },
                            "duplicate `@index` attribute",
                        ));
                    } else {
                        seen_index = true;
                    }

                    if !self.kind().is_scalar() {
                        errors.push(ValidationError::new(
                            ValidationLocation::Attribute {
                                model: model_name.to_owned(),
                                field: self.name.clone(),
                                attribute: "@index".to_owned(),
                            },
                            "`@index` can only be used on scalar fields",
                        ));
                    }
                }
                Attribute::Default(default) => {
                    if seen_default {
                        errors.push(ValidationError::new(
                            ValidationLocation::Attribute {
                                model: model_name.to_owned(),
                                field: self.name.clone(),
                                attribute: "@default".to_owned(),
                            },
                            "duplicate `@default` attribute",
                        ));
                    } else {
                        seen_default = true;
                    }

                    self.validate_default(model_name, default, errors);
                }
                Attribute::Relation(relation) => {
                    if seen_relation {
                        errors.push(ValidationError::new(
                            ValidationLocation::Attribute {
                                model: model_name.to_owned(),
                                field: self.name.clone(),
                                attribute: "@relation".to_owned(),
                            },
                            "duplicate `@relation` attribute",
                        ));
                    } else {
                        seen_relation = true;
                    }

                    if self.kind().is_scalar() {
                        errors.push(ValidationError::new(
                            ValidationLocation::Field {
                                model: model_name.to_owned(),
                                field: self.name.clone(),
                            },
                            format!("scalar field `{}` cannot declare `@relation`", self.name),
                        ));
                    }

                    if relation.fields.is_empty() || relation.references.is_empty() {
                        continue;
                    }
                }
                Attribute::DbUuid => {
                    if seen_db_uuid {
                        errors.push(ValidationError::new(
                            ValidationLocation::Attribute {
                                model: model_name.to_owned(),
                                field: self.name.clone(),
                                attribute: "@db.Uuid".to_owned(),
                            },
                            "duplicate `@db.Uuid` attribute",
                        ));
                    } else {
                        seen_db_uuid = true;
                    }

                    if self.kind().is_relation() {
                        errors.push(ValidationError::new(
                            ValidationLocation::Attribute {
                                model: model_name.to_owned(),
                                field: self.name.clone(),
                                attribute: "@db.Uuid".to_owned(),
                            },
                            "`@db.Uuid` can only be used on scalar fields",
                        ));
                    } else if !matches!(
                        &self.ty,
                        FieldType::Scalar(ScalarFieldType {
                            scalar: ScalarType::String,
                            ..
                        })
                    ) {
                        errors.push(ValidationError::new(
                            ValidationLocation::Attribute {
                                model: model_name.to_owned(),
                                field: self.name.clone(),
                                attribute: "@db.Uuid".to_owned(),
                            },
                            "`@db.Uuid` is only supported on `String` fields",
                        ));
                    } else if seen_rust_type {
                        errors.push(ValidationError::new(
                            ValidationLocation::Attribute {
                                model: model_name.to_owned(),
                                field: self.name.clone(),
                                attribute: "@db.Uuid".to_owned(),
                            },
                            "`@db.Uuid` cannot be combined with `@rust_ty`",
                        ));
                    }
                }
                Attribute::RustType(_) => {
                    if seen_rust_type {
                        errors.push(ValidationError::new(
                            ValidationLocation::Attribute {
                                model: model_name.to_owned(),
                                field: self.name.clone(),
                                attribute: "@rust_ty".to_owned(),
                            },
                            "duplicate `@rust_ty` attribute",
                        ));
                    } else {
                        seen_rust_type = true;
                    }

                    if self.kind().is_relation() {
                        errors.push(ValidationError::new(
                            ValidationLocation::Attribute {
                                model: model_name.to_owned(),
                                field: self.name.clone(),
                                attribute: "@rust_ty".to_owned(),
                            },
                            "`@rust_ty` can only be used on scalar fields",
                        ));
                    } else if !matches!(
                        &self.ty,
                        FieldType::Scalar(ScalarFieldType {
                            scalar: ScalarType::String,
                            ..
                        })
                    ) {
                        errors.push(ValidationError::new(
                            ValidationLocation::Attribute {
                                model: model_name.to_owned(),
                                field: self.name.clone(),
                                attribute: "@rust_ty".to_owned(),
                            },
                            "`@rust_ty` is only supported on `String` fields",
                        ));
                    } else if seen_db_uuid {
                        errors.push(ValidationError::new(
                            ValidationLocation::Attribute {
                                model: model_name.to_owned(),
                                field: self.name.clone(),
                                attribute: "@rust_ty".to_owned(),
                            },
                            "`@rust_ty` cannot be combined with `@db.Uuid`",
                        ));
                    }
                }
            }
        }
    }

    fn validate_type(&self, schema: &Schema, model_name: &str, errors: &mut Vec<ValidationError>) {
        let FieldType::Relation { model, .. } = &self.ty else {
            return;
        };

        match schema.resolve_model(model) {
            Resolution::Found(_) => {}
            Resolution::NotFound => errors.push(ValidationError::new(
                ValidationLocation::FieldType {
                    model: model_name.to_owned(),
                    field: self.name.clone(),
                    ty: model.clone(),
                },
                format!(
                    "unknown relation target model `{}` for field `{}`",
                    model, self.name
                ),
            )),
            Resolution::Ambiguous(models) => {
                let candidates = models
                    .into_iter()
                    .map(|candidate| format!("`{}`", candidate.name))
                    .collect::<Vec<_>>()
                    .join(", ");

                errors.push(ValidationError::new(
                    ValidationLocation::FieldType {
                        model: model_name.to_owned(),
                        field: self.name.clone(),
                        ty: model.clone(),
                    },
                    format!(
                        "ambiguous relation target `{}` for field `{}`; matches {}",
                        model, self.name, candidates
                    ),
                ));
            }
        }
    }

    fn validate_default(
        &self,
        model_name: &str,
        default: &DefaultAttribute,
        errors: &mut Vec<ValidationError>,
    ) {
        if self.kind().is_relation() {
            errors.push(ValidationError::new(
                ValidationLocation::Attribute {
                    model: model_name.to_owned(),
                    field: self.name.clone(),
                    attribute: "@default".to_owned(),
                },
                "`@default` can only be used on scalar fields",
            ));
            return;
        }

        match default.function {
            DefaultFunction::Autoincrement => {
                if self.ty != FieldType::int() && self.ty != FieldType::big_int() {
                    errors.push(ValidationError::new(
                        ValidationLocation::Attribute {
                            model: model_name.to_owned(),
                            field: self.name.clone(),
                            attribute: "@default".to_owned(),
                        },
                        "`@default(autoincrement())` is only supported on `Int` and `BigInt` fields",
                    ));
                }
            }
            DefaultFunction::Now => {
                if self.ty != FieldType::date_time() {
                    errors.push(ValidationError::new(
                        ValidationLocation::Attribute {
                            model: model_name.to_owned(),
                            field: self.name.clone(),
                            attribute: "@default".to_owned(),
                        },
                        "`@default(now())` is only supported on `DateTime` fields",
                    ));
                }
            }
            DefaultFunction::Other(ref other) => errors.push(ValidationError::new(
                ValidationLocation::Attribute {
                    model: model_name.to_owned(),
                    field: self.name.clone(),
                    attribute: "@default".to_owned(),
                },
                format!(
                    "unsupported default function `{}`; expected `autoincrement` or `now`",
                    other
                ),
            )),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FieldBuilder {
    name: String,
    ty: FieldType,
    attributes: Vec<Attribute>,
}

impl FieldBuilder {
    pub fn new(name: impl Into<String>, ty: FieldType) -> Self {
        Self {
            name: name.into(),
            ty,
            attributes: Vec::new(),
        }
    }

    pub fn attribute(mut self, attribute: Attribute) -> Self {
        self.attributes.push(attribute);
        self
    }

    pub fn attributes(mut self, attributes: Vec<Attribute>) -> Self {
        self.attributes = attributes;
        self
    }

    pub fn build(self) -> Result<Field, ValidationErrors> {
        self.build_for_model("<field>")
    }

    pub fn build_for_model(self, model_name: &str) -> Result<Field, ValidationErrors> {
        let field = Field {
            name: self.name,
            ty: self.ty,
            attributes: self.attributes,
        };

        let mut errors = Vec::new();
        field.validate_attributes(model_name, &mut errors);

        if errors.is_empty() {
            Ok(field)
        } else {
            Err(ValidationErrors::from(errors))
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FieldType {
    Scalar(ScalarFieldType),
    Relation {
        model: String,
        optional: bool,
        many: bool,
    },
}

impl FieldType {
    pub fn scalar(scalar: ScalarType, optional: bool) -> Self {
        Self::Scalar(ScalarFieldType { scalar, optional })
    }

    pub fn relation(model: impl Into<String>, optional: bool, many: bool) -> Self {
        Self::Relation {
            model: model.into(),
            optional,
            many,
        }
    }

    pub fn relation_many(model: impl Into<String>) -> Self {
        Self::relation(model, false, true)
    }

    pub fn int() -> Self {
        Self::scalar(ScalarType::Int, false)
    }

    pub fn big_int() -> Self {
        Self::scalar(ScalarType::BigInt, false)
    }

    pub fn string() -> Self {
        Self::scalar(ScalarType::String, false)
    }

    pub fn date_time() -> Self {
        Self::scalar(ScalarType::DateTime, false)
    }

    pub fn is_optional(&self) -> bool {
        match self {
            FieldType::Scalar(scalar) => scalar.optional,
            FieldType::Relation { optional, .. } => *optional,
        }
    }

    pub fn is_many(&self) -> bool {
        match self {
            FieldType::Scalar(_) => false,
            FieldType::Relation { many, .. } => *many,
        }
    }

    pub fn name(&self) -> &str {
        match self {
            FieldType::Scalar(scalar) => scalar.scalar.as_str(),
            FieldType::Relation { model, .. } => model.as_str(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalarFieldType {
    scalar: ScalarType,
    optional: bool,
}

impl ScalarFieldType {
    pub fn scalar(&self) -> ScalarType {
        self.scalar
    }

    pub fn optional(&self) -> bool {
        self.optional
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScalarType {
    Int,
    BigInt,
    String,
    Boolean,
    DateTime,
    Float,
    Decimal,
    Bytes,
    Json,
}

impl ScalarType {
    pub fn as_str(self) -> &'static str {
        match self {
            ScalarType::Int => "Int",
            ScalarType::BigInt => "BigInt",
            ScalarType::String => "String",
            ScalarType::Boolean => "Boolean",
            ScalarType::DateTime => "DateTime",
            ScalarType::Float => "Float",
            ScalarType::Decimal => "Decimal",
            ScalarType::Bytes => "Bytes",
            ScalarType::Json => "Json",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Attribute {
    Id,
    Unique,
    Index,
    Default(DefaultAttribute),
    Relation(RelationAttribute),
    DbUuid,
    RustType(RustTypeAttribute),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RustTypeAttribute {
    path: String,
}

impl RustTypeAttribute {
    pub fn new(path: impl Into<String>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ModelAttribute {
    Id(ModelPrimaryKeyAttribute),
    Unique(ModelUniqueAttribute),
    Index(ModelIndexAttribute),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ModelPrimaryKeyAttribute {
    fields: Vec<String>,
}

impl ModelPrimaryKeyAttribute {
    pub fn builder() -> ModelPrimaryKeyAttributeBuilder {
        ModelPrimaryKeyAttributeBuilder::new()
    }

    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    fn validate(&self, model: &Model, errors: &mut Vec<ValidationError>) {
        if self.fields.is_empty() {
            errors.push(ValidationError::new(
                ValidationLocation::ModelAttribute {
                    model: model.name.clone(),
                    attribute: "@@id".to_owned(),
                },
                "`@@id([...])` cannot be empty",
            ));
            return;
        }

        let mut seen = HashSet::new();
        for field_name in &self.fields {
            if !seen.insert(field_name.as_str()) {
                errors.push(ValidationError::new(
                    ValidationLocation::ModelPrimaryKeyField {
                        model: model.name.clone(),
                        field: field_name.clone(),
                    },
                    format!("duplicate primary key field `{}`", field_name),
                ));
                continue;
            }

            match model.field_named(field_name) {
                Some(field) => match field.ty() {
                    FieldType::Scalar(scalar) => {
                        if scalar.optional() {
                            errors.push(ValidationError::new(
                                ValidationLocation::ModelPrimaryKeyField {
                                    model: model.name.clone(),
                                    field: field_name.clone(),
                                },
                                format!("primary key field `{}` must not be optional", field_name),
                            ));
                        }
                    }
                    FieldType::Relation { .. } => errors.push(ValidationError::new(
                        ValidationLocation::ModelPrimaryKeyField {
                            model: model.name.clone(),
                            field: field_name.clone(),
                        },
                        format!("primary key field `{}` must be scalar", field_name),
                    )),
                },
                None => errors.push(ValidationError::new(
                    ValidationLocation::ModelPrimaryKeyField {
                        model: model.name.clone(),
                        field: field_name.clone(),
                    },
                    format!("unknown primary key field `{}`", field_name),
                )),
            }
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ModelPrimaryKeyAttributeBuilder {
    fields: Vec<String>,
}

impl ModelPrimaryKeyAttributeBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.fields.push(field.into());
        self
    }

    pub fn fields(mut self, fields: Vec<String>) -> Self {
        self.fields = fields;
        self
    }

    pub fn build(self) -> Result<ModelPrimaryKeyAttribute, ValidationErrors> {
        let attribute = ModelPrimaryKeyAttribute {
            fields: self.fields,
        };

        if attribute.fields.is_empty() {
            Err(ValidationErrors::from(vec![ValidationError::new(
                ValidationLocation::ModelAttribute {
                    model: "<model>".to_owned(),
                    attribute: "@@id".to_owned(),
                },
                "`@@id([...])` cannot be empty",
            )]))
        } else {
            Ok(attribute)
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ModelUniqueAttribute {
    fields: Vec<String>,
}

impl ModelUniqueAttribute {
    pub fn builder() -> ModelUniqueAttributeBuilder {
        ModelUniqueAttributeBuilder::new()
    }

    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    fn validate(&self, model: &Model, errors: &mut Vec<ValidationError>) {
        if self.fields.is_empty() {
            errors.push(ValidationError::new(
                ValidationLocation::ModelAttribute {
                    model: model.name.clone(),
                    attribute: "@@unique".to_owned(),
                },
                "`@@unique([...])` cannot be empty",
            ));
            return;
        }

        let mut seen = HashSet::new();
        for field_name in &self.fields {
            if !seen.insert(field_name.as_str()) {
                errors.push(ValidationError::new(
                    ValidationLocation::ModelUniqueField {
                        model: model.name.clone(),
                        field: field_name.clone(),
                    },
                    format!("duplicate unique field `{}`", field_name),
                ));
                continue;
            }

            match model.field_named(field_name) {
                Some(field) => {
                    if matches!(field.ty(), FieldType::Relation { .. }) {
                        errors.push(ValidationError::new(
                            ValidationLocation::ModelUniqueField {
                                model: model.name.clone(),
                                field: field_name.clone(),
                            },
                            format!("unique field `{}` must be scalar", field_name),
                        ));
                    }
                }
                None => errors.push(ValidationError::new(
                    ValidationLocation::ModelUniqueField {
                        model: model.name.clone(),
                        field: field_name.clone(),
                    },
                    format!("unknown unique field `{}`", field_name),
                )),
            }
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ModelUniqueAttributeBuilder {
    fields: Vec<String>,
}

impl ModelUniqueAttributeBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.fields.push(field.into());
        self
    }

    pub fn fields(mut self, fields: Vec<String>) -> Self {
        self.fields = fields;
        self
    }

    pub fn build(self) -> Result<ModelUniqueAttribute, ValidationErrors> {
        let attribute = ModelUniqueAttribute {
            fields: self.fields,
        };

        if attribute.fields.is_empty() {
            Err(ValidationErrors::from(vec![ValidationError::new(
                ValidationLocation::ModelAttribute {
                    model: "<model>".to_owned(),
                    attribute: "@@unique".to_owned(),
                },
                "`@@unique([...])` cannot be empty",
            )]))
        } else {
            Ok(attribute)
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ModelIndexAttribute {
    fields: Vec<String>,
}

impl ModelIndexAttribute {
    pub fn builder() -> ModelIndexAttributeBuilder {
        ModelIndexAttributeBuilder::new()
    }

    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    fn validate(&self, model: &Model, errors: &mut Vec<ValidationError>) {
        if self.fields.is_empty() {
            errors.push(ValidationError::new(
                ValidationLocation::ModelAttribute {
                    model: model.name.clone(),
                    attribute: "@@index".to_owned(),
                },
                "`@@index([...])` cannot be empty",
            ));
            return;
        }

        let mut seen = HashSet::new();
        for field_name in &self.fields {
            if !seen.insert(field_name.as_str()) {
                errors.push(ValidationError::new(
                    ValidationLocation::ModelIndexField {
                        model: model.name.clone(),
                        field: field_name.clone(),
                    },
                    format!("duplicate index field `{}`", field_name),
                ));
                continue;
            }

            match model.field_named(field_name) {
                Some(field) => {
                    if matches!(field.ty(), FieldType::Relation { .. }) {
                        errors.push(ValidationError::new(
                            ValidationLocation::ModelIndexField {
                                model: model.name.clone(),
                                field: field_name.clone(),
                            },
                            format!("index field `{}` must be scalar", field_name),
                        ));
                    }
                }
                None => errors.push(ValidationError::new(
                    ValidationLocation::ModelIndexField {
                        model: model.name.clone(),
                        field: field_name.clone(),
                    },
                    format!("unknown index field `{}`", field_name),
                )),
            }
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ModelIndexAttributeBuilder {
    fields: Vec<String>,
}

impl ModelIndexAttributeBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.fields.push(field.into());
        self
    }

    pub fn fields(mut self, fields: Vec<String>) -> Self {
        self.fields = fields;
        self
    }

    pub fn build(self) -> Result<ModelIndexAttribute, ValidationErrors> {
        let attribute = ModelIndexAttribute {
            fields: self.fields,
        };

        if attribute.fields.is_empty() {
            Err(ValidationErrors::from(vec![ValidationError::new(
                ValidationLocation::ModelAttribute {
                    model: "<model>".to_owned(),
                    attribute: "@@index".to_owned(),
                },
                "`@@index([...])` cannot be empty",
            )]))
        } else {
            Ok(attribute)
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DefaultAttribute {
    function: DefaultFunction,
}

impl DefaultAttribute {
    pub fn new(function: DefaultFunction) -> Self {
        Self { function }
    }

    pub fn autoincrement() -> Self {
        Self::new(DefaultFunction::Autoincrement)
    }

    pub fn now() -> Self {
        Self::new(DefaultFunction::Now)
    }

    pub fn function(&self) -> &DefaultFunction {
        &self.function
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DefaultFunction {
    Autoincrement,
    Now,
    Other(String),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RelationAttribute {
    fields: Vec<String>,
    references: Vec<String>,
}

impl RelationAttribute {
    pub fn builder() -> RelationAttributeBuilder {
        RelationAttributeBuilder::new()
    }

    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    pub fn references(&self) -> &[String] {
        &self.references
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RelationAttributeBuilder {
    fields: Vec<String>,
    references: Vec<String>,
}

impl RelationAttributeBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.fields.push(field.into());
        self
    }

    pub fn fields(mut self, fields: Vec<String>) -> Self {
        self.fields = fields;
        self
    }

    pub fn reference(mut self, reference: impl Into<String>) -> Self {
        self.references.push(reference.into());
        self
    }

    pub fn references(mut self, references: Vec<String>) -> Self {
        self.references = references;
        self
    }

    pub fn build(self) -> Result<RelationAttribute, ValidationErrors> {
        self.build_for_field("<field>", "<field>")
    }

    pub fn build_for_field(
        self,
        model_name: &str,
        field_name: &str,
    ) -> Result<RelationAttribute, ValidationErrors> {
        let relation = RelationAttribute {
            fields: self.fields,
            references: self.references,
        };

        let mut errors = Vec::new();

        if relation.fields.is_empty() {
            errors.push(ValidationError::new(
                ValidationLocation::RelationAttribute {
                    model: model_name.to_owned(),
                    field: field_name.to_owned(),
                },
                "`@relation(fields: [...])` cannot be empty",
            ));
        }

        if relation.references.is_empty() {
            errors.push(ValidationError::new(
                ValidationLocation::RelationAttribute {
                    model: model_name.to_owned(),
                    field: field_name.to_owned(),
                },
                "`@relation(references: [...])` cannot be empty",
            ));
        }

        if errors.is_empty() {
            Ok(relation)
        } else {
            Err(ValidationErrors::from(errors))
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FieldKind {
    Scalar,
    Relation,
}

impl FieldKind {
    pub fn is_scalar(self) -> bool {
        matches!(self, Self::Scalar)
    }

    pub fn is_relation(self) -> bool {
        matches!(self, Self::Relation)
    }
}

pub enum Resolution<'a> {
    Found(&'a Model),
    NotFound,
    Ambiguous(Vec<&'a Model>),
}
