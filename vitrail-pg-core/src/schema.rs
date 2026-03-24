use std::collections::{HashMap, HashSet};

use heck::ToUpperCamelCase;

use crate::validation::{ValidationError, ValidationErrors, ValidationLocation};

/// Schema definition for `vitrail-pg`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Schema {
    models: Vec<Model>,
}

impl Schema {
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::new()
    }

    pub fn models(&self) -> &[Model] {
        &self.models
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

    pub fn build(self) -> Result<Schema, ValidationErrors> {
        let schema = Schema {
            models: self.models,
        };
        schema.validate()?;
        Ok(schema)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Model {
    name: String,
    fields: Vec<Field>,
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

        match id_fields.len() {
            0 => errors.push(ValidationError::new(
                ValidationLocation::Model {
                    model: self.name.clone(),
                },
                format!("model `{}` must declare exactly one `@id` field", self.name),
            )),
            1 => {}
            _ => {
                errors.push(ValidationError::new(
                    ValidationLocation::Model {
                        model: self.name.clone(),
                    },
                    format!(
                        "model `{}` declares multiple `@id` fields; compound ids are not supported yet",
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
}

impl ModelBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            fields: Vec::new(),
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

    pub fn build(self) -> Result<Model, ValidationErrors> {
        let model = Model {
            name: self.name,
            fields: self.fields,
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

    fn validate_attributes(&self, model_name: &str, errors: &mut Vec<ValidationError>) {
        let mut seen_id = false;
        let mut seen_unique = false;
        let mut seen_default = false;
        let mut seen_relation = false;
        let mut seen_db_uuid = false;

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
                    } else if self.ty != FieldType::string() {
                        errors.push(ValidationError::new(
                            ValidationLocation::Attribute {
                                model: model_name.to_owned(),
                                field: self.name.clone(),
                                attribute: "@db.Uuid".to_owned(),
                            },
                            "`@db.Uuid` is only supported on `String` fields",
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
                if self.ty != FieldType::int() {
                    errors.push(ValidationError::new(
                        ValidationLocation::Attribute {
                            model: model_name.to_owned(),
                            field: self.name.clone(),
                            attribute: "@default".to_owned(),
                        },
                        "`@default(autoincrement())` is only supported on `Int` fields",
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
    Default(DefaultAttribute),
    Relation(RelationAttribute),
    DbUuid,
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
