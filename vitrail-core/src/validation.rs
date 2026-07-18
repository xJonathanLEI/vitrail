use std::error::Error;
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValidationLocation {
    Schema,
    ExternalTable {
        table: String,
    },
    Model {
        model: String,
    },
    ModelAttribute {
        model: String,
        attribute: String,
    },
    ModelPrimaryKeyField {
        model: String,
        field: String,
    },
    ModelUniqueField {
        model: String,
        field: String,
    },
    ModelIndexField {
        model: String,
        field: String,
    },
    Field {
        model: String,
        field: String,
    },
    FieldType {
        model: String,
        field: String,
        ty: String,
    },
    Attribute {
        model: String,
        field: String,
        attribute: String,
    },
    RelationAttribute {
        model: String,
        field: String,
    },
    RelationField {
        model: String,
        field: String,
        relation_field: String,
    },
    RelationReference {
        model: String,
        field: String,
        referenced_field: String,
        target_model: String,
    },
}

impl fmt::Display for ValidationLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidationLocation::Schema => write!(f, "schema"),
            ValidationLocation::ExternalTable { table } => {
                write!(f, "external table `{}`", table)
            }
            ValidationLocation::Model { model } => write!(f, "model `{}`", model),
            ValidationLocation::ModelAttribute { model, attribute } => {
                write!(f, "attribute `{}` on model `{}`", attribute, model)
            }
            ValidationLocation::ModelPrimaryKeyField { model, field } => {
                write!(f, "primary key field `{}` on model `{}`", field, model)
            }
            ValidationLocation::ModelUniqueField { model, field } => {
                write!(f, "unique field `{}` on model `{}`", field, model)
            }
            ValidationLocation::ModelIndexField { model, field } => {
                write!(f, "index field `{}` on model `{}`", field, model)
            }
            ValidationLocation::Field { model, field } => {
                write!(f, "field `{}.{}`", model, field)
            }
            ValidationLocation::FieldType { model, field, ty } => {
                write!(f, "type `{}` for field `{}.{}`", ty, model, field)
            }
            ValidationLocation::Attribute {
                model,
                field,
                attribute,
            } => write!(
                f,
                "attribute `{}` on field `{}.{}`",
                attribute, model, field
            ),
            ValidationLocation::RelationAttribute { model, field } => {
                write!(f, "relation metadata for field `{}.{}`", model, field)
            }
            ValidationLocation::RelationField {
                model,
                field,
                relation_field,
            } => write!(
                f,
                "relation field `{}` in `{}.{}`",
                relation_field, model, field
            ),
            ValidationLocation::RelationReference {
                model,
                field,
                referenced_field,
                target_model,
            } => write!(
                f,
                "relation reference `{} -> {}.{}` for field `{}.{}`",
                referenced_field, target_model, referenced_field, model, field
            ),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationError {
    pub location: ValidationLocation,
    pub message: String,
}

impl ValidationError {
    pub fn new(location: ValidationLocation, message: impl Into<String>) -> Self {
        Self {
            location,
            message: message.into(),
        }
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.location, self.message)
    }
}

impl Error for ValidationError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationErrors {
    errors: Vec<ValidationError>,
}

impl Default for ValidationErrors {
    fn default() -> Self {
        Self::new()
    }
}

impl ValidationErrors {
    pub fn new() -> Self {
        Self { errors: Vec::new() }
    }

    pub fn push(&mut self, error: ValidationError) {
        self.errors.push(error);
    }

    pub fn is_empty(&self) -> bool {
        self.errors.is_empty()
    }

    pub fn len(&self) -> usize {
        self.errors.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &ValidationError> {
        self.errors.iter()
    }

    pub fn into_vec(self) -> Vec<ValidationError> {
        self.errors
    }
}

impl From<Vec<ValidationError>> for ValidationErrors {
    fn from(errors: Vec<ValidationError>) -> Self {
        Self { errors }
    }
}

impl AsRef<[ValidationError]> for ValidationErrors {
    fn as_ref(&self) -> &[ValidationError] {
        &self.errors
    }
}

impl fmt::Display for ValidationErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.errors.as_slice() {
            [] => write!(f, "no validation errors"),
            [only] => write!(f, "{only}"),
            many => {
                writeln!(f, "{} validation errors:", many.len())?;
                for error in many {
                    writeln!(f, "- {error}")?;
                }
                Ok(())
            }
        }
    }
}

impl Error for ValidationErrors {}
