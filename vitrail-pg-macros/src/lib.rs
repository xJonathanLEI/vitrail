use proc_macro::TokenStream;
use proc_macro2::Span;
use syn::ext::IdentExt;
use syn::parse::{Parse, ParseStream};
use syn::{Error, Ident, Result, Token, bracketed, parenthesized};
use vitrail_pg_core as core;

mod kw {
    syn::custom_keyword!(model);
}

/// Validates a schema DSL declaration at compile time.
#[proc_macro]
pub fn schema(input: TokenStream) -> TokenStream {
    let schema = syn::parse_macro_input!(input as ParsedSchema);

    match schema.validate() {
        Ok(()) => TokenStream::new(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Parsed top-level schema definition plus enough source metadata to translate
/// clean core validation errors back into compiler diagnostics with spans.
#[derive(Debug)]
struct ParsedSchema {
    models: Vec<ParsedModel>,
}

impl Parse for ParsedSchema {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut models = Vec::new();

        while !input.is_empty() {
            models.push(input.parse()?);
        }

        Ok(Self { models })
    }
}

impl ParsedSchema {
    fn validate(&self) -> Result<()> {
        match self.to_core() {
            Ok(_) => Ok(()),
            Err(errors) => {
                let mut combined = None;

                for validation_error in errors.iter() {
                    push_error(
                        &mut combined,
                        Error::new(
                            self.span_for_validation_error(validation_error),
                            validation_error.message.clone(),
                        ),
                    );
                }

                Err(combined.expect("validation should emit at least one error"))
            }
        }
    }

    fn to_core(&self) -> std::result::Result<core::Schema, core::ValidationErrors> {
        let mut models = Vec::with_capacity(self.models.len());

        for model in &self.models {
            models.push(model.to_core()?);
        }

        core::Schema::builder().models(models).build()
    }

    fn span_for_validation_error(&self, error: &core::ValidationError) -> Span {
        let message = error.message.as_str();

        match &error.location {
            core::ValidationLocation::Schema => Span::call_site(),
            core::ValidationLocation::Model { model } => {
                let prefer_first = message == "first declaration of this model";
                self.model_span(model, prefer_first)
            }
            core::ValidationLocation::Field { model, field } => {
                let prefer_first = message == "first declaration of this field";
                self.field_span(model, field, prefer_first)
            }
            core::ValidationLocation::FieldType { model, field, .. } => self
                .field(model, field, false)
                .map(|field| field.ty.name.span())
                .unwrap_or_else(Span::call_site),
            core::ValidationLocation::Attribute {
                model,
                field,
                attribute,
            } => self
                .field(model, field, false)
                .and_then(|field| field.attribute_span(attribute, false))
                .unwrap_or_else(|| self.field_span(model, field, false)),
            core::ValidationLocation::RelationAttribute { model, field } => self
                .field(model, field, false)
                .and_then(|field| field.relation())
                .map(|relation| relation.span)
                .unwrap_or_else(|| self.field_span(model, field, false)),
            core::ValidationLocation::RelationField {
                model,
                field,
                relation_field,
            } => {
                let prefer_last = message.starts_with("duplicate relation field ");
                self.field(model, field, false)
                    .and_then(|field| field.relation())
                    .and_then(|relation| relation.field_span(relation_field, !prefer_last))
                    .unwrap_or_else(|| self.field_span(model, field, false))
            }
            core::ValidationLocation::RelationReference {
                model,
                field,
                referenced_field,
                ..
            } => {
                let prefer_last = message.starts_with("duplicate referenced field ");
                self.field(model, field, false)
                    .and_then(|field| field.relation())
                    .and_then(|relation| relation.reference_span(referenced_field, !prefer_last))
                    .unwrap_or_else(|| self.field_span(model, field, false))
            }
        }
    }

    fn model(&self, name: &str, prefer_first: bool) -> Option<&ParsedModel> {
        let mut matches = self.models.iter().filter(|model| model.name == name);
        if prefer_first {
            matches.next()
        } else {
            self.models.iter().rev().find(|model| model.name == name)
        }
    }

    fn model_span(&self, name: &str, prefer_first: bool) -> Span {
        self.model(name, prefer_first)
            .map(|model| model.name.span())
            .unwrap_or_else(Span::call_site)
    }

    fn field(
        &self,
        model_name: &str,
        field_name: &str,
        prefer_first: bool,
    ) -> Option<&ParsedField> {
        let model = self.model(model_name, prefer_first)?;
        let mut matches = model.fields.iter().filter(|field| field.name == field_name);

        if prefer_first {
            matches.next()
        } else {
            model
                .fields
                .iter()
                .rev()
                .find(|field| field.name == field_name)
        }
    }

    fn field_span(&self, model_name: &str, field_name: &str, prefer_first: bool) -> Span {
        self.field(model_name, field_name, prefer_first)
            .map(|field| field.name.span())
            .unwrap_or_else(|| self.model_span(model_name, prefer_first))
    }
}

/// Parsed model declaration.
#[derive(Debug)]
struct ParsedModel {
    name: Ident,
    fields: Vec<ParsedField>,
}

impl Parse for ParsedModel {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        input.parse::<kw::model>()?;
        let name = input.call(Ident::parse_any)?;

        let content;
        syn::braced!(content in input);

        let mut fields = Vec::new();
        while !content.is_empty() {
            fields.push(content.parse()?);
        }

        Ok(Self { name, fields })
    }
}

impl ParsedModel {
    fn to_core(&self) -> std::result::Result<core::Model, core::ValidationErrors> {
        let mut fields = Vec::with_capacity(self.fields.len());

        for field in &self.fields {
            fields.push(field.to_core(&self.name.to_string())?);
        }

        core::Model::builder(self.name.to_string())
            .fields(fields)
            .build()
    }
}

/// Parsed field declaration within a model.
#[derive(Debug)]
struct ParsedField {
    name: Ident,
    ty: ParsedFieldType,
    attributes: Vec<ParsedAttribute>,
}

impl Parse for ParsedField {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let name = input.call(Ident::parse_any)?;
        let ty = input.parse()?;

        let mut attributes = Vec::new();
        while input.peek(Token![@]) {
            attributes.push(input.parse()?);
        }

        Ok(Self {
            name,
            ty,
            attributes,
        })
    }
}

impl ParsedField {
    fn to_core(
        &self,
        model_name: &str,
    ) -> std::result::Result<core::Field, core::ValidationErrors> {
        let mut attributes = Vec::with_capacity(self.attributes.len());

        for attribute in &self.attributes {
            attributes.push(attribute.to_core(model_name, &self.name.to_string())?);
        }

        core::Field::builder(self.name.to_string(), self.ty.to_core())
            .attributes(attributes)
            .build_for_model(model_name)
    }

    fn relation(&self) -> Option<&ParsedRelationAttribute> {
        self.attributes
            .iter()
            .find_map(|attribute| match &attribute.kind {
                ParsedAttributeKind::Relation(relation) => Some(relation),
                _ => None,
            })
    }

    fn attribute_span(&self, attribute: &str, prefer_first: bool) -> Option<Span> {
        let mut matches = self
            .attributes
            .iter()
            .filter(|candidate| candidate.name() == attribute);

        if prefer_first {
            matches.next().map(|attribute| attribute.span)
        } else {
            self.attributes
                .iter()
                .rev()
                .find(|candidate| candidate.name() == attribute)
                .map(|attribute| attribute.span)
        }
    }
}

/// Parsed field type, including optionality.
#[derive(Debug)]
struct ParsedFieldType {
    name: Ident,
    optional: bool,
}

impl Parse for ParsedFieldType {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let name = input.call(Ident::parse_any)?;
        let optional = if input.peek(Token![?]) {
            input.parse::<Token![?]>()?;
            true
        } else {
            false
        };

        Ok(Self { name, optional })
    }
}

impl ParsedFieldType {
    fn to_core(&self) -> core::FieldType {
        match scalar_type_from_ident(&self.name) {
            Some(scalar) => core::FieldType::scalar(scalar, self.optional),
            None => core::FieldType::relation(self.name.to_string(), self.optional),
        }
    }
}

/// Parsed field attribute with its source span.
#[derive(Debug)]
struct ParsedAttribute {
    kind: ParsedAttributeKind,
    span: Span,
}

impl Parse for ParsedAttribute {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        input.parse::<Token![@]>()?;
        let first = input.call(Ident::parse_any)?;
        let span = first.span();

        let kind = if input.peek(Token![.]) {
            input.parse::<Token![.]>()?;
            let second = input.call(Ident::parse_any)?;

            match (first.to_string().as_str(), second.to_string().as_str()) {
                ("db", "Uuid") => ParsedAttributeKind::DbUuid,
                _ => {
                    return Err(Error::new(
                        second.span(),
                        format!("unknown attribute `@{}.{}`", first, second),
                    ));
                }
            }
        } else {
            match first.to_string().as_str() {
                "id" => ParsedAttributeKind::Id,
                "unique" => ParsedAttributeKind::Unique,
                "default" => ParsedAttributeKind::Default(input.parse()?),
                "relation" => ParsedAttributeKind::Relation(input.parse()?),
                _ => {
                    return Err(Error::new(
                        first.span(),
                        format!("unknown attribute `@{}`", first),
                    ));
                }
            }
        };

        Ok(Self { kind, span })
    }
}

impl ParsedAttribute {
    fn to_core(
        &self,
        model_name: &str,
        field_name: &str,
    ) -> std::result::Result<core::Attribute, core::ValidationErrors> {
        match &self.kind {
            ParsedAttributeKind::Id => Ok(core::Attribute::Id),
            ParsedAttributeKind::Unique => Ok(core::Attribute::Unique),
            ParsedAttributeKind::Default(default) => {
                Ok(core::Attribute::Default(default.to_core()))
            }
            ParsedAttributeKind::Relation(relation) => Ok(core::Attribute::Relation(
                relation.to_core(model_name, field_name)?,
            )),
            ParsedAttributeKind::DbUuid => Ok(core::Attribute::DbUuid),
        }
    }

    fn name(&self) -> &'static str {
        match self.kind {
            ParsedAttributeKind::Id => "@id",
            ParsedAttributeKind::Unique => "@unique",
            ParsedAttributeKind::Default(_) => "@default",
            ParsedAttributeKind::Relation(_) => "@relation",
            ParsedAttributeKind::DbUuid => "@db.Uuid",
        }
    }
}

/// Supported field attributes in the schema DSL.
#[derive(Debug)]
enum ParsedAttributeKind {
    Id,
    Unique,
    Default(ParsedDefaultAttribute),
    Relation(ParsedRelationAttribute),
    DbUuid,
}

/// Parsed `@default(...)` attribute payload.
#[derive(Debug)]
struct ParsedDefaultAttribute {
    function: Ident,
}

impl Parse for ParsedDefaultAttribute {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let content;
        parenthesized!(content in input);

        let function = content.call(Ident::parse_any)?;
        let args;
        parenthesized!(args in content);

        if !args.is_empty() {
            return Err(Error::new(
                args.span(),
                "default functions do not take any arguments yet",
            ));
        }

        if !content.is_empty() {
            return Err(Error::new(
                content.span(),
                "unexpected tokens in `@default(...)`",
            ));
        }

        Ok(Self { function })
    }
}

impl ParsedDefaultAttribute {
    fn to_core(&self) -> core::DefaultAttribute {
        core::DefaultAttribute::new(match self.function.to_string().as_str() {
            "autoincrement" => core::DefaultFunction::Autoincrement,
            "now" => core::DefaultFunction::Now,
            other => core::DefaultFunction::Other(other.to_owned()),
        })
    }
}

/// Parsed `@relation(...)` attribute payload.
#[derive(Debug)]
struct ParsedRelationAttribute {
    fields: Vec<Ident>,
    references: Vec<Ident>,
    span: Span,
}

impl Parse for ParsedRelationAttribute {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let content;
        parenthesized!(content in input);

        let span = content.span();
        let mut fields = None;
        let mut references = None;

        while !content.is_empty() {
            let key = content.call(Ident::parse_any)?;
            content.parse::<Token![:]>()?;

            match key.to_string().as_str() {
                "fields" => {
                    if fields.is_some() {
                        return Err(Error::new(key.span(), "duplicate `fields` argument"));
                    }
                    fields = Some(parse_ident_list(&content)?);
                }
                "references" => {
                    if references.is_some() {
                        return Err(Error::new(key.span(), "duplicate `references` argument"));
                    }
                    references = Some(parse_ident_list(&content)?);
                }
                _ => {
                    return Err(Error::new(
                        key.span(),
                        "unknown relation argument; expected `fields` or `references`",
                    ));
                }
            }

            if content.peek(Token![,]) {
                content.parse::<Token![,]>()?;
            }
        }

        let fields = fields.ok_or_else(|| {
            Error::new(span, "`@relation(...)` requires a `fields: [...]` argument")
        })?;
        let references = references.ok_or_else(|| {
            Error::new(
                span,
                "`@relation(...)` requires a `references: [...]` argument",
            )
        })?;

        Ok(Self {
            fields,
            references,
            span,
        })
    }
}

impl ParsedRelationAttribute {
    fn to_core(
        &self,
        model_name: &str,
        field_name: &str,
    ) -> std::result::Result<core::RelationAttribute, core::ValidationErrors> {
        core::RelationAttribute::builder()
            .fields(self.fields.iter().map(ToString::to_string).collect())
            .references(self.references.iter().map(ToString::to_string).collect())
            .build_for_field(model_name, field_name)
    }

    fn field_span(&self, name: &str, prefer_first: bool) -> Option<Span> {
        if prefer_first {
            self.fields
                .iter()
                .find(|ident| *ident == name)
                .map(Ident::span)
        } else {
            self.fields
                .iter()
                .rev()
                .find(|ident| *ident == name)
                .map(Ident::span)
        }
    }

    fn reference_span(&self, name: &str, prefer_first: bool) -> Option<Span> {
        if prefer_first {
            self.references
                .iter()
                .find(|ident| *ident == name)
                .map(Ident::span)
        } else {
            self.references
                .iter()
                .rev()
                .find(|ident| *ident == name)
                .map(Ident::span)
        }
    }
}

/// Parses a bracketed comma-separated list of identifiers.
fn parse_ident_list(input: ParseStream<'_>) -> Result<Vec<Ident>> {
    let content;
    bracketed!(content in input);

    let mut items = Vec::new();
    while !content.is_empty() {
        items.push(content.call(Ident::parse_any)?);

        if content.peek(Token![,]) {
            content.parse::<Token![,]>()?;
        }
    }

    Ok(items)
}

fn scalar_type_from_ident(ident: &Ident) -> Option<core::ScalarType> {
    match ident.to_string().as_str() {
        "Int" => Some(core::ScalarType::Int),
        "String" => Some(core::ScalarType::String),
        "Boolean" => Some(core::ScalarType::Boolean),
        "DateTime" => Some(core::ScalarType::DateTime),
        "Float" => Some(core::ScalarType::Float),
        "Decimal" => Some(core::ScalarType::Decimal),
        "Bytes" => Some(core::ScalarType::Bytes),
        "Json" => Some(core::ScalarType::Json),
        _ => None,
    }
}

/// Adds an error to the accumulator, preserving earlier failures.
fn push_error(target: &mut Option<Error>, error: Error) {
    match target {
        Some(existing) => existing.combine(error),
        None => *target = Some(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    fn parse_schema(tokens: proc_macro2::TokenStream) -> ParsedSchema {
        syn::parse2(tokens).expect("schema should parse")
    }

    #[test]
    fn accepts_valid_schema_definition() {
        let schema = parse_schema(quote! {
            model user {
                id      Int     @id @default(autoincrement())
                uid     String  @unique @db.Uuid
                post    post?
                comment comment?
                status  String
            }

            model post {
                id         Int      @id @default(autoincrement())
                uid        String   @unique @db.Uuid
                user_id    Int      @unique
                created_at DateTime @default(now())
                user       User     @relation(fields: [user_id], references: [id])
                comment    comment?
            }

            model comment {
                id      Int    @id @default(autoincrement())
                post_id Int    @unique
                body    String
                post    post   @relation(fields: [post_id], references: [id])
            }
        });

        assert!(schema.validate().is_ok());
    }

    #[test]
    fn rejects_duplicate_fields() {
        let schema = parse_schema(quote! {
            model user {
                id Int @id
                id Int
            }
        });

        let error = schema.validate().expect_err("schema should fail");
        assert!(error.to_string().contains("duplicate field"));
    }

    #[test]
    fn rejects_unknown_relation_target() {
        let schema = parse_schema(quote! {
            model post {
                id      Int  @id
                user_id Int
                user    User @relation(fields: [user_id], references: [id])
            }
        });

        let error = schema.validate().expect_err("schema should fail");
        assert!(error.to_string().contains("unknown relation target model"));
    }

    #[test]
    fn rejects_invalid_default_usage() {
        let schema = parse_schema(quote! {
            model user {
                id String @id @default(autoincrement())
            }
        });

        let error = schema.validate().expect_err("schema should fail");
        assert!(error.to_string().contains("only supported on `Int` fields"));
    }

    #[test]
    fn allows_inferred_relation_fields() {
        let schema = parse_schema(quote! {
            model user {
                id      Int      @id
                post    Post?
                comment Comment?
            }

            model post {
                id      Int      @id
                user_id Int
                user    user     @relation(fields: [user_id], references: [id])
                comment Comment?
            }

            model comment {
                id      Int  @id
                post_id Int
                post    post @relation(fields: [post_id], references: [id])
            }
        });

        assert!(schema.validate().is_ok());
    }

    #[test]
    fn rejects_unknown_inferred_relation_target() {
        let schema = parse_schema(quote! {
            model user {
                id      Int      @id
                comment Missing?
            }
        });

        let error = schema.validate().expect_err("schema should fail");
        assert!(error.to_string().contains("unknown relation target model"));
    }
}
