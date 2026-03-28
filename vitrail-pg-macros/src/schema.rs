use proc_macro2::{Ident, Punct, Spacing, Span, TokenStream as TokenStream2, TokenTree};
use quote::quote;
use syn::ext::IdentExt;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{Error, Result, Token, bracketed, parenthesized};
use vitrail_pg_core as core;

mod kw {
    syn::custom_keyword!(model);
    syn::custom_keyword!(schema);
    syn::custom_keyword!(name);
    syn::custom_keyword!(include);
    syn::custom_keyword!(field);
    syn::custom_keyword!(relation);
}

mod helpers;
mod insert_helpers;
mod query_helpers;

/// Parsed top-level schema definition plus enough source metadata to translate
/// clean core validation errors back into compiler diagnostics with spans.
#[derive(Debug)]
pub(crate) struct ParsedSchema {
    module_name: Ident,
    models: Vec<ParsedModel>,
}

impl Parse for ParsedSchema {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        input.parse::<kw::name>()?;
        let module_name = input.call(Ident::parse_any)?;

        let mut models = Vec::new();
        while !input.is_empty() {
            models.push(input.parse()?);
        }

        Ok(Self {
            module_name,
            models,
        })
    }
}

impl ParsedSchema {
    pub(crate) fn expand(&self) -> Result<TokenStream2> {
        self.validate()?;
        self.generate_named_schema()
    }

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

    fn generate_schema_model(&self, schema: &ParsedSchema) -> Result<TokenStream2> {
        let model_name = syn::LitStr::new(&self.name.to_string(), self.name.span());
        let mut fields = Vec::with_capacity(self.fields.len());

        for field in &self.fields {
            fields.push(field.generate_schema_field(schema, self)?);
        }

        Ok(quote! {
            ::vitrail_pg::Model::builder(#model_name)
                .fields(vec![#(#fields),*])
                .build()
                .expect("model was validated during macro expansion")
        })
    }

    fn scalar_fields(&self) -> Vec<&ParsedField> {
        self.fields
            .iter()
            .filter(|field| scalar_type_from_ident(&field.ty.name).is_some())
            .collect()
    }

    fn relation_fields(&self) -> Vec<&ParsedField> {
        self.fields
            .iter()
            .filter(|field| scalar_type_from_ident(&field.ty.name).is_none())
            .collect()
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

    fn generate_schema_field(
        &self,
        schema: &ParsedSchema,
        model: &ParsedModel,
    ) -> Result<TokenStream2> {
        let field_name = syn::LitStr::new(&self.name.to_string(), self.name.span());
        let ty = self.ty.generate_schema_field_type();
        let mut attributes = Vec::with_capacity(self.attributes.len() + 1);

        for attribute in &self.attributes {
            attributes.push(attribute.generate_schema_attribute()?);
        }

        if matches!(
            self.ty.to_core(),
            core::FieldType::Relation { many: false, .. }
        ) && self.relation().is_none()
        {
            attributes.push(schema.generate_relation_attribute(model, self)?);
        }

        Ok(quote! {
            ::vitrail_pg::Field::builder(#field_name, #ty)
                .attributes(vec![#(#attributes),*])
                .build()
                .expect("field was validated during macro expansion")
        })
    }

    fn relation(&self) -> Option<&ParsedRelationAttribute> {
        self.attributes
            .iter()
            .find_map(|attribute| match &attribute.kind {
                ParsedAttributeKind::Relation(relation) => Some(relation),
                _ => None,
            })
    }

    fn can_be_omitted_in_insert(&self) -> bool {
        self.ty.optional
            || self.attributes.iter().any(|attribute| {
                matches!(
                    &attribute.kind,
                    ParsedAttributeKind::Default(default)
                        if matches!(
                            default.function.to_string().as_str(),
                            "autoincrement" | "now"
                        )
                )
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

/// Parsed field type, including optionality and relation cardinality.
#[derive(Debug)]
struct ParsedFieldType {
    name: Ident,
    optional: bool,
    many: bool,
}

impl Parse for ParsedFieldType {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let name = input.call(Ident::parse_any)?;
        let many = if input.peek(syn::token::Bracket) {
            let content;
            bracketed!(content in input);
            if !content.is_empty() {
                return Err(Error::new(
                    content.span(),
                    "expected `[]` for relation list syntax",
                ));
            }
            true
        } else {
            false
        };
        let optional = if input.peek(Token![?]) {
            if many {
                return Err(Error::new(
                    input.span(),
                    "relation list fields cannot be optional",
                ));
            }
            input.parse::<Token![?]>()?;
            true
        } else {
            false
        };

        if many && scalar_type_from_ident(&name).is_some() {
            return Err(Error::new(
                name.span(),
                "list syntax is only supported for relation fields",
            ));
        }

        Ok(Self {
            name,
            optional,
            many,
        })
    }
}

impl ParsedFieldType {
    fn to_core(&self) -> core::FieldType {
        match scalar_type_from_ident(&self.name) {
            Some(scalar) => core::FieldType::scalar(scalar, self.optional),
            None => core::FieldType::relation(self.name.to_string(), self.optional, self.many),
        }
    }

    fn generate_schema_field_type(&self) -> TokenStream2 {
        match scalar_type_from_ident(&self.name) {
            Some(scalar) => {
                let variant = scalar_type_variant(scalar);
                let optional = self.optional;
                quote! { ::vitrail_pg::FieldType::scalar(::vitrail_pg::ScalarType::#variant, #optional) }
            }
            None => {
                let model = syn::LitStr::new(&self.name.to_string(), self.name.span());
                let optional = self.optional;
                let many = self.many;
                quote! { ::vitrail_pg::FieldType::relation(#model, #optional, #many) }
            }
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

    fn generate_schema_attribute(&self) -> Result<TokenStream2> {
        Ok(match &self.kind {
            ParsedAttributeKind::Id => quote! { ::vitrail_pg::Attribute::Id },
            ParsedAttributeKind::Unique => quote! { ::vitrail_pg::Attribute::Unique },
            ParsedAttributeKind::Default(default) => {
                let default = default.generate_schema_default_attribute();
                quote! { ::vitrail_pg::Attribute::Default(#default) }
            }
            ParsedAttributeKind::Relation(relation) => {
                let relation = relation.generate_schema_relation_attribute()?;
                quote! { ::vitrail_pg::Attribute::Relation(#relation) }
            }
            ParsedAttributeKind::DbUuid => quote! { ::vitrail_pg::Attribute::DbUuid },
        })
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

    fn generate_schema_default_attribute(&self) -> TokenStream2 {
        let function = match self.function.to_string().as_str() {
            "autoincrement" => quote! { ::vitrail_pg::DefaultFunction::Autoincrement },
            "now" => quote! { ::vitrail_pg::DefaultFunction::Now },
            other => {
                let other = syn::LitStr::new(other, self.function.span());
                quote! { ::vitrail_pg::DefaultFunction::Other(#other.to_owned()) }
            }
        };

        quote! { ::vitrail_pg::DefaultAttribute::new(#function) }
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

    fn generate_schema_relation_attribute(&self) -> Result<TokenStream2> {
        let fields = self
            .fields
            .iter()
            .map(|field| syn::LitStr::new(&field.to_string(), field.span()))
            .collect::<Vec<_>>();
        let references = self
            .references
            .iter()
            .map(|reference| syn::LitStr::new(&reference.to_string(), reference.span()))
            .collect::<Vec<_>>();

        Ok(quote! {
            ::vitrail_pg::RelationAttribute::builder()
                .fields(vec![#(#fields.to_owned()),*])
                .references(vec![#(#references.to_owned()),*])
                .build()
                .expect("relation attribute was validated during macro expansion")
        })
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

    Punctuated::<Ident, Token![,]>::parse_terminated(&content)
        .map(|items| items.into_iter().collect())
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

fn rust_type_tokens(ty: &ParsedFieldType) -> Result<TokenStream2> {
    let base = match ty.name.to_string().as_str() {
        "Int" => quote! { i64 },
        "String" => quote! { String },
        "Boolean" => quote! { bool },
        "DateTime" => quote! { ::chrono::DateTime<::chrono::Utc> },
        "Float" => quote! { f64 },
        other => {
            return Err(Error::new(
                ty.name.span(),
                format!("unsupported scalar field type `{other}`"),
            ));
        }
    };

    Ok(if ty.optional {
        quote! { Option<#base> }
    } else {
        base
    })
}

fn to_pascal_case(name: &str) -> String {
    let mut result = String::new();

    for segment in name.split('_').filter(|segment| !segment.is_empty()) {
        let mut chars = segment.chars();
        if let Some(first) = chars.next() {
            result.extend(first.to_uppercase());
            result.push_str(chars.as_str());
        }
    }

    result
}

/// Adds an error to the accumulator, preserving earlier failures.
fn push_error(target: &mut Option<Error>, error: Error) {
    match target {
        Some(existing) => existing.combine(error),
        None => *target = Some(error),
    }
}

fn scalar_type_variant(scalar: core::ScalarType) -> Ident {
    match scalar {
        core::ScalarType::Int => Ident::new("Int", Span::call_site()),
        core::ScalarType::String => Ident::new("String", Span::call_site()),
        core::ScalarType::Boolean => Ident::new("Boolean", Span::call_site()),
        core::ScalarType::DateTime => Ident::new("DateTime", Span::call_site()),
        core::ScalarType::Float => Ident::new("Float", Span::call_site()),
        core::ScalarType::Decimal => Ident::new("Decimal", Span::call_site()),
        core::ScalarType::Bytes => Ident::new("Bytes", Span::call_site()),
        core::ScalarType::Json => Ident::new("Json", Span::call_site()),
    }
}

fn dollar_crate() -> TokenStream2 {
    let mut tokens = TokenStream2::new();
    tokens.extend([
        TokenTree::Punct(Punct::new('$', Spacing::Joint)),
        TokenTree::Ident(Ident::new("crate", Span::call_site())),
    ]);
    tokens
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
            name my_schema

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

        schema.validate().expect("schema should validate");
    }

    #[test]
    fn generates_named_schema_support_items() {
        let schema = parse_schema(quote! {
            name my_schema

            model user {
                id         Int      @id @default(autoincrement())
                email      String   @unique
                name       String
                created_at DateTime @default(now())
            }

            model post {
                id         Int      @id @default(autoincrement())
                title      String
                body       String?
                published  Boolean
                author_id  Int
                created_at DateTime @default(now())
                author     user     @relation(fields: [author_id], references: [id])
            }
        });

        let generated = schema.expand().expect("schema should expand").to_string();
        assert!(generated.contains("pub mod my_schema"));
        assert!(generated.contains("pub fn query < T > ()"));
        assert!(generated.contains("macro_rules ! __vitrail_query_my_schema"));
    }

    #[test]
    fn accepts_relation_list_schema_definition() {
        let schema: ParsedSchema = syn::parse2(quote! {
            name relation_list_schema

            model user {
                id    Int    @id @default(autoincrement())
                posts post[]
            }

            model post {
                id        Int    @id @default(autoincrement())
                author_id Int
                author    user   @relation(fields: [author_id], references: [id])
            }
        })
        .expect("schema should parse");

        schema.validate().expect("schema should validate");
    }
}
