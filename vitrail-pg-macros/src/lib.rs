use proc_macro::TokenStream;
use proc_macro2::{Ident, Span, TokenStream as TokenStream2};
use quote::quote;
use syn::ext::IdentExt;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{
    Attribute, Data, DataStruct, Error, Fields, LitStr, Path, Result, Token, Type, bracketed,
    parenthesized,
};
use vitrail_pg_core as core;

mod kw {
    syn::custom_keyword!(model);
    syn::custom_keyword!(name);
}

/// Validates a schema DSL declaration at compile time.
#[proc_macro]
pub fn schema(input: TokenStream) -> TokenStream {
    let schema = syn::parse_macro_input!(input as ParsedSchema);

    match schema.expand() {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

#[proc_macro_derive(QueryResult, attributes(vitrail))]
pub fn derive_query_result(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    match QueryResultDerive::parse(input).and_then(|derive| derive.expand()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Parsed top-level schema definition plus enough source metadata to translate
/// clean core validation errors back into compiler diagnostics with spans.
#[derive(Debug)]
struct ParsedSchema {
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
    fn expand(&self) -> Result<TokenStream2> {
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

    fn generate_named_schema(&self) -> Result<TokenStream2> {
        let module_name = &self.module_name;
        let schema = self.generate_schema()?;

        Ok(quote! {
            pub mod #module_name {
                static __SCHEMA: ::std::sync::OnceLock<::vitrail_pg::Schema> =
                    ::std::sync::OnceLock::new();

                #[derive(Clone, Copy, Debug, Default)]
                pub struct Schema;

                impl ::vitrail_pg::SchemaAccess for Schema {
                    fn schema() -> &'static ::vitrail_pg::Schema {
                        __SCHEMA.get_or_init(|| #schema)
                    }
                }

                #[derive(Clone, Debug)]
                pub struct VitrailClient(::vitrail_pg::SqlxVitrailClient);

                impl VitrailClient {
                    pub async fn new(database_url: &str) -> Result<Self, ::sqlx::Error> {
                        Ok(Self(::vitrail_pg::SqlxVitrailClient::new(database_url).await?))
                    }

                    pub fn from_inner(inner: ::vitrail_pg::SqlxVitrailClient) -> Self {
                        Self(inner)
                    }

                    pub fn inner(&self) -> &::vitrail_pg::SqlxVitrailClient {
                        &self.0
                    }

                    pub async fn find_many<Q>(
                        &self,
                        query: Q,
                    ) -> Result<Vec<Q::Output>, ::sqlx::Error>
                    where
                        Q: ::vitrail_pg::QuerySpec,
                    {
                        self.0.find_many(query).await
                    }

                    pub async fn find_optional<Q>(
                        &self,
                        query: Q,
                    ) -> Result<Option<Q::Output>, ::sqlx::Error>
                    where
                        Q: ::vitrail_pg::QuerySpec,
                    {
                        self.0.find_optional(query).await
                    }

                    pub async fn find_unique<Q>(
                        &self,
                        query: Q,
                    ) -> Result<Q::Output, ::sqlx::Error>
                    where
                        Q: ::vitrail_pg::QuerySpec,
                    {
                        self.0.find_unique(query).await
                    }
                }

                pub fn query<T>() -> ::vitrail_pg::Query<Schema, T>
                where
                    T: ::vitrail_pg::QueryModel<Schema = Schema> + Sync,
                {
                    ::vitrail_pg::Query::new()
                }
            }
        })
    }

    fn generate_schema(&self) -> Result<TokenStream2> {
        let mut models = Vec::with_capacity(self.models.len());

        for model in &self.models {
            models.push(model.generate_schema_model(self)?);
        }

        Ok(quote! {
            ::vitrail_pg::Schema::builder()
                .models(vec![#(#models),*])
                .build()
                .expect("schema was validated during macro expansion")
        })
    }

    fn find_model(&self, name: &str) -> Option<&ParsedModel> {
        self.models
            .iter()
            .find(|model| self.model_names_match(&model.name.to_string(), name))
    }

    fn model_names_match(&self, left: &str, right: &str) -> bool {
        left.eq_ignore_ascii_case(right)
    }

    fn infer_relation_fields(
        &self,
        model: &ParsedModel,
        field: &ParsedField,
        target_model: &ParsedModel,
    ) -> Result<(Vec<syn::LitStr>, Vec<syn::LitStr>)> {
        let reverse_relation = target_model
            .fields
            .iter()
            .find(|candidate| {
                self.model_names_match(&candidate.ty.name.to_string(), &model.name.to_string())
                    && candidate.relation().is_some()
            })
            .ok_or_else(|| {
                Error::new(
                    field.ty.name.span(),
                    format!(
                        "could not infer relation metadata for `{}.{}`",
                        model.name, field.name
                    ),
                )
            })?;

        let reverse_relation = reverse_relation
            .relation()
            .expect("reverse relation existence checked above");

        let local_fields = reverse_relation
            .references
            .iter()
            .map(|ident| syn::LitStr::new(&ident.to_string(), ident.span()))
            .collect::<Vec<_>>();
        let referenced_fields = reverse_relation
            .fields
            .iter()
            .map(|ident| syn::LitStr::new(&ident.to_string(), ident.span()))
            .collect::<Vec<_>>();

        Ok((local_fields, referenced_fields))
    }

    fn generate_relation_attribute(
        &self,
        model: &ParsedModel,
        field: &ParsedField,
    ) -> Result<TokenStream2> {
        let (fields, references) = match field.relation() {
            Some(relation) => (
                relation
                    .fields
                    .iter()
                    .map(|ident| syn::LitStr::new(&ident.to_string(), ident.span()))
                    .collect::<Vec<_>>(),
                relation
                    .references
                    .iter()
                    .map(|ident| syn::LitStr::new(&ident.to_string(), ident.span()))
                    .collect::<Vec<_>>(),
            ),
            None => {
                let target_model =
                    self.find_model(&field.ty.name.to_string()).ok_or_else(|| {
                        Error::new(
                            field.ty.name.span(),
                            format!(
                                "unknown relation target model `{}` for field `{}`",
                                field.ty.name, field.name
                            ),
                        )
                    })?;

                self.infer_relation_fields(model, field, target_model)?
            }
        };

        Ok(quote! {
            ::vitrail_pg::Attribute::Relation(
                ::vitrail_pg::RelationAttribute::builder()
                    .fields(vec![#(#fields.to_owned()),*])
                    .references(vec![#(#references.to_owned()),*])
                    .build()
                    .expect("relation attribute was validated during macro expansion")
            )
        })
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

        if matches!(self.ty.to_core(), core::FieldType::Relation { .. })
            && self.relation().is_none()
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
                quote! { ::vitrail_pg::FieldType::relation(#model, #optional) }
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

struct QueryResultDerive {
    ident: Ident,
    fields: Vec<QueryResultField>,
    schema_path: Path,
    model_name: LitStr,
}

impl QueryResultDerive {
    fn parse(input: syn::DeriveInput) -> Result<Self> {
        let ident = input.ident;
        let (schema_path, model_name) = parse_container_attrs(&input.attrs)?;

        let Data::Struct(DataStruct {
            fields: Fields::Named(fields),
            ..
        }) = input.data
        else {
            return Err(Error::new(
                ident.span(),
                "`QueryResult` can only be derived for structs with named fields",
            ));
        };

        let fields = fields
            .named
            .into_iter()
            .map(QueryResultField::parse)
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            ident,
            fields,
            schema_path,
            model_name,
        })
    }

    fn expand(self) -> Result<TokenStream2> {
        let ident = self.ident;
        let schema_path = self.schema_path;
        let model_name = self.model_name;
        let scalar_fields: Vec<_> = self.fields.iter().filter(|field| !field.include).collect();
        let relation_fields: Vec<_> = self.fields.iter().filter(|field| field.include).collect();

        let selection_scalars = scalar_fields.iter().map(|field| {
            let name = &field.query_name;
            quote! { #name }
        });
        let selection_relations = relation_fields.iter().map(|field| {
            let name = &field.query_name;
            let nested_ty = field.nested_type().expect("include field");
            quote! {
                ::vitrail_pg::QueryRelationSelection {
                    field: #name,
                    selection: <#nested_ty as ::vitrail_pg::QueryModel>::selection(),
                }
            }
        });

        let decode_fields = self
            .fields
            .iter()
            .map(|field| {
                let ident = &field.ident;
                let field_name = &field.query_name;

                if field.include {
                    let nested_ty = field.nested_type().expect("include field");
                    let decode_relation = field.decode_relation_tokens(&nested_ty);
                    quote! {
                        #ident: {
                            let __vitrail_prefix = ::vitrail_pg::alias_name(prefix, #field_name);
                            #decode_relation
                        }
                    }
                } else {
                    quote! {
                        #ident: {
                            let __vitrail_alias = ::vitrail_pg::alias_name(prefix, #field_name);
                            row.try_get(__vitrail_alias.as_str())?
                        }
                    }
                }
            })
            .collect::<Vec<_>>();

        Ok(quote! {
            impl ::vitrail_pg::QueryModel for #ident {
                type Schema = #schema_path;

                fn model_name() -> &'static str {
                    #model_name
                }

                fn selection() -> ::vitrail_pg::QuerySelection {
                    ::vitrail_pg::QuerySelection {
                        model: #model_name,
                        scalar_fields: vec![#(#selection_scalars),*],
                        relations: vec![#(#selection_relations),*],
                    }
                }

                fn from_row(
                    row: &::sqlx::postgres::PgRow,
                    prefix: &str,
                ) -> Result<Self, ::sqlx::Error> {
                    use ::sqlx::Row as _;

                    Ok(Self {
                        #(#decode_fields),*
                    })
                }
            }
        })
    }
}

struct QueryResultField {
    ident: Ident,
    ty: Type,
    query_name: LitStr,
    include: bool,
}

impl QueryResultField {
    fn parse(field: syn::Field) -> Result<Self> {
        let span = field.span();
        let ident = field
            .ident
            .ok_or_else(|| Error::new(span, "expected a named field"))?;
        let mut include = false;
        let mut rename = None;

        for attribute in &field.attrs {
            if !attribute.path().is_ident("vitrail") {
                continue;
            }

            attribute.parse_nested_meta(|meta| {
                if meta.path.is_ident("include") {
                    include = true;
                    return Ok(());
                }
                if meta.path.is_ident("field") || meta.path.is_ident("relation") {
                    let value = meta.value()?;
                    rename = Some(value.parse::<LitStr>()?);
                    return Ok(());
                }
                Err(meta.error("unsupported `#[vitrail(...)]` field attribute"))
            })?;
        }

        let query_name = rename.unwrap_or_else(|| LitStr::new(&ident.to_string(), ident.span()));

        Ok(Self {
            ident,
            ty: field.ty,
            query_name,
            include,
        })
    }

    fn nested_type(&self) -> Option<TokenStream2> {
        if !self.include {
            return None;
        }

        if let Some(inner) = option_inner_type(&self.ty) {
            Some(quote! { #inner })
        } else {
            let ty = &self.ty;
            Some(quote! { #ty })
        }
    }

    fn decode_relation_tokens(&self, nested_ty: &TokenStream2) -> TokenStream2 {
        if option_inner_type(&self.ty).is_some() {
            quote! {
                if ::vitrail_pg::query_model_is_null::<#nested_ty>(row, &__vitrail_prefix)? {
                    None
                } else {
                    Some(<#nested_ty as ::vitrail_pg::QueryModel>::from_row(row, &__vitrail_prefix)?)
                }
            }
        } else {
            quote! {
                <#nested_ty as ::vitrail_pg::QueryModel>::from_row(row, &__vitrail_prefix)?
            }
        }
    }
}

fn parse_container_attrs(attrs: &[Attribute]) -> Result<(Path, LitStr)> {
    let mut schema_path = None;
    let mut model_name = None;

    for attribute in attrs {
        if !attribute.path().is_ident("vitrail") {
            continue;
        }

        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("schema") {
                schema_path = Some(meta.value()?.parse()?);
                return Ok(());
            }
            if meta.path.is_ident("model") {
                let value = meta.value()?;
                if value.peek(LitStr) {
                    model_name = Some(value.parse::<LitStr>()?);
                } else {
                    let ident = value.parse::<Ident>()?;
                    model_name = Some(LitStr::new(&ident.to_string(), ident.span()));
                }
                return Ok(());
            }
            Err(meta.error("unsupported `#[vitrail(...)]` container attribute"))
        })?;
    }

    let schema_path = schema_path.ok_or_else(|| {
        Error::new(
            Span::call_site(),
            "`#[derive(QueryResult)]` requires `#[vitrail(schema = ...)]`",
        )
    })?;
    let model_name = model_name.ok_or_else(|| {
        Error::new(
            Span::call_site(),
            "`#[derive(QueryResult)]` requires `#[vitrail(model = ...)]`",
        )
    })?;

    Ok((schema_path, model_name))
}

fn option_inner_type(ty: &Type) -> Option<Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let segment = type_path.path.segments.last()?;
    if segment.ident != "Option" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    let generic = arguments.args.first()?;
    let syn::GenericArgument::Type(inner) = generic else {
        return None;
    };
    Some(inner.clone())
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
        assert!(generated.contains("pub struct Schema"));
        assert!(generated.contains("pub fn query < T > ()"));
    }
}
