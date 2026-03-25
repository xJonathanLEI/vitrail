use proc_macro::TokenStream;
use proc_macro2::{Ident, Punct, Spacing, Span, TokenStream as TokenStream2, TokenTree};
use quote::{ToTokens, format_ident, quote};
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
    syn::custom_keyword!(schema);
    syn::custom_keyword!(name);
    syn::custom_keyword!(include);
    syn::custom_keyword!(field);
    syn::custom_keyword!(relation);
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

#[proc_macro]
pub fn query(input: TokenStream) -> TokenStream {
    let query = syn::parse_macro_input!(input as QueryMacroInput);
    query.expand().into()
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
        let helper_macros = self.generate_query_helper_macros(module_name)?;
        let local_query_macro_ident = format_ident!("__vitrail_query_local_{}", module_name);

        Ok(quote! {
            #helper_macros

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

                pub fn query<T>() -> ::vitrail_pg::Query<Schema, T>
                where
                    T: ::vitrail_pg::QueryModel<Schema = Schema> + Sync,
                {
                    ::vitrail_pg::Query::new()
                }

                pub(crate) use #local_query_macro_ident as __query;
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

    fn generate_query_helper_macros(&self, module_name: &Ident) -> Result<TokenStream2> {
        let main_macro_ident = format_ident!("__vitrail_query_{}", module_name);
        let local_main_macro_ident = format_ident!("__vitrail_query_local_{}", module_name);
        let mut helpers = TokenStream2::new();
        let mut main_arms = Vec::new();
        let dollar_crate = dollar_crate();

        for model in &self.models {
            let model_ident = &model.name;
            let model_name = LitStr::new(&model.name.to_string(), model.name.span());
            let root_struct_ident =
                format_ident!("__VitrailQuery{}", to_pascal_case(&model.name.to_string()));
            let root_struct_macro_ident =
                format_ident!("__vitrail_root_struct_{}_{}", module_name, model.name);
            let select_assert_ident =
                format_ident!("__vitrail_assert_select_{}_{}", module_name, model.name);
            let include_assert_ident =
                format_ident!("__vitrail_assert_include_{}_{}", module_name, model.name);
            let include_struct_ident =
                format_ident!("__vitrail_include_struct_{}_{}", module_name, model.name);

            let scalar_fields = model.scalar_fields();
            let relation_fields = model.relation_fields();

            let select_assert_arms = scalar_fields.iter().map(|field| {
                let ident = &field.name;
                quote! { (#ident) => {}; }
            });

            let include_assert_arms = relation_fields.iter().map(|field| {
                let ident = &field.name;
                quote! { (#ident) => {}; }
            });

            let include_struct_arms = relation_fields
                .iter()
                .map(|field| {
                    let ident = &field.name;
                    let target = self
                        .models
                        .iter()
                        .find(|candidate| {
                            candidate.name == field.ty.name
                                || field.ty.name == to_pascal_case(&candidate.name.to_string())
                        })
                        .expect("validated relation target");
                    let target_model_name =
                        LitStr::new(&target.name.to_string(), target.name.span());
                    let target_scalar_fields = target
                        .scalar_fields()
                        .iter()
                        .map(|target_field| {
                            let field_ident = &target_field.name;
                            let field_ty = rust_type_tokens(&target_field.ty)?;
                            Ok(quote! { pub #field_ident: #field_ty, })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let target_root_struct_macro_ident =
                        format_ident!("__vitrail_root_struct_{}_{}", module_name, target.name);

                    Ok(quote! {
                        (#ident, $nested_ident:ident, true) => {
                            #[allow(dead_code)]
                            #[derive(::vitrail_pg::QueryResult)]
                            #[vitrail(schema = #dollar_crate::#module_name::Schema, model = #target_model_name)]
                            struct $nested_ident {
                                #(#target_scalar_fields)*
                            }
                        };
                        (
                            #ident,
                            $nested_ident:ident,
                            {
                                select: {
                                    $($select_field:ident : true),* $(,)?
                                }
                                $(,
                                    include: {
                                        $($include_field:ident : $include_value:tt),* $(,)?
                                    }
                                )?
                                $(,)?
                            }
                        ) => {
                            #target_root_struct_macro_ident! {
                                $nested_ident;
                                select { $($select_field),* }
                                $( include { $($include_field : $include_value),* } )?
                            }
                        };
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            let root_struct_arms = scalar_fields
                .iter()
                .map(|field| {
                    let ident = &field.name;
                    let ty = rust_type_tokens(&field.ty)?;
                    Ok(quote! {
                        (
                            @struct
                            $root_ident:ident
                            [ $($fields:tt)* ]
                            [ #ident, $($rest_select:ident,)* ]
                            [ $($include_field:ident => $include_value:tt,)* ]
                        ) => {
                            #root_struct_macro_ident! {
                                @struct
                                $root_ident
                                [ $($fields)* pub #ident: #ty, ]
                                [ $($rest_select,)* ]
                                [ $($include_field => $include_value,)* ]
                            }
                        };
                    })
                })
                .chain(relation_fields.iter().map(|field| {
                    let ident = &field.name;
                    let nested_ident = format_ident!(
                        "__VitrailQuery{}{}",
                        to_pascal_case(&model.name.to_string()),
                        to_pascal_case(&field.name.to_string())
                    );
                    let ty = if field.ty.many {
                        quote! { Vec<#nested_ident> }
                    } else if field.ty.optional {
                        quote! { Option<#nested_ident> }
                    } else {
                        quote! { #nested_ident }
                    };
                    Ok(quote! {
                        (
                            @struct
                            $root_ident:ident
                            [ $($fields:tt)* ]
                            [ ]
                            [ #ident => $include_value:tt, $($rest_include:ident => $rest_include_value:tt,)* ]
                        ) => {
                            #include_struct_ident!(#ident, #nested_ident, $include_value);

                            #root_struct_macro_ident! {
                                @struct
                                $root_ident
                                [
                                    $($fields)*
                                    #[vitrail(include)]
                                    pub #ident: #ty,
                                ]
                                [ ]
                                [ $($rest_include => $rest_include_value,)* ]
                            }
                        };
                    })
                }))
                .collect::<Result<Vec<_>>>()?;

            helpers.extend(quote! {
                #[doc(hidden)]
                #[macro_export]
                macro_rules! #select_assert_ident {
                    #(#select_assert_arms)*
                    ($other:ident) => {
                        compile_error!(concat!("unknown scalar field `", stringify!($other), "` in model `", #model_name, "`"));
                    };
                }

                #[doc(hidden)]
                #[macro_export]
                macro_rules! #include_assert_ident {
                    #(#include_assert_arms)*
                    ($other:ident) => {
                        compile_error!(concat!("unknown relation field `", stringify!($other), "` in model `", #model_name, "`"));
                    };
                }

                #[doc(hidden)]
                #[macro_export]
                macro_rules! #include_struct_ident {
                    #(#include_struct_arms)*
                    ($other:ident, $nested_ident:ident, $($tokens:tt)*) => {
                        compile_error!(concat!("unknown relation field `", stringify!($other), "` in model `", #model_name, "`"));
                    };
                }

                #[doc(hidden)]
                #[macro_export]
                macro_rules! #root_struct_macro_ident {
                    #(#root_struct_arms)*
                    (
                        $root_ident:ident;
                        select { $($select_field:ident),* $(,)? }
                        $( include { $($include_field:ident : $include_value:tt),* $(,)? } )?
                    ) => {
                        $( #select_assert_ident!($select_field); )*
                        $( $( #include_assert_ident!($include_field); )* )?

                        #root_struct_macro_ident! {
                            @struct
                            $root_ident
                            [ ]
                            [ $($select_field,)* ]
                            [ $($( $include_field => $include_value, )*)? ]
                        }
                    };
                    (
                        @struct
                        $root_ident:ident
                        [ $($fields:tt)* ]
                        [ ]
                        [ ]
                    ) => {
                        #[allow(dead_code)]
                        #[derive(::vitrail_pg::QueryResult)]
                        #[vitrail(schema = #dollar_crate::#module_name::Schema, model = #model_name)]
                        struct $root_ident {
                            $($fields)*
                        }
                    };
                }
            });

            main_arms.push(quote! {
                (
                    #model_ident {
                        select: {
                            $($select_field:ident : true),* $(,)?
                        }
                        $(,
                            include: {
                                $($include_field:ident : $include_value:tt),* $(,)?
                            }
                        )?
                        $(,)?
                    }
                ) => {{
                    #root_struct_macro_ident! {
                        #root_struct_ident;
                        select { $($select_field),* }
                        $( include { $($include_field : $include_value),* } )?
                    }

                    #dollar_crate::#module_name::query::<#root_struct_ident>()
                }};
            });
        }

        helpers.extend(quote! {
            #[doc(hidden)]
            macro_rules! #local_main_macro_ident {
                #(#main_arms)*
                ($($tokens:tt)*) => {
                    compile_error!("unsupported query shape");
                };
            }

            #[doc(hidden)]
            #[macro_export(local_inner_macros)]
            macro_rules! #main_macro_ident {
                #(#main_arms)*
                ($($tokens:tt)*) => {
                    compile_error!("unsupported query shape");
                };
            }
        });

        Ok(helpers)
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
                format!("unsupported query field type `{other}`"),
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

struct QueryMacroInput {
    schema_path: Path,
    body: TokenStream2,
}

impl Parse for QueryMacroInput {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let schema_path = input.parse()?;
        input.parse::<Token![,]>()?;
        let body: TokenStream2 = input.parse()?;
        Ok(Self { schema_path, body })
    }
}

impl QueryMacroInput {
    fn expand(self) -> TokenStream2 {
        let schema_path = self.schema_path;
        let body = self.body;
        let segments = schema_path.segments.iter().collect::<Vec<_>>();
        let module_segment = segments
            .last()
            .expect("schema path should contain at least one segment");
        let macro_ident = format_ident!("__vitrail_query_{}", module_segment.ident);

        if segments.len() == 1
            || segments
                .first()
                .is_some_and(|segment| segment.ident == "crate")
            || segments
                .first()
                .is_some_and(|segment| segment.ident == "self")
        {
            quote! {
                #schema_path::__query! {
                    #body
                }
            }
        } else {
            let root_path = Path {
                leading_colon: schema_path.leading_colon,
                segments: segments[..segments.len() - 1]
                    .iter()
                    .map(|segment| (*segment).clone())
                    .collect(),
            };
            quote! {
                #root_path::#macro_ident! {
                    #body
                }
            }
        }
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
                            #decode_relation
                        }
                    }
                } else {
                    let type_name = field.ty.to_token_stream().to_string().replace(' ', "");

                    if type_name == "chrono::DateTime<chrono::Utc>"
                        || type_name == "::chrono::DateTime<::chrono::Utc>"
                    {
                        quote! {
                            #ident: {
                                let __vitrail_alias = ::vitrail_pg::alias_name(prefix, #field_name);
                                ::vitrail_pg::row_as_datetime_utc(row, __vitrail_alias.as_str())?
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
                }
            })
            .collect::<Vec<_>>();
        let json_decode_fields = self
            .fields
            .iter()
            .enumerate()
            .map(|(index, field)| field.decode_json_field_tokens(index))
            .collect::<Result<Vec<_>>>()?;

        Ok(quote! {
            impl ::vitrail_pg::QueryValue for #ident {
                fn from_json(value: &::vitrail_pg::serde_json::Value) -> Result<Self, ::sqlx::Error> {
                    Ok(Self {
                        #(#json_decode_fields),*
                    })
                }
            }

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
        } else if let Some(inner) = vec_inner_type(&self.ty) {
            Some(quote! { #inner })
        } else {
            let ty = &self.ty;
            Some(quote! { #ty })
        }
    }

    fn decode_relation_tokens(&self, nested_ty: &TokenStream2) -> TokenStream2 {
        let field_name = &self.query_name;

        if option_inner_type(&self.ty).is_some() {
            quote! {
                {
                    let __vitrail_alias = ::vitrail_pg::alias_name(prefix, #field_name);
                    let __vitrail_value: Option<::vitrail_pg::serde_json::Value> = row.try_get(__vitrail_alias.as_str())?;
                    __vitrail_value
                        .as_ref()
                        .map(<#nested_ty as ::vitrail_pg::QueryValue>::from_json)
                        .transpose()?
                }
            }
        } else if vec_inner_type(&self.ty).is_some() {
            quote! {
                {
                    let __vitrail_alias = ::vitrail_pg::alias_name(prefix, #field_name);
                    let __vitrail_value: ::vitrail_pg::serde_json::Value = row.try_get(__vitrail_alias.as_str())?;
                    let __vitrail_items = __vitrail_value.as_array().ok_or_else(|| {
                        ::vitrail_pg::schema_error("expected JSON array in query result".to_owned())
                    })?;
                    let mut __vitrail_values = Vec::with_capacity(__vitrail_items.len());
                    for __vitrail_item in __vitrail_items {
                        __vitrail_values.push(<#nested_ty as ::vitrail_pg::QueryValue>::from_json(__vitrail_item)?);
                    }
                    __vitrail_values
                }
            }
        } else {
            quote! {
                {
                    let __vitrail_alias = ::vitrail_pg::alias_name(prefix, #field_name);
                    let __vitrail_value: ::vitrail_pg::serde_json::Value = row.try_get(__vitrail_alias.as_str())?;
                    <#nested_ty as ::vitrail_pg::QueryValue>::from_json(&__vitrail_value)?
                }
            }
        }
    }

    fn decode_json_field_tokens(&self, json_index: usize) -> Result<TokenStream2> {
        let ident = &self.ident;
        let json_index = syn::Index::from(json_index);

        if self.include {
            let nested_ty = self.nested_type().expect("include field");
            if option_inner_type(&self.ty).is_some() {
                Ok(quote! {
                    #ident: {
                        let __vitrail_value = ::vitrail_pg::json_array_field(value, #json_index)?;
                        if __vitrail_value.is_null() {
                            None
                        } else {
                            Some(<#nested_ty as ::vitrail_pg::QueryValue>::from_json(__vitrail_value)?)
                        }
                    }
                })
            } else if vec_inner_type(&self.ty).is_some() {
                Ok(quote! {
                    #ident: {
                        let __vitrail_value = ::vitrail_pg::json_array_field(value, #json_index)?;
                        let __vitrail_items = __vitrail_value.as_array().ok_or_else(|| {
                            ::vitrail_pg::schema_error("expected JSON array in query result".to_owned())
                        })?;
                        let mut __vitrail_values = Vec::with_capacity(__vitrail_items.len());
                        for __vitrail_item in __vitrail_items {
                            __vitrail_values.push(<#nested_ty as ::vitrail_pg::QueryValue>::from_json(__vitrail_item)?);
                        }
                        __vitrail_values
                    }
                })
            } else {
                Ok(quote! {
                    #ident: {
                        let __vitrail_value = ::vitrail_pg::json_array_field(value, #json_index)?;
                        <#nested_ty as ::vitrail_pg::QueryValue>::from_json(__vitrail_value)?
                    }
                })
            }
        } else {
            let decode = json_decode_tokens_for_type(
                &self.ty,
                quote! { ::vitrail_pg::json_array_field(value, #json_index)? },
            )?;
            Ok(quote! {
                #ident: { #decode }
            })
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
    generic_inner_type(ty, "Option")
}

fn vec_inner_type(ty: &Type) -> Option<Type> {
    generic_inner_type(ty, "Vec")
}

fn generic_inner_type(ty: &Type, expected: &str) -> Option<Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let segment = type_path.path.segments.last()?;
    if segment.ident != expected {
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

fn json_decode_tokens_for_type(value_ty: &Type, value_expr: TokenStream2) -> Result<TokenStream2> {
    if let Some(inner) = option_inner_type(value_ty) {
        let inner_decode = json_decode_tokens_for_type(&inner, quote! { __vitrail_value })?;
        return Ok(quote! {
            {
                let __vitrail_value = #value_expr;
                if __vitrail_value.is_null() {
                    None
                } else {
                    Some({ #inner_decode })
                }
            }
        });
    }

    let type_name = value_ty.to_token_stream().to_string().replace(' ', "");

    if type_name == "i64" {
        return Ok(quote! { ::vitrail_pg::json_as_i64(#value_expr)? });
    }
    if type_name == "String" {
        return Ok(quote! { ::vitrail_pg::json_as_string(#value_expr)? });
    }
    if type_name == "bool" {
        return Ok(quote! { ::vitrail_pg::json_as_bool(#value_expr)? });
    }
    if type_name == "f64" {
        return Ok(quote! { ::vitrail_pg::json_as_f64(#value_expr)? });
    }
    if type_name == "chrono::DateTime<chrono::Utc>"
        || type_name == "::chrono::DateTime<::chrono::Utc>"
    {
        return Ok(quote! { ::vitrail_pg::json_as_datetime_utc(#value_expr)? });
    }

    Err(Error::new(
        value_ty.span(),
        format!("unsupported query field type `{}`", type_name),
    ))
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
