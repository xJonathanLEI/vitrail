use proc_macro2::{Ident, Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use std::collections::HashSet;
use syn::ext::IdentExt;
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{
    Attribute, Data, DataStruct, Error, Fields, LitStr, Path, Result, Token, Type, parenthesized,
};

type UpdateDataContainerAttrs = (Path, LitStr);
type UpdateManyContainerAttrs = (Path, LitStr, Type, Option<Type>, Vec<UpdateManyRootFilter>);

pub(crate) struct UpdateDataDerive {
    ident: Ident,
    generics: syn::Generics,
    fields: Vec<UpdateField>,
    schema_path: Path,
    model_name: LitStr,
}

impl UpdateDataDerive {
    pub(crate) fn parse(input: syn::DeriveInput) -> Result<Self> {
        let ident = input.ident;
        let generics = input.generics;
        let (schema_path, model_name) = parse_update_data_container_attrs(&input.attrs)?;

        let Data::Struct(DataStruct {
            fields: Fields::Named(fields),
            ..
        }) = input.data
        else {
            return Err(Error::new(
                ident.span(),
                "`UpdateData` can only be derived for structs with named fields",
            ));
        };

        let fields = fields
            .named
            .into_iter()
            .map(|field| UpdateField::parse(field, "update data"))
            .collect::<Result<Vec<_>>>()?;

        validate_unique_update_fields(&fields, &ident, "update data")?;

        Ok(Self {
            ident,
            generics,
            fields,
            schema_path,
            model_name,
        })
    }

    pub(crate) fn expand(self) -> Result<TokenStream2> {
        let ident = self.ident;
        let mut generics = self.generics;
        let fields = self.fields;
        let schema_path = self.schema_path;
        let model_name = self.model_name;

        let schema_module_ident = schema_module_ident(&schema_path, "UpdateData")?;
        let model_ident = syn::parse_str::<Ident>(&model_name.value()).map_err(|_| {
            Error::new(
                model_name.span(),
                "`#[vitrail(model = ...)]` must be a valid identifier for `UpdateData`",
            )
        })?;
        let schema_module_path = schema_module_path(&schema_path, "UpdateData")?;
        let field_type_assert_ident = format_ident!(
            "__vitrail_assert_update_data_type_{}_{}",
            schema_module_ident,
            model_ident
        );
        let model_trait_module_ident = format_ident!(
            "__vitrail_update_traits_{}_{}",
            schema_module_ident,
            model_ident
        );

        for field in &fields {
            let field_ty = &field.ty;
            generics
                .make_where_clause()
                .predicates
                .push(syn::parse_quote!(#field_ty: ::vitrail_pg::UpdateScalar));
        }

        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        let validation_tokens = fields
            .iter()
            .map(|field| {
                let field_ident = field.schema_field_ident()?;
                let field_ty = &field.ty;

                Ok(quote! {
                    #field_type_assert_ident!(#field_ty, #field_ident);
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let update_values = fields.iter().map(|field| {
            let ident = &field.ident;
            let field_name = &field.field_name;

            quote! {
                __vitrail_values
                    .push(#field_name, ::vitrail_pg::UpdateScalar::into_update_value(self.#ident))
                    .expect("update data field names should be unique after derive validation");
            }
        });

        Ok(quote! {
            impl #impl_generics #ident #ty_generics
            #where_clause
            {
                #[doc(hidden)]
                fn __vitrail_validate_update_data() {
                    let _ = stringify!(#model_name);
                    #(#validation_tokens)*
                }
            }

            impl #impl_generics #schema_module_path::#model_trait_module_ident::__VitrailUpdateDataModel
                for #ident #ty_generics
            #where_clause
            {
            }

            impl #impl_generics ::vitrail_pg::UpdateValueSet for #ident #ty_generics
            #where_clause
            {
                fn into_update_values(self) -> ::vitrail_pg::UpdateValues {
                    Self::__vitrail_validate_update_data();

                    let mut __vitrail_values = ::vitrail_pg::UpdateValues::new();
                    #(#update_values)*
                    __vitrail_values
                }
            }
        })
    }
}

pub(crate) struct UpdateManyDerive {
    ident: Ident,
    generics: syn::Generics,
    schema_path: Path,
    model_name: LitStr,
    data_ty: Type,
    variables_ty: Option<Type>,
    root_filters: Vec<UpdateManyRootFilter>,
}

impl UpdateManyDerive {
    pub(crate) fn parse(input: syn::DeriveInput) -> Result<Self> {
        let ident = input.ident;
        let generics = input.generics;
        let (schema_path, model_name, data_ty, variables_ty, root_filters) =
            parse_update_many_container_attrs(&input.attrs)?;

        match input.data {
            Data::Struct(DataStruct {
                fields: Fields::Unit,
                ..
            }) => {}
            Data::Struct(DataStruct {
                fields: Fields::Named(ref fields),
                ..
            }) if fields.named.is_empty() => {}
            _ => {
                return Err(Error::new(
                    ident.span(),
                    "`UpdateMany` can only be derived for unit structs or empty structs",
                ));
            }
        }

        Ok(Self {
            ident,
            generics,
            schema_path,
            model_name,
            data_ty,
            variables_ty,
            root_filters,
        })
    }

    pub(crate) fn expand(self) -> Result<TokenStream2> {
        let ident = self.ident;
        let generics = self.generics;
        let schema_path = self.schema_path;
        let model_name = self.model_name;
        let data_ty = self.data_ty;
        let variables_ty = self.variables_ty;
        let root_filters = self.root_filters;

        let schema_module_ident = schema_module_ident(&schema_path, "UpdateMany")?;
        let model_ident = syn::parse_str::<Ident>(&model_name.value()).map_err(|_| {
            Error::new(
                model_name.span(),
                "`#[vitrail(model = ...)]` must be a valid identifier for `UpdateMany`",
            )
        })?;
        let schema_module_path = schema_module_path(&schema_path, "UpdateMany")?;
        let model_trait_module_ident = format_ident!(
            "__vitrail_update_traits_{}_{}",
            schema_module_ident,
            model_ident
        );
        let where_path_assert_ident = format_ident!(
            "__vitrail_assert_update_where_path_{}_{}",
            schema_module_ident,
            model_ident
        );

        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        if root_filters
            .iter()
            .any(|filter| filter.variable().is_some())
            && variables_ty.is_none()
        {
            return Err(Error::new(
                ident.span(),
                "update filters using `eq(...)`, `in(...)`, or `not(...)` require `#[vitrail(variables = YourVariablesType)]`",
            ));
        }

        let root_filter_validation_tokens = root_filters
            .iter()
            .map(|filter| filter.validation_tokens(&where_path_assert_ident))
            .collect::<Vec<_>>();

        let typed_validation_fn = if let Some(variables_ty) = &variables_ty {
            let variable_accesses = root_filters
                .iter()
                .filter_map(UpdateManyRootFilter::variable)
                .collect::<Vec<_>>();

            quote! {
                impl #impl_generics #ident #ty_generics
                #where_clause
                {
                    #[doc(hidden)]
                    fn __vitrail_validate_update_many(__vitrail_variables: Option<&#variables_ty>) {
                        #(#root_filter_validation_tokens)*
                        fn __vitrail_assert_update_values<
                            T: ::vitrail_pg::UpdateValueSet
                                + #schema_module_path::#model_trait_module_ident::__VitrailUpdateDataModel,
                        >() {
                        }
                        __vitrail_assert_update_values::<#data_ty>();

                        if let Some(__vitrail_variables) = __vitrail_variables {
                            #(let _ = &__vitrail_variables.#variable_accesses;)*
                        }
                    }
                }
            }
        } else {
            quote! {
                impl #impl_generics #ident #ty_generics
                #where_clause
                {
                    #[doc(hidden)]
                    fn __vitrail_validate_update_many() {
                        #(#root_filter_validation_tokens)*
                        fn __vitrail_assert_update_values<
                            T: ::vitrail_pg::UpdateValueSet
                                + #schema_module_path::#model_trait_module_ident::__VitrailUpdateDataModel,
                        >() {
                        }
                        __vitrail_assert_update_values::<#data_ty>();
                    }
                }
            }
        };

        let filter_exprs = root_filters
            .iter()
            .map(UpdateManyRootFilter::expand)
            .collect::<Vec<_>>();

        let filter_tokens = if filter_exprs.is_empty() {
            quote! { None }
        } else if filter_exprs.len() == 1 {
            let filter = &filter_exprs[0];
            quote! { Some(#filter) }
        } else {
            quote! { Some(::vitrail_pg::QueryFilter::And(vec![#(#filter_exprs),*])) }
        };

        let update_variables_ty = variables_ty
            .as_ref()
            .map(|variables_ty| quote! { #variables_ty })
            .unwrap_or_else(|| quote! { () });

        let validation_call = if variables_ty.is_some() {
            quote! {
                Self::__vitrail_validate_update_many(None::<&#update_variables_ty>);
            }
        } else {
            quote! {
                Self::__vitrail_validate_update_many();
            }
        };

        Ok(quote! {
            #typed_validation_fn

            impl #impl_generics ::vitrail_pg::UpdateManyModel for #ident #ty_generics
            #where_clause
            {
                type Schema = #schema_path;
                type Values = #data_ty;
                type Variables = #update_variables_ty;

                fn model_name() -> &'static str {
                    #model_name
                }

                fn filter_with_variables(
                    variables: &::vitrail_pg::QueryVariables,
                ) -> Option<::vitrail_pg::QueryFilter> {
                    let _ = variables;
                    #validation_call
                    #filter_tokens
                }
            }
        })
    }
}

struct UpdateField {
    ident: Ident,
    ty: Type,
    field_name: LitStr,
}

impl UpdateField {
    fn parse(field: syn::Field, derive_target: &str) -> Result<Self> {
        let span = field.span();
        let ident = field
            .ident
            .ok_or_else(|| Error::new(span, "expected a named field"))?;
        let mut rename = None;

        for attribute in &field.attrs {
            if !attribute.path().is_ident("vitrail") {
                continue;
            }

            attribute.parse_nested_meta(|meta| {
                if meta.path.is_ident("field") {
                    rename = Some(meta.value()?.parse::<LitStr>()?);
                    return Ok(());
                }

                Err(meta.error(format!(
                    "unsupported `#[vitrail(...)]` field attribute for {derive_target}"
                )))
            })?;
        }

        let field_name = rename.unwrap_or_else(|| LitStr::new(&ident.to_string(), ident.span()));

        Ok(Self {
            ident,
            ty: field.ty,
            field_name,
        })
    }

    fn schema_field_ident(&self) -> Result<Ident> {
        syn::parse_str::<Ident>(&self.field_name.value()).map_err(|_| {
            Error::new(
                self.field_name.span(),
                "update field names must be valid identifiers",
            )
        })
    }
}

fn validate_unique_update_fields(
    fields: &[UpdateField],
    ident: &Ident,
    derive_target: &str,
) -> Result<()> {
    let mut seen = HashSet::new();

    for field in fields {
        let field_name = field.field_name.value();
        if !seen.insert(field_name.clone()) {
            return Err(Error::new(
                ident.span(),
                format!("duplicate field `{field_name}` in {derive_target} derive"),
            ));
        }
    }

    Ok(())
}

fn parse_update_data_container_attrs(attrs: &[Attribute]) -> Result<UpdateDataContainerAttrs> {
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
            "`#[derive(UpdateData)]` requires `#[vitrail(schema = ...)]`",
        )
    })?;
    let model_name = model_name.ok_or_else(|| {
        Error::new(
            Span::call_site(),
            "`#[derive(UpdateData)]` requires `#[vitrail(model = ...)]`",
        )
    })?;

    Ok((schema_path, model_name))
}

fn parse_update_many_container_attrs(attrs: &[Attribute]) -> Result<UpdateManyContainerAttrs> {
    let mut schema_path = None;
    let mut model_name = None;
    let mut data_ty = None;
    let mut variables_ty = None;
    let mut root_filters = Vec::new();

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
            if meta.path.is_ident("data") {
                data_ty = Some(meta.value()?.parse()?);
                return Ok(());
            }
            if meta.path.is_ident("variables") {
                variables_ty = Some(meta.value()?.parse()?);
                return Ok(());
            }
            if meta.path.is_ident("where") {
                root_filters.push(parse_update_many_root_filter(meta.input)?);
                return Ok(());
            }
            Err(meta.error("unsupported `#[vitrail(...)]` container attribute"))
        })?;
    }

    let schema_path = schema_path.ok_or_else(|| {
        Error::new(
            Span::call_site(),
            "`#[derive(UpdateMany)]` requires `#[vitrail(schema = ...)]`",
        )
    })?;
    let model_name = model_name.ok_or_else(|| {
        Error::new(
            Span::call_site(),
            "`#[derive(UpdateMany)]` requires `#[vitrail(model = ...)]`",
        )
    })?;
    let data_ty = data_ty.ok_or_else(|| {
        Error::new(
            Span::call_site(),
            "`#[derive(UpdateMany)]` requires `#[vitrail(data = ...)]`",
        )
    })?;

    Ok((schema_path, model_name, data_ty, variables_ty, root_filters))
}

struct UpdateManyRootFilter {
    path: Vec<Ident>,
    filter: UpdateManyScalarFilter,
}

enum UpdateManyScalarFilter {
    Eq { variable: Ident },
    In { variable: Ident },
    Ne { variable: Ident },
    IsNull,
    IsNotNull,
}

impl UpdateManyRootFilter {
    fn expand(&self) -> TokenStream2 {
        let final_field = self
            .path
            .last()
            .expect("update filter path should never be empty");

        let mut filter = match &self.filter {
            UpdateManyScalarFilter::Eq { variable } => quote! {
                ::vitrail_pg::QueryFilter::eq(
                    stringify!(#final_field),
                    ::vitrail_pg::QueryFilterValue::variable(stringify!(#variable)),
                )
            },
            UpdateManyScalarFilter::In { variable } => quote! {
                ::vitrail_pg::QueryFilter::r#in(
                    stringify!(#final_field),
                    ::vitrail_pg::QueryFilterValues::variable(stringify!(#variable)),
                )
            },
            UpdateManyScalarFilter::Ne { variable } => quote! {
                ::vitrail_pg::QueryFilter::ne(
                    stringify!(#final_field),
                    ::vitrail_pg::QueryFilterValue::variable(stringify!(#variable)),
                )
            },
            UpdateManyScalarFilter::IsNull => quote! {
                ::vitrail_pg::QueryFilter::is_null(stringify!(#final_field))
            },
            UpdateManyScalarFilter::IsNotNull => quote! {
                ::vitrail_pg::QueryFilter::is_not_null(stringify!(#final_field))
            },
        };

        for segment in self.path[..self.path.len() - 1].iter().rev() {
            filter = quote! {
                ::vitrail_pg::QueryFilter::relation(stringify!(#segment), #filter)
            };
        }

        filter
    }

    fn validation_tokens(&self, where_path_assert_ident: &Ident) -> TokenStream2 {
        let segments = &self.path;
        quote! {
            #where_path_assert_ident!(#(#segments).*);
        }
    }

    fn variable(&self) -> Option<&Ident> {
        match &self.filter {
            UpdateManyScalarFilter::Eq { variable }
            | UpdateManyScalarFilter::In { variable }
            | UpdateManyScalarFilter::Ne { variable } => Some(variable),
            UpdateManyScalarFilter::IsNull | UpdateManyScalarFilter::IsNotNull => None,
        }
    }
}

impl Parse for UpdateManyRootFilter {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut path = vec![input.call(Ident::parse_any)?];

        while input.peek(Token![.]) {
            input.parse::<Token![.]>()?;
            path.push(input.call(Ident::parse_any)?);
        }

        input.parse::<Token![=]>()?;
        let operator = input.call(Ident::parse_any)?;

        let filter = if operator == "eq" {
            let operator_args;
            parenthesized!(operator_args in input);
            let variable = operator_args.call(Ident::parse_any)?;

            if !operator_args.is_empty() {
                return Err(Error::new(
                    operator_args.span(),
                    "unexpected tokens in `where(... = eq(...))`",
                ));
            }

            UpdateManyScalarFilter::Eq { variable }
        } else if operator == "in" {
            let operator_args;
            parenthesized!(operator_args in input);
            let variable = operator_args.call(Ident::parse_any)?;

            if !operator_args.is_empty() {
                return Err(Error::new(
                    operator_args.span(),
                    "unexpected tokens in `where(... = in(...))`",
                ));
            }

            UpdateManyScalarFilter::In { variable }
        } else if operator == "null" {
            UpdateManyScalarFilter::IsNull
        } else if operator == "not" {
            let operator_args;
            parenthesized!(operator_args in input);
            let value = operator_args.call(Ident::parse_any)?;

            if !operator_args.is_empty() {
                return Err(Error::new(
                    operator_args.span(),
                    "unexpected tokens in `where(... = not(...))`",
                ));
            }

            if value == "null" {
                UpdateManyScalarFilter::IsNotNull
            } else {
                UpdateManyScalarFilter::Ne { variable: value }
            }
        } else {
            return Err(Error::new(
                operator.span(),
                "unsupported `where` operator; only `eq`, `in`, `null`, and `not(...)` are currently supported",
            ));
        };

        if !input.is_empty() {
            return Err(Error::new(
                input.span(),
                "unexpected tokens in `where(...)`",
            ));
        }

        Ok(Self { path, filter })
    }
}

fn parse_update_many_root_filter(input: ParseStream<'_>) -> Result<UpdateManyRootFilter> {
    let content;
    parenthesized!(content in input);
    content.parse()
}

pub(crate) fn schema_module_path(schema_path: &Path, derive_name: &str) -> Result<Path> {
    if schema_path.segments.len() < 2 {
        return Err(Error::new(
            schema_path.span(),
            format!(
                "`#[vitrail(schema = ...)]` for `{derive_name}` must point to a schema type like `crate::my_schema::Schema`"
            ),
        ));
    }

    Ok(Path {
        leading_colon: schema_path.leading_colon,
        segments: schema_path
            .segments
            .iter()
            .take(schema_path.segments.len() - 1)
            .cloned()
            .collect(),
    })
}

pub(crate) fn schema_module_ident(schema_path: &Path, derive_name: &str) -> Result<Ident> {
    schema_path
        .segments
        .iter()
        .rev()
        .nth(1)
        .map(|segment| segment.ident.clone())
        .ok_or_else(|| {
            Error::new(
                schema_path.span(),
                format!(
                    "`#[vitrail(schema = ...)]` for `{derive_name}` must point to a schema type like `crate::my_schema::Schema`"
                ),
            )
        })
}
