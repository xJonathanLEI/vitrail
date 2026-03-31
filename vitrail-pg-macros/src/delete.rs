use crate::filter::{RootFilter, parse_root_filter};
use proc_macro2::{Ident, Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{Attribute, Data, DataStruct, Error, Fields, LitStr, Path, Result};

use crate::update::{schema_module_ident, schema_module_path};

type DeleteManyContainerAttrs = (Path, LitStr, Option<syn::Type>, Vec<RootFilter>);

pub(crate) struct DeleteManyDerive {
    ident: Ident,
    generics: syn::Generics,
    schema_path: Path,
    model_name: LitStr,
    variables_ty: Option<syn::Type>,
    root_filters: Vec<RootFilter>,
}

impl DeleteManyDerive {
    pub(crate) fn parse(input: syn::DeriveInput) -> Result<Self> {
        let ident = input.ident;
        let generics = input.generics;
        let (schema_path, model_name, variables_ty, root_filters) =
            parse_delete_many_container_attrs(&input.attrs)?;

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
                    "`DeleteMany` can only be derived for unit structs or empty structs",
                ));
            }
        }

        Ok(Self {
            ident,
            generics,
            schema_path,
            model_name,
            variables_ty,
            root_filters,
        })
    }

    pub(crate) fn expand(self) -> Result<TokenStream2> {
        let ident = self.ident;
        let generics = self.generics;
        let schema_path = self.schema_path;
        let model_name = self.model_name;
        let variables_ty = self.variables_ty;
        let root_filters = self.root_filters;

        let schema_module_ident = schema_module_ident(&schema_path, "DeleteMany")?;
        let model_ident = syn::parse_str::<Ident>(&model_name.value()).map_err(|_| {
            Error::new(
                model_name.span(),
                "`#[vitrail(model = ...)]` must be a valid identifier for `DeleteMany`",
            )
        })?;
        let schema_module_path = schema_module_path(&schema_path, "DeleteMany")?;
        let model_trait_module_ident = format_ident!(
            "__vitrail_delete_traits_{}_{}",
            schema_module_ident,
            model_ident
        );
        let where_path_assert_ident = format_ident!(
            "__vitrail_assert_delete_where_path_{}_{}",
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
                "delete filters using `eq(...)`, `in(...)`, or `not(...)` require `#[vitrail(variables = YourVariablesType)]`",
            ));
        }

        let where_path_assert_macro = quote! {
            #schema_module_path::#where_path_assert_ident
        };
        let root_filter_validation_tokens = root_filters
            .iter()
            .map(|filter| filter.validation_tokens(&where_path_assert_macro))
            .collect::<Vec<_>>();

        let typed_validation_fn = if let Some(variables_ty) = &variables_ty {
            let variable_accesses = root_filters
                .iter()
                .filter_map(RootFilter::variable)
                .collect::<Vec<_>>();

            quote! {
                impl #impl_generics #ident #ty_generics
                #where_clause
                {
                    #[doc(hidden)]
                    fn __vitrail_validate_delete_many(__vitrail_variables: Option<&#variables_ty>) {
                        #(#root_filter_validation_tokens)*
                        fn __vitrail_assert_delete_model<
                            T: #schema_module_path::#model_trait_module_ident::__VitrailDeleteModel,
                        >() {
                        }
                        __vitrail_assert_delete_model::<Self>();

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
                    fn __vitrail_validate_delete_many() {
                        #(#root_filter_validation_tokens)*
                        fn __vitrail_assert_delete_model<
                            T: #schema_module_path::#model_trait_module_ident::__VitrailDeleteModel,
                        >() {
                        }
                        __vitrail_assert_delete_model::<Self>();
                    }
                }
            }
        };

        let filter_exprs = root_filters
            .iter()
            .map(RootFilter::expand)
            .collect::<Vec<_>>();

        let filter_tokens = if filter_exprs.is_empty() {
            quote! { None }
        } else if filter_exprs.len() == 1 {
            let filter = &filter_exprs[0];
            quote! { Some(#filter) }
        } else {
            quote! { Some(::vitrail_pg::QueryFilter::And(vec![#(#filter_exprs),*])) }
        };

        let delete_variables_ty = variables_ty
            .as_ref()
            .map(|variables_ty| quote! { #variables_ty })
            .unwrap_or_else(|| quote! { () });

        let validation_call = if variables_ty.is_some() {
            quote! {
                Self::__vitrail_validate_delete_many(None::<&#delete_variables_ty>);
            }
        } else {
            quote! {
                Self::__vitrail_validate_delete_many();
            }
        };

        Ok(quote! {
            #typed_validation_fn

            impl #impl_generics #schema_module_path::#model_trait_module_ident::__VitrailDeleteModel
                for #ident #ty_generics
            #where_clause
            {
            }

            impl #impl_generics ::vitrail_pg::DeleteManyModel for #ident #ty_generics
            #where_clause
            {
                type Schema = #schema_path;
                type Variables = #delete_variables_ty;

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

fn parse_delete_many_container_attrs(attrs: &[Attribute]) -> Result<DeleteManyContainerAttrs> {
    let mut schema_path = None;
    let mut model_name = None;
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
            if meta.path.is_ident("variables") {
                variables_ty = Some(meta.value()?.parse()?);
                return Ok(());
            }
            if meta.path.is_ident("where") {
                root_filters.push(parse_root_filter(meta.input)?);
                return Ok(());
            }
            Err(meta.error("unsupported `#[vitrail(...)]` container attribute"))
        })?;
    }

    let schema_path = schema_path.ok_or_else(|| {
        Error::new(
            Span::call_site(),
            "`#[derive(DeleteMany)]` requires `#[vitrail(schema = ...)]`",
        )
    })?;
    let model_name = model_name.ok_or_else(|| {
        Error::new(
            Span::call_site(),
            "`#[derive(DeleteMany)]` requires `#[vitrail(model = ...)]`",
        )
    })?;

    Ok((schema_path, model_name, variables_ty, root_filters))
}
