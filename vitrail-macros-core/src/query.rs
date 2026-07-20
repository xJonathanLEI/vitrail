use proc_macro2::{Ident, Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::ext::IdentExt;
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{
    Attribute, Data, DataStruct, Error, Fields, LitStr, Path, Result, Token, Type, parenthesized,
};

use crate::filter::{RootFilter, parse_root_filter};
use crate::helper_macro::expand_helper_macro;
use crate::order::{RootOrder, parse_root_orders};

/// Dialect-specific paths used by shared query procedural macro expansion.
pub struct QueryMacroConfig {
    runtime_path: Path,
    row_path: Path,
    error_path: Path,
}

impl QueryMacroConfig {
    pub fn new(runtime_path: Path, row_path: Path, error_path: Path) -> Self {
        Self {
            runtime_path,
            row_path,
            error_path,
        }
    }

    pub fn runtime_path(&self) -> &Path {
        &self.runtime_path
    }

    pub fn row_path(&self) -> &Path {
        &self.row_path
    }

    pub fn error_path(&self) -> &Path {
        &self.error_path
    }
}

/// Expands the user-facing `query!` macro into its schema-generated helper.
pub fn expand_query(input: TokenStream2) -> Result<TokenStream2> {
    let input = syn::parse2::<QueryMacroInput>(input)?;
    Ok(expand_helper_macro(input.schema_path, input.body, "query"))
}

/// Expands a `QueryResult` derive using dialect-specific runtime paths.
pub fn expand_query_result(
    input: syn::DeriveInput,
    config: &QueryMacroConfig,
) -> Result<TokenStream2> {
    QueryResultDerive::parse(input)?.expand(config)
}

/// Expands a `QueryVariables` derive using a dialect-specific runtime path.
pub fn expand_query_variables(
    input: syn::DeriveInput,
    config: &QueryMacroConfig,
) -> Result<TokenStream2> {
    QueryVariablesDerive::parse(input)?.expand(config)
}

struct QueryMacroInput {
    schema_path: Path,
    body: TokenStream2,
}

impl Parse for QueryMacroInput {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let schema_path = input.parse()?;
        input.parse::<Token![,]>()?;
        let body = input.parse()?;

        Ok(Self { schema_path, body })
    }
}

type QueryResultContainerAttrs = (
    Path,
    LitStr,
    Option<Type>,
    Vec<RootFilter>,
    Vec<RootOrder>,
    Option<QueryPaginationAttr>,
    Option<QueryPaginationAttr>,
);

pub(crate) struct QueryResultDerive {
    ident: Ident,
    generics: syn::Generics,
    fields: Vec<QueryResultField>,
    schema_path: Path,
    model_name: LitStr,
    variables_ty: Option<Type>,
    root_filters: Vec<RootFilter>,
    root_orders: Vec<RootOrder>,
    root_skip: Option<QueryPaginationAttr>,
    root_limit: Option<QueryPaginationAttr>,
}

impl QueryResultDerive {
    pub(crate) fn parse(input: syn::DeriveInput) -> Result<Self> {
        let ident = input.ident;
        let generics = input.generics;
        let (
            schema_path,
            model_name,
            variables_ty,
            root_filters,
            root_orders,
            root_skip,
            root_limit,
        ) = parse_container_attrs(&input.attrs)?;

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
            generics,
            fields,
            schema_path,
            model_name,
            variables_ty,
            root_filters,
            root_orders,
            root_skip,
            root_limit,
        })
    }

    pub(crate) fn expand(self, config: &QueryMacroConfig) -> Result<TokenStream2> {
        let runtime_path = config.runtime_path();
        let row_path = config.row_path();
        let error_path = config.error_path();
        let ident = self.ident;
        let generics = self.generics;
        let schema_path = self.schema_path;
        let model_name = self.model_name;
        let variables_ty = self.variables_ty;
        let root_filters = self.root_filters;
        let root_orders = self.root_orders;
        let root_skip = self.root_skip;
        let root_limit = self.root_limit;
        let scalar_fields: Vec<_> = self.fields.iter().filter(|field| !field.include).collect();
        let relation_fields: Vec<_> = self.fields.iter().filter(|field| field.include).collect();
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        if (self.fields.iter().any(|field| {
            matches!(
                field.filter,
                Some(QueryResultFieldFilter::Eq { .. } | QueryResultFieldFilter::Ne { .. })
            )
        }) || root_filters
            .iter()
            .any(|filter| filter.variable().is_some())
            || root_skip
                .as_ref()
                .is_some_and(QueryPaginationAttr::is_variable)
            || root_limit
                .as_ref()
                .is_some_and(QueryPaginationAttr::is_variable))
            && variables_ty.is_none()
        {
            return Err(Error::new(
                ident.span(),
                "query filters using `eq(...)`, `in(...)`, `not(...)`, `skip = ...`, or `limit = ...` require `#[vitrail(variables = YourVariablesType)]`",
            ));
        }

        let selection_scalars = scalar_fields
            .iter()
            .map(|field| {
                let name = &field.query_name;
                quote! { #name }
            })
            .collect::<Vec<_>>();
        let selection_relations = relation_fields.iter().map(|field| {
            let name = &field.query_name;
            let nested_ty = field.nested_type().expect("include field");
            quote! {
                #runtime_path::QueryRelationSelection {
                    field: #name,
                    selection: <#nested_ty as #runtime_path::QueryModel>::selection(),
                }
            }
        });
        let selection_relation_assertions = if variables_ty.is_none() {
            relation_fields
                .iter()
                .map(|field| {
                    let nested_ty = field.nested_type().expect("include field");
                    quote! {
                        {
                            fn __vitrail_assert_query_variables_match<
                                T: #runtime_path::QueryModel<Variables = ()>,
                            >() {
                            }

                            __vitrail_assert_query_variables_match::<#nested_ty>();
                        }
                    }
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let selection_relations_with_variables = relation_fields
            .iter()
            .map(|field| {
                let name = &field.query_name;
                let nested_ty = field.nested_type().expect("include field");
                quote! {
                    #runtime_path::QueryRelationSelection {
                        field: #name,
                        selection: <#nested_ty as #runtime_path::QueryModel>::selection_with_variables(variables),
                    }
                }
            })
            .collect::<Vec<_>>();
        let order_exprs = root_orders
            .iter()
            .map(|order| order.expand(runtime_path))
            .collect::<Vec<_>>();
        let order_tokens = if order_exprs.is_empty() {
            quote! { ::std::vec![] }
        } else {
            quote! { ::std::vec![#(#order_exprs),*] }
        };

        let static_filter_exprs = {
            let mut filters = root_filters
                .iter()
                .filter(|filter| filter.variable().is_none())
                .map(|filter| filter.expand(runtime_path))
                .collect::<Vec<_>>();

            filters.extend(scalar_fields.iter().filter_map(|field| {
                let field_name = &field.query_name;
                let filter = field.filter.as_ref()?;
                match filter {
                    QueryResultFieldFilter::Eq { .. }
                    | QueryResultFieldFilter::In { .. }
                    | QueryResultFieldFilter::Ne { .. } => None,
                    QueryResultFieldFilter::IsNull => Some(quote! {
                        #runtime_path::QueryFilter::is_null(#field_name)
                    }),
                    QueryResultFieldFilter::IsNotNull => Some(quote! {
                        #runtime_path::QueryFilter::is_not_null(#field_name)
                    }),
                }
            }));

            filters
        };
        let static_filter_tokens = if static_filter_exprs.is_empty() {
            quote! { None }
        } else if static_filter_exprs.len() == 1 {
            let filter = &static_filter_exprs[0];
            quote! { Some(#filter) }
        } else {
            quote! { Some(#runtime_path::QueryFilter::And(vec![#(#static_filter_exprs),*])) }
        };
        let static_skip_tokens = root_skip
            .as_ref()
            .and_then(|pagination| pagination.expand_static_tokens(runtime_path))
            .map(|tokens| quote! { Some(#tokens) })
            .unwrap_or_else(|| quote! { None });
        let static_limit_tokens = root_limit
            .as_ref()
            .and_then(|pagination| pagination.expand_static_tokens(runtime_path))
            .map(|tokens| quote! { Some(#tokens) })
            .unwrap_or_else(|| quote! { None });
        let selection_with_variables_tokens = if variables_ty.is_some() {
            let filter_exprs = {
                let mut filters = root_filters
                    .iter()
                    .map(|filter| filter.expand(runtime_path))
                    .collect::<Vec<_>>();

                filters.extend(scalar_fields.iter().filter_map(|field| {
                    let field_name = &field.query_name;
                    let filter = field.filter.as_ref()?;
                    Some(match filter {
                        QueryResultFieldFilter::Eq { variable } => quote! {
                            #runtime_path::QueryFilter::eq(
                                #field_name,
                                #runtime_path::QueryFilterValue::variable(stringify!(#variable)),
                            )
                        },
                        QueryResultFieldFilter::In { variable } => quote! {
                            #runtime_path::QueryFilter::r#in(
                                #field_name,
                                #runtime_path::QueryFilterValues::variable(stringify!(#variable)),
                            )
                        },
                        QueryResultFieldFilter::Ne { variable } => quote! {
                            #runtime_path::QueryFilter::ne(
                                #field_name,
                                #runtime_path::QueryFilterValue::variable(stringify!(#variable)),
                            )
                        },
                        QueryResultFieldFilter::IsNull => quote! {
                            #runtime_path::QueryFilter::is_null(#field_name)
                        },
                        QueryResultFieldFilter::IsNotNull => quote! {
                            #runtime_path::QueryFilter::is_not_null(#field_name)
                        },
                    })
                }));

                filters
            };

            let filter_tokens = if filter_exprs.is_empty() {
                quote! { None }
            } else if filter_exprs.len() == 1 {
                let filter = &filter_exprs[0];
                quote! { Some(#filter) }
            } else {
                quote! { Some(#runtime_path::QueryFilter::And(vec![#(#filter_exprs),*])) }
            };
            let skip_tokens = root_skip
                .as_ref()
                .map(|pagination| pagination.expand_dynamic_tokens(runtime_path))
                .map(|tokens| quote! { Some(#tokens) })
                .unwrap_or_else(|| quote! { None });
            let limit_tokens = root_limit
                .as_ref()
                .map(|pagination| pagination.expand_dynamic_tokens(runtime_path))
                .map(|tokens| quote! { Some(#tokens) })
                .unwrap_or_else(|| quote! { None });

            quote! {
                let _ = variables;

                #runtime_path::QuerySelection {
                    model: #model_name,
                    scalar_fields: vec![#(#selection_scalars),*],
                    relations: vec![#(#selection_relations_with_variables),*],
                    filter: #filter_tokens,
                    order_by: #order_tokens,
                    skip: #skip_tokens,
                    limit: #limit_tokens,
                }
            }
        } else {
            quote! {
                let _ = variables;
                Self::selection()
            }
        };

        let root_filter_validation_tokens = {
            let schema_module_ident = schema_module_ident(&schema_path, "QueryResult")?;
            let schema_module_path = schema_module_path(&schema_path, "QueryResult")?;
            let model_ident = syn::parse_str::<Ident>(&model_name.value()).map_err(|_| {
                Error::new(
                    model_name.span(),
                    "`#[vitrail(model = ...)]` must be a valid identifier when used with `where(...)`",
                )
            })?;
            let where_path_assert_ident = format_ident!(
                "__vitrail_assert_query_where_path_{}_{}",
                schema_module_ident,
                model_ident,
            );
            let order_path_assert_ident = format_ident!(
                "__vitrail_assert_query_order_path_{}_{}",
                schema_module_ident,
                model_ident,
            );
            let where_path_assert_macro = quote! {
                #schema_module_path::#where_path_assert_ident
            };
            let order_path_assert_macro = quote! {
                #schema_module_path::#order_path_assert_ident
            };
            let mut validations = root_filters
                .iter()
                .map(|filter| filter.validation_tokens(&where_path_assert_macro))
                .collect::<Vec<_>>();
            validations.extend(
                root_orders
                    .iter()
                    .map(|order| order.validation_tokens(&order_path_assert_macro)),
            );

            quote! {
                #(#validations)*
            }
        };

        let query_type_validation_tokens = {
            let schema_module_ident = schema_module_ident(&schema_path, "QueryResult")?;
            let schema_module_path = schema_module_path(&schema_path, "QueryResult")?;
            let model_ident = syn::parse_str::<Ident>(&model_name.value()).map_err(|_| {
                Error::new(
                    model_name.span(),
                    "`#[vitrail(model = ...)]` must be a valid identifier",
                )
            })?;

            scalar_fields
                .iter()
                .map(|field| {
                    let field_ident = syn::parse_str::<Ident>(&field.query_name.value())?;
                    let field_ty = &field.ty;
                    let trait_ident = format_ident!(
                        "__VitrailQueryResultType_{}_{}_{}",
                        schema_module_ident,
                        model_ident,
                        field_ident,
                    );
                    let trait_module_ident = format_ident!(
                        "__vitrail_query_traits_{}_{}",
                        schema_module_ident,
                        model_ident,
                    );
                    Ok(quote! {
                        {
                            fn __vitrail_assert_query_result_field_type<
                                T: #schema_module_path::#trait_module_ident::#trait_ident,
                            >() {}
                            __vitrail_assert_query_result_field_type::<#field_ty>();
                        }
                    })
                })
                .collect::<Result<Vec<_>>>()?
        };

        let query_filter_type_validation_tokens = {
            let schema_module_ident = schema_module_ident(&schema_path, "QueryResult")?;
            let schema_module_path = schema_module_path(&schema_path, "QueryResult")?;
            let model_ident = syn::parse_str::<Ident>(&model_name.value()).map_err(|_| {
                Error::new(
                    model_name.span(),
                    "`#[vitrail(model = ...)]` must be a valid identifier",
                )
            })?;
            let query_filter_type_assert_ident = format_ident!(
                "__vitrail_assert_query_filter_value_type_{}_{}",
                schema_module_ident,
                model_ident,
            );
            let query_filter_type_assert_macro = quote! {
                #schema_module_path::#query_filter_type_assert_ident
            };

            let mut validations = root_filters
                .iter()
                .filter_map(|filter| filter.type_validation_tokens(&query_filter_type_assert_macro))
                .collect::<Vec<_>>();

            validations.extend(
                scalar_fields
                    .iter()
                    .filter_map(|field| {
                        let field_ident = match syn::parse_str::<Ident>(&field.query_name.value()) {
                            Ok(field_ident) => field_ident,
                            Err(error) => return Some(Err(error)),
                        };
                        let filter = field.filter.as_ref()?;

                        Some(Ok(match filter {
                            QueryResultFieldFilter::Eq { variable } => quote! {
                                #query_filter_type_assert_macro!(#field_ident, eq, &__vitrail_variables.#variable);
                            },
                            QueryResultFieldFilter::In { variable } => quote! {
                                #query_filter_type_assert_macro!(#field_ident, in, &__vitrail_variables.#variable);
                            },
                            QueryResultFieldFilter::Ne { variable } => quote! {
                                #query_filter_type_assert_macro!(#field_ident, not, &__vitrail_variables.#variable);
                            },
                            QueryResultFieldFilter::IsNull
                            | QueryResultFieldFilter::IsNotNull => quote! {},
                        }))
                    })
                    .collect::<Result<Vec<_>>>()?,
            );

            validations
        };

        let variable_validation_tokens = if let Some(variables_ty) = &variables_ty {
            let mut validations = root_filters
                .iter()
                .filter_map(RootFilter::variable)
                .collect::<Vec<_>>();

            validations.extend(scalar_fields.iter().filter_map(|field| {
                let filter = field.filter.as_ref()?;
                match filter {
                    QueryResultFieldFilter::Eq { variable }
                    | QueryResultFieldFilter::In { variable }
                    | QueryResultFieldFilter::Ne { variable } => Some(variable),
                    QueryResultFieldFilter::IsNull | QueryResultFieldFilter::IsNotNull => None,
                }
            }));

            let pagination_validations = [root_skip.as_ref(), root_limit.as_ref()]
                .into_iter()
                .flatten()
                .filter_map(QueryPaginationAttr::variable)
                .collect::<Vec<_>>();
            let pagination_type_validations = pagination_validations.iter().map(|variable| {
                quote! {
                    {
                        fn __vitrail_assert_query_pagination_value_type(_: &i64) {}
                        __vitrail_assert_query_pagination_value_type(&__vitrail_variables.#variable);
                    }
                }
            });

            quote! {
                impl #impl_generics #ident #ty_generics
                #where_clause
                {
                    #[doc(hidden)]
                    fn __vitrail_validate_query(__vitrail_variables: Option<&#variables_ty>) {
                        #root_filter_validation_tokens
                        #(#query_type_validation_tokens)*
                        if let Some(__vitrail_variables) = __vitrail_variables {
                            #(let _ = &__vitrail_variables.#validations;)*
                            #(#query_filter_type_validation_tokens)*
                            #(#pagination_type_validations)*
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
                    fn __vitrail_validate_query() {
                        #root_filter_validation_tokens
                        #(#query_type_validation_tokens)*
                    }
                }
            }
        };

        let query_variables_ty = variables_ty
            .as_ref()
            .map(|variables_ty| quote! { #variables_ty })
            .unwrap_or_else(|| quote! { () });

        let decode_fields = self
            .fields
            .iter()
            .map(|field| {
                let ident = &field.ident;
                let field_name = &field.query_name;
                let field_ty = &field.ty;

                if field.include {
                    let nested_ty = field.nested_type().expect("include field");
                    let decode_relation = field.decode_relation_tokens(&nested_ty, runtime_path);
                    quote! {
                        #ident: {
                            #decode_relation
                        }
                    }
                } else {
                    quote! {
                        #ident: {
                            let __vitrail_alias = #runtime_path::alias_name(prefix, #field_name);
                            #runtime_path::row_value::<#field_ty>(row, __vitrail_alias.as_str())?
                        }
                    }
                }
            })
            .collect::<Vec<_>>();
        // SQL builders encode nested JSON rows with scalar fields first, followed by relations.
        let json_decode_fields = scalar_fields
            .iter()
            .chain(&relation_fields)
            .enumerate()
            .map(|(index, field)| field.decode_json_field_tokens(index, runtime_path))
            .collect::<Result<Vec<_>>>()?;

        Ok(quote! {
            #variable_validation_tokens

            impl #impl_generics #runtime_path::QueryValue for #ident #ty_generics
            #where_clause
            {
                fn from_json(
                    value: &#runtime_path::serde_json::Value,
                ) -> Result<Self, #error_path> {
                    Ok(Self {
                        #(#json_decode_fields),*
                    })
                }
            }

            impl #impl_generics #runtime_path::QueryModel for #ident #ty_generics
            #where_clause
            {
                type Schema = #schema_path;
                type Variables = #query_variables_ty;

                fn model_name() -> &'static str {
                    #model_name
                }

                fn selection() -> #runtime_path::QuerySelection {
                    #(#selection_relation_assertions)*

                    #runtime_path::QuerySelection {
                        model: #model_name,
                        scalar_fields: vec![#(#selection_scalars),*],
                        relations: vec![#(#selection_relations),*],
                        filter: #static_filter_tokens,
                        order_by: #order_tokens,
                        skip: #static_skip_tokens,
                        limit: #static_limit_tokens,
                    }
                }

                fn selection_with_variables(
                    variables: &#runtime_path::QueryVariables,
                ) -> #runtime_path::QuerySelection {
                    #selection_with_variables_tokens
                }

                fn from_row(
                    row: &#row_path,
                    prefix: &str,
                ) -> Result<Self, #error_path> {
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
    filter: Option<QueryResultFieldFilter>,
}

enum QueryResultFieldFilter {
    Eq { variable: Ident },
    In { variable: Ident },
    Ne { variable: Ident },
    IsNull,
    IsNotNull,
}

impl QueryResultField {
    fn parse(field: syn::Field) -> Result<Self> {
        let span = field.span();
        let ident = field
            .ident
            .ok_or_else(|| Error::new(span, "expected a named field"))?;
        let mut include = false;
        let mut rename = None;
        let mut filter = None;

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
                if meta.path.is_ident("where") {
                    let content;
                    parenthesized!(content in meta.input);
                    let operator = content.call(Ident::parse_any)?;

                    filter = if operator == "eq" {
                        content.parse::<Token![=]>()?;
                        let variable = content.call(Ident::parse_any)?;

                        if !content.is_empty() {
                            return Err(Error::new(
                                content.span(),
                                "unexpected tokens in `where(...)`",
                            ));
                        }

                        Some(QueryResultFieldFilter::Eq { variable })
                    } else if operator == "in" {
                        content.parse::<Token![=]>()?;
                        let variable = content.call(Ident::parse_any)?;

                        if !content.is_empty() {
                            return Err(Error::new(
                                content.span(),
                                "unexpected tokens in `where(...)`",
                            ));
                        }

                        Some(QueryResultFieldFilter::In { variable })
                    } else if operator == "null" {
                        if !content.is_empty() {
                            return Err(Error::new(
                                content.span(),
                                "unexpected tokens in `where(null)`",
                            ));
                        }

                        Some(QueryResultFieldFilter::IsNull)
                    } else if operator == "not" {
                        let operator_args;
                        parenthesized!(operator_args in content);
                        let value = operator_args.call(Ident::parse_any)?;

                        if !operator_args.is_empty() {
                            return Err(Error::new(
                                operator_args.span(),
                                "unexpected tokens in `where(not(...))`",
                            ));
                        }

                        if !content.is_empty() {
                            return Err(Error::new(
                                content.span(),
                                "unexpected tokens in `where(not(...))`",
                            ));
                        }

                        if value == "null" {
                            Some(QueryResultFieldFilter::IsNotNull)
                        } else {
                            Some(QueryResultFieldFilter::Ne { variable: value })
                        }
                    } else {
                        return Err(Error::new(
                            operator.span(),
                            "unsupported `where` operator; only `eq`, `in`, `null`, and `not(...)` are currently supported",
                        ));
                    };

                    return Ok(());
                }
                Err(meta.error("unsupported `#[vitrail(...)]` field attribute"))
            })?;
        }

        if include && filter.is_some() {
            return Err(Error::new(
                ident.span(),
                "relation fields do not support `where(...)`; place filters on the nested query model instead",
            ));
        }

        let query_name = rename.unwrap_or_else(|| LitStr::new(&ident.to_string(), ident.span()));

        Ok(Self {
            ident,
            ty: field.ty,
            query_name,
            include,
            filter,
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

    fn decode_relation_tokens(
        &self,
        nested_ty: &TokenStream2,
        runtime_path: &Path,
    ) -> TokenStream2 {
        let field_name = &self.query_name;

        if option_inner_type(&self.ty).is_some() {
            quote! {
                {
                    let __vitrail_alias = #runtime_path::alias_name(prefix, #field_name);
                    let __vitrail_value =
                        #runtime_path::row_optional_relation_json(
                            row,
                            __vitrail_alias.as_str(),
                        )?;
                    __vitrail_value
                        .as_ref()
                        .map(<#nested_ty as #runtime_path::QueryValue>::from_json)
                        .transpose()?
                }
            }
        } else if vec_inner_type(&self.ty).is_some() {
            quote! {
                {
                    let __vitrail_alias = #runtime_path::alias_name(prefix, #field_name);
                    let __vitrail_value =
                        #runtime_path::row_relation_json(row, __vitrail_alias.as_str())?;
                    let __vitrail_items = __vitrail_value.as_array().ok_or_else(|| {
                        #runtime_path::schema_error(
                            "expected JSON array in query result".to_owned(),
                        )
                    })?;
                    let mut __vitrail_values = Vec::with_capacity(__vitrail_items.len());
                    for __vitrail_item in __vitrail_items {
                        __vitrail_values.push(
                            <#nested_ty as #runtime_path::QueryValue>::from_json(
                                __vitrail_item,
                            )?,
                        );
                    }
                    __vitrail_values
                }
            }
        } else {
            quote! {
                {
                    let __vitrail_alias = #runtime_path::alias_name(prefix, #field_name);
                    let __vitrail_value =
                        #runtime_path::row_relation_json(row, __vitrail_alias.as_str())?;
                    <#nested_ty as #runtime_path::QueryValue>::from_json(&__vitrail_value)?
                }
            }
        }
    }

    fn decode_json_field_tokens(
        &self,
        json_index: usize,
        runtime_path: &Path,
    ) -> Result<TokenStream2> {
        let ident = &self.ident;
        let json_index = syn::Index::from(json_index);

        if self.include {
            let nested_ty = self.nested_type().expect("include field");
            if option_inner_type(&self.ty).is_some() {
                Ok(quote! {
                    #ident: {
                        let __vitrail_value =
                            #runtime_path::json_array_field(value, #json_index)?;
                        if __vitrail_value.is_null() {
                            None
                        } else {
                            Some(
                                <#nested_ty as #runtime_path::QueryValue>::from_json(
                                    __vitrail_value,
                                )?,
                            )
                        }
                    }
                })
            } else if vec_inner_type(&self.ty).is_some() {
                Ok(quote! {
                    #ident: {
                        let __vitrail_value =
                            #runtime_path::json_array_field(value, #json_index)?;
                        let __vitrail_items = __vitrail_value.as_array().ok_or_else(|| {
                            #runtime_path::schema_error(
                                "expected JSON array in query result".to_owned(),
                            )
                        })?;
                        let mut __vitrail_values = Vec::with_capacity(__vitrail_items.len());
                        for __vitrail_item in __vitrail_items {
                            __vitrail_values.push(
                                <#nested_ty as #runtime_path::QueryValue>::from_json(
                                    __vitrail_item,
                                )?,
                            );
                        }
                        __vitrail_values
                    }
                })
            } else {
                Ok(quote! {
                    #ident: {
                        let __vitrail_value =
                            #runtime_path::json_array_field(value, #json_index)?;
                        <#nested_ty as #runtime_path::QueryValue>::from_json(__vitrail_value)?
                    }
                })
            }
        } else {
            let decode = json_decode_tokens_for_type(
                &self.ty,
                quote! { #runtime_path::json_array_field(value, #json_index)? },
                runtime_path,
            )?;
            Ok(quote! {
                #ident: { #decode }
            })
        }
    }
}

fn parse_container_attrs(attrs: &[Attribute]) -> Result<QueryResultContainerAttrs> {
    let mut schema_path = None;
    let mut model_name = None;
    let mut variables_ty = None;
    let mut root_filters = Vec::new();
    let mut root_orders = Vec::new();
    let mut root_skip = None;
    let mut root_limit = None;

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
            if meta.path.is_ident("order_by") {
                root_orders.extend(parse_root_orders(meta.input)?);
                return Ok(());
            }
            if meta.path.is_ident("skip") {
                root_skip = Some(parse_query_pagination_attr(meta.value()?)?);
                return Ok(());
            }
            if meta.path.is_ident("limit") {
                root_limit = Some(parse_query_pagination_attr(meta.value()?)?);
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

    Ok((
        schema_path,
        model_name,
        variables_ty,
        root_filters,
        root_orders,
        root_skip,
        root_limit,
    ))
}

enum QueryPaginationAttr {
    Value(i64),
    Variable(Ident),
}

impl QueryPaginationAttr {
    fn is_variable(&self) -> bool {
        matches!(self, Self::Variable(_))
    }

    fn variable(&self) -> Option<&Ident> {
        match self {
            Self::Value(_) => None,
            Self::Variable(variable) => Some(variable),
        }
    }

    fn expand_static_tokens(&self, runtime_path: &Path) -> Option<TokenStream2> {
        match self {
            Self::Value(value) => Some(quote! { #runtime_path::QueryPagination::value(#value) }),
            Self::Variable(_) => None,
        }
    }

    fn expand_dynamic_tokens(&self, runtime_path: &Path) -> TokenStream2 {
        match self {
            Self::Value(value) => {
                quote! { #runtime_path::QueryPagination::value(#value) }
            }
            Self::Variable(variable) => {
                let variable_name = variable.to_string();
                quote! { #runtime_path::QueryPagination::variable(#variable_name) }
            }
        }
    }
}

fn parse_query_pagination_attr(input: ParseStream<'_>) -> Result<QueryPaginationAttr> {
    if input.peek(syn::LitInt) {
        let value = input.parse::<syn::LitInt>()?.base10_parse::<i64>()?;
        return Ok(QueryPaginationAttr::Value(value));
    }

    Ok(QueryPaginationAttr::Variable(input.call(Ident::parse_any)?))
}

pub(crate) struct QueryVariablesDerive {
    ident: Ident,
    generics: syn::Generics,
    fields: Vec<(Ident, Type)>,
}

impl QueryVariablesDerive {
    pub(crate) fn parse(input: syn::DeriveInput) -> Result<Self> {
        let ident = input.ident;
        let generics = input.generics;

        let Data::Struct(DataStruct {
            fields: Fields::Named(fields),
            ..
        }) = input.data
        else {
            return Err(Error::new(
                ident.span(),
                "`QueryVariables` can only be derived for structs with named fields",
            ));
        };

        let fields = fields
            .named
            .into_iter()
            .map(|field| {
                let span = field.span();
                let ident = field
                    .ident
                    .ok_or_else(|| Error::new(span, "expected a named field"))?;
                Ok((ident, field.ty))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            ident,
            generics,
            fields,
        })
    }

    pub(crate) fn expand(self, config: &QueryMacroConfig) -> Result<TokenStream2> {
        let runtime_path = config.runtime_path();
        let ident = self.ident;
        let mut generics = self.generics;
        let fields = self.fields;

        for (_, field_ty) in &fields {
            generics
                .make_where_clause()
                .predicates
                .push(syn::parse_quote!(#field_ty: #runtime_path::QueryScalar));
        }

        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        let named_values = fields.iter().map(|(field, _)| {
            let name = field.to_string();
            quote! {
                (
                    #name,
                    #runtime_path::QueryScalar::into_query_variable_value(self.#field),
                )
            }
        });

        Ok(quote! {
            impl #impl_generics #runtime_path::QueryVariableSet for #ident #ty_generics
            #where_clause
            {
                fn into_query_variables(self) -> #runtime_path::QueryVariables {
                    #runtime_path::QueryVariables::from_values(vec![#(#named_values),*])
                }
            }
        })
    }
}

fn schema_module_path(schema_path: &Path, derive_name: &str) -> Result<Path> {
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

fn schema_module_ident(schema_path: &Path, derive_name: &str) -> Result<Ident> {
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

fn json_decode_tokens_for_type(
    value_ty: &Type,
    value_expr: TokenStream2,
    runtime_path: &Path,
) -> Result<TokenStream2> {
    if let Some(inner) = option_inner_type(value_ty) {
        let inner_decode =
            json_decode_tokens_for_type(&inner, quote! { __vitrail_value }, runtime_path)?;
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

    Ok(quote! { #runtime_path::json_value::<#value_ty>(#value_expr)? })
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    fn custom_config() -> QueryMacroConfig {
        QueryMacroConfig::new(
            syn::parse_quote!(::custom_facade),
            syn::parse_quote!(::custom_backend::CustomRow),
            syn::parse_quote!(::custom_backend::CustomError),
        )
    }

    #[test]
    fn query_result_expansion_uses_configured_facade_and_row_paths() {
        let input = syn::parse2(quote! {
            #[vitrail(
                schema = crate::custom_schema::Schema,
                model = user,
                variables = Variables,
                where(profile.email = eq(email)),
                order_by(profile.id = desc),
                skip = skip,
                limit = 10
            )]
            struct UserQuery {
                id: i64,
                #[vitrail(include)]
                profile: Option<ProfileQuery>,
                email: String,
                #[vitrail(include)]
                posts: Vec<PostQuery>,
            }
        })
        .expect("query result input should parse");

        let generated = expand_query_result(input, &custom_config())
            .expect("query result should expand")
            .to_string();

        for expected in [
            "custom_facade :: QueryModel",
            "custom_facade :: QueryValue",
            "custom_facade :: QueryFilter :: relation",
            "custom_facade :: QueryOrder :: relation",
            "custom_facade :: QueryPagination :: variable",
            "custom_facade :: QueryPagination :: value",
            "custom_facade :: row_optional_relation_json",
            "custom_facade :: row_relation_json",
            "custom_backend :: CustomRow",
            "custom_backend :: CustomError",
        ] {
            assert!(
                generated.contains(expected),
                "generated query result is missing `{expected}`"
            );
        }

        let from_json_start = generated
            .find("fn from_json")
            .expect("query result expansion should contain a JSON decoder");
        let query_model_start = generated[from_json_start..]
            .find("custom_facade :: QueryModel")
            .map(|offset| from_json_start + offset)
            .expect("query result expansion should contain a query model implementation");
        let from_json = &generated[from_json_start..query_model_start];
        let field_positions = ["id :", "email :", "profile :", "posts :"].map(|field| {
            from_json
                .find(field)
                .unwrap_or_else(|| panic!("JSON decoder is missing field `{field}`"))
        });

        for positions in field_positions.windows(2) {
            assert!(
                positions[0] < positions[1],
                "nested JSON fields should decode scalars before relations: {from_json}"
            );
        }

        assert!(
            !generated.contains("sqlx"),
            "generated query result unexpectedly depends on SQLx"
        );

        for hardcoded_facade in ["vitrail_pg", "vitrail_sqlite"] {
            assert!(
                !generated.contains(hardcoded_facade),
                "generated query result leaked `{hardcoded_facade}`"
            );
        }
    }

    #[test]
    fn query_variables_expansion_uses_configured_facade_path() {
        let input = syn::parse2(quote! {
            struct Variables {
                email: String,
                skip: i64,
            }
        })
        .expect("query variables input should parse");

        let generated = expand_query_variables(input, &custom_config())
            .expect("query variables should expand")
            .to_string();

        for expected in [
            "custom_facade :: QueryScalar",
            "custom_facade :: QueryVariableSet",
            "custom_facade :: QueryVariables",
        ] {
            assert!(
                generated.contains(expected),
                "generated query variables are missing `{expected}`"
            );
        }

        assert!(!generated.contains("custom_backend"));
        assert!(!generated.contains("vitrail_pg"));
        assert!(!generated.contains("vitrail_sqlite"));
    }
}
