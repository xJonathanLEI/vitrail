use proc_macro2::{Ident, Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::ext::IdentExt;
use syn::parse::ParseStream;
use syn::spanned::Spanned;
use syn::{
    Attribute, Data, DataStruct, Error, Fields, LitStr, Path, Result, Token, Type, parenthesized,
};

pub(crate) struct QueryResultDerive {
    ident: Ident,
    generics: syn::Generics,
    fields: Vec<QueryResultField>,
    schema_path: Path,
    model_name: LitStr,
    variables_ty: Option<Type>,
    root_filters: Vec<QueryResultRootFilter>,
}

impl QueryResultDerive {
    pub(crate) fn parse(input: syn::DeriveInput) -> Result<Self> {
        let ident = input.ident;
        let generics = input.generics;
        let (schema_path, model_name, variables_ty, root_filters) =
            parse_container_attrs(&input.attrs)?;

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
        })
    }

    pub(crate) fn expand(self) -> Result<TokenStream2> {
        let ident = self.ident;
        let generics = self.generics;
        let schema_path = self.schema_path;
        let model_name = self.model_name;
        let variables_ty = self.variables_ty;
        let root_filters = self.root_filters;
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
            .any(|filter| filter.variable().is_some()))
            && variables_ty.is_none()
        {
            return Err(Error::new(
                ident.span(),
                "query filters using `eq(...)` or `not(...)` require `#[vitrail(variables = YourVariablesType)]`",
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
                ::vitrail_pg::QueryRelationSelection {
                    field: #name,
                    selection: <#nested_ty as ::vitrail_pg::QueryModel>::selection(),
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
                                T: ::vitrail_pg::QueryModel<Variables = ()>,
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
                    ::vitrail_pg::QueryRelationSelection {
                        field: #name,
                        selection: <#nested_ty as ::vitrail_pg::QueryModel>::selection_with_variables(variables),
                    }
                }
            })
            .collect::<Vec<_>>();
        let static_filter_exprs = {
            let mut filters = root_filters
                .iter()
                .filter(|filter| filter.variable().is_none())
                .map(QueryResultRootFilter::expand)
                .collect::<Vec<_>>();

            filters.extend(scalar_fields.iter().filter_map(|field| {
                let field_name = &field.query_name;
                let filter = field.filter.as_ref()?;
                match filter {
                    QueryResultFieldFilter::Eq { .. } | QueryResultFieldFilter::Ne { .. } => None,
                    QueryResultFieldFilter::IsNull => Some(quote! {
                        ::vitrail_pg::QueryFilter::is_null(#field_name)
                    }),
                    QueryResultFieldFilter::IsNotNull => Some(quote! {
                        ::vitrail_pg::QueryFilter::is_not_null(#field_name)
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
            quote! { Some(::vitrail_pg::QueryFilter::And(vec![#(#static_filter_exprs),*])) }
        };
        let selection_with_variables_tokens = if variables_ty.is_some() {
            let filter_exprs = {
                let mut filters = root_filters
                    .iter()
                    .map(QueryResultRootFilter::expand)
                    .collect::<Vec<_>>();

                filters.extend(scalar_fields.iter().filter_map(|field| {
                    let field_name = &field.query_name;
                    let filter = field.filter.as_ref()?;
                    Some(match filter {
                        QueryResultFieldFilter::Eq { variable } => quote! {
                            ::vitrail_pg::QueryFilter::eq(
                                #field_name,
                                ::vitrail_pg::QueryFilterValue::variable(stringify!(#variable)),
                            )
                        },
                        QueryResultFieldFilter::Ne { variable } => quote! {
                            ::vitrail_pg::QueryFilter::ne(
                                #field_name,
                                ::vitrail_pg::QueryFilterValue::variable(stringify!(#variable)),
                            )
                        },
                        QueryResultFieldFilter::IsNull => quote! {
                            ::vitrail_pg::QueryFilter::is_null(#field_name)
                        },
                        QueryResultFieldFilter::IsNotNull => quote! {
                            ::vitrail_pg::QueryFilter::is_not_null(#field_name)
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
                quote! { Some(::vitrail_pg::QueryFilter::And(vec![#(#filter_exprs),*])) }
            };

            quote! {
                let _ = variables;

                ::vitrail_pg::QuerySelection {
                    model: #model_name,
                    scalar_fields: vec![#(#selection_scalars),*],
                    relations: vec![#(#selection_relations_with_variables),*],
                    filter: #filter_tokens,
                }
            }
        } else {
            quote! {
                let _ = variables;
                Self::selection()
            }
        };

        let root_filter_validation_tokens = {
            let schema_module_ident = schema_path
                .segments
                .iter()
                .rev()
                .nth(1)
                .map(|segment| segment.ident.clone())
                .ok_or_else(|| {
                    Error::new(
                        schema_path.span(),
                        "`#[vitrail(schema = ...)]` must point to a schema type like `crate::my_schema::Schema` when used with `where(...)`",
                    )
                })?;
            let model_ident = syn::parse_str::<Ident>(&model_name.value()).map_err(|_| {
                Error::new(
                    model_name.span(),
                    "`#[vitrail(model = ...)]` must be a valid identifier when used with `where(...)`",
                )
            })?;
            let validations = root_filters
                .iter()
                .map(|filter| filter.validation_tokens(&schema_module_ident, &model_ident))
                .collect::<Vec<_>>();

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

        let variable_validation_tokens = if let Some(variables_ty) = &variables_ty {
            let mut validations = root_filters
                .iter()
                .filter_map(QueryResultRootFilter::variable)
                .collect::<Vec<_>>();

            validations.extend(scalar_fields.iter().filter_map(|field| {
                let filter = field.filter.as_ref()?;
                match filter {
                    QueryResultFieldFilter::Eq { variable }
                    | QueryResultFieldFilter::Ne { variable } => Some(variable),
                    QueryResultFieldFilter::IsNull | QueryResultFieldFilter::IsNotNull => None,
                }
            }));

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
                    let decode_relation = field.decode_relation_tokens(&nested_ty);
                    quote! {
                        #ident: {
                            #decode_relation
                        }
                    }
                } else {
                    quote! {
                        #ident: {
                            let __vitrail_alias = ::vitrail_pg::alias_name(prefix, #field_name);
                            ::vitrail_pg::row_value::<#field_ty>(row, __vitrail_alias.as_str())?
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
            #variable_validation_tokens

            impl #impl_generics ::vitrail_pg::QueryValue for #ident #ty_generics
            #where_clause
            {
                fn from_json(value: &::vitrail_pg::serde_json::Value) -> Result<Self, ::sqlx::Error> {
                    Ok(Self {
                        #(#json_decode_fields),*
                    })
                }
            }

            impl #impl_generics ::vitrail_pg::QueryModel for #ident #ty_generics
            #where_clause
            {
                type Schema = #schema_path;
                type Variables = #query_variables_ty;

                fn model_name() -> &'static str {
                    #model_name
                }

                fn selection() -> ::vitrail_pg::QuerySelection {
                    #(#selection_relation_assertions)*

                    ::vitrail_pg::QuerySelection {
                        model: #model_name,
                        scalar_fields: vec![#(#selection_scalars),*],
                        relations: vec![#(#selection_relations),*],
                        filter: #static_filter_tokens,
                    }
                }

                fn selection_with_variables(
                    variables: &::vitrail_pg::QueryVariables,
                ) -> ::vitrail_pg::QuerySelection {
                    #selection_with_variables_tokens
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
    filter: Option<QueryResultFieldFilter>,
}

enum QueryResultFieldFilter {
    Eq { variable: Ident },
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
                            "unsupported `where` operator; only `eq`, `null`, and `not(...)` are currently supported",
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

fn parse_container_attrs(
    attrs: &[Attribute],
) -> Result<(Path, LitStr, Option<Type>, Vec<QueryResultRootFilter>)> {
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
                root_filters.push(parse_query_result_root_filter(meta.input)?);
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

    Ok((schema_path, model_name, variables_ty, root_filters))
}

struct QueryResultRootFilter {
    field: LitStr,
    filter: QueryResultFieldFilter,
}

impl QueryResultRootFilter {
    fn expand(&self) -> TokenStream2 {
        let field_name = &self.field;

        match &self.filter {
            QueryResultFieldFilter::Eq { variable } => quote! {
                ::vitrail_pg::QueryFilter::eq(
                    #field_name,
                    ::vitrail_pg::QueryFilterValue::variable(stringify!(#variable)),
                )
            },
            QueryResultFieldFilter::Ne { variable } => quote! {
                ::vitrail_pg::QueryFilter::ne(
                    #field_name,
                    ::vitrail_pg::QueryFilterValue::variable(stringify!(#variable)),
                )
            },
            QueryResultFieldFilter::IsNull => quote! {
                ::vitrail_pg::QueryFilter::is_null(#field_name)
            },
            QueryResultFieldFilter::IsNotNull => quote! {
                ::vitrail_pg::QueryFilter::is_not_null(#field_name)
            },
        }
    }

    fn validation_tokens(&self, schema_module_ident: &Ident, model_ident: &Ident) -> TokenStream2 {
        let field_ident = syn::parse_str::<Ident>(&self.field.value())
            .expect("root filter fields are parsed as identifiers");
        let where_assert_macro_ident = format_ident!(
            "__vitrail_assert_where_{}_{}",
            schema_module_ident,
            model_ident,
            span = self.field.span()
        );

        quote! {
            #where_assert_macro_ident!(#field_ident);
        }
    }

    fn variable(&self) -> Option<&Ident> {
        match &self.filter {
            QueryResultFieldFilter::Eq { variable } | QueryResultFieldFilter::Ne { variable } => {
                Some(variable)
            }
            QueryResultFieldFilter::IsNull | QueryResultFieldFilter::IsNotNull => None,
        }
    }
}

fn parse_query_result_root_filter(input: ParseStream<'_>) -> Result<QueryResultRootFilter> {
    let content;
    parenthesized!(content in input);

    let field = content.call(Ident::parse_any)?;
    content.parse::<Token![=]>()?;
    let operator = content.call(Ident::parse_any)?;

    let filter = if operator == "eq" {
        let operator_args;
        parenthesized!(operator_args in content);
        let variable = operator_args.call(Ident::parse_any)?;

        if !operator_args.is_empty() {
            return Err(Error::new(
                operator_args.span(),
                "unexpected tokens in `where(... = eq(...))`",
            ));
        }

        QueryResultFieldFilter::Eq { variable }
    } else if operator == "null" {
        QueryResultFieldFilter::IsNull
    } else if operator == "not" {
        let operator_args;
        parenthesized!(operator_args in content);
        let value = operator_args.call(Ident::parse_any)?;

        if !operator_args.is_empty() {
            return Err(Error::new(
                operator_args.span(),
                "unexpected tokens in `where(... = not(...))`",
            ));
        }

        if value == "null" {
            QueryResultFieldFilter::IsNotNull
        } else {
            QueryResultFieldFilter::Ne { variable: value }
        }
    } else {
        return Err(Error::new(
            operator.span(),
            "unsupported `where` operator; only `eq`, `null`, and `not(...)` are currently supported",
        ));
    };

    if !content.is_empty() {
        return Err(Error::new(
            content.span(),
            "unexpected tokens in `where(...)`",
        ));
    }

    Ok(QueryResultRootFilter {
        field: LitStr::new(&field.to_string(), field.span()),
        filter,
    })
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

    pub(crate) fn expand(self) -> Result<TokenStream2> {
        let ident = self.ident;
        let mut generics = self.generics;
        let fields = self.fields;

        for (_, field_ty) in &fields {
            generics
                .make_where_clause()
                .predicates
                .push(syn::parse_quote!(#field_ty: ::vitrail_pg::QueryScalar));
        }

        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        let named_values = fields.iter().map(|(field, _)| {
            let name = field.to_string();
            quote! { (#name, ::vitrail_pg::QueryScalar::into_query_variable_value(self.#field)) }
        });

        Ok(quote! {
            impl #impl_generics ::vitrail_pg::QueryVariableSet for #ident #ty_generics
            #where_clause
            {
                fn into_query_variables(self) -> ::vitrail_pg::QueryVariables {
                    ::vitrail_pg::QueryVariables::from_values(vec![#(#named_values),*])
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

    Ok(quote! { ::vitrail_pg::json_value::<#value_ty>(#value_expr)? })
}
