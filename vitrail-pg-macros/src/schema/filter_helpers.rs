use proc_macro2::{Ident, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{LitStr, Result};

use super::{ParsedModel, ParsedSchema, to_pascal_case};

pub(super) fn generate_filter_helper_items(
    schema: &ParsedSchema,
    module_name: &Ident,
    model: &ParsedModel,
    operation: &str,
    where_path_assert_ident: &Ident,
    where_filter_macro_ident: &Ident,
    where_field_filter_macro_ident: &Ident,
) -> Result<TokenStream2> {
    let model_name = LitStr::new(&model.name.to_string(), model.name.span());
    let scalar_fields = model.scalar_fields();
    let relation_fields = model.relation_fields();
    let operation_display = LitStr::new(operation, model.name.span());

    let scalar_where_path_arms = scalar_fields.iter().map(|field| {
        let ident = &field.name;
        quote! {
            (#ident) => {};
            (#ident . $($rest:ident).+) => {
                compile_error!(concat!(
                    "scalar field `",
                    stringify!(#ident),
                    "` cannot be traversed in ",
                    #operation_display,
                    " `where(...)` for model `",
                    #model_name,
                    "`"
                ));
            };
        }
    });

    let relation_where_path_arms = relation_fields
        .iter()
        .map(|field| {
            let ident = &field.name;
            let target = schema
                .models
                .iter()
                .find(|candidate| {
                    candidate.name == field.ty.name
                        || field.ty.name == to_pascal_case(&candidate.name.to_string())
                })
                .expect("validated relation target");
            let target_where_path_assert_ident = format_ident!(
                "__vitrail_assert_{}_where_path_{}_{}",
                operation,
                module_name,
                target.name
            );

            Ok(quote! {
                (#ident) => {
                    compile_error!(concat!(
                        "relation field `",
                        stringify!(#ident),
                        "` cannot terminate a ",
                        #operation_display,
                        " `where(...)` path for model `",
                        #model_name,
                        "`"
                    ));
                };
                (#ident . $($rest:ident).+) => {
                    #target_where_path_assert_ident!($($rest).+);
                };
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let scalar_where_field_arms = scalar_fields.iter().map(|field| {
        let ident = &field.name;

        quote! {
            (#ident : null) => {
                ::vitrail_pg::QueryFilter::is_null(stringify!(#ident))
            };
            (#ident : { eq : $value:expr $(,)? }) => {
                ::vitrail_pg::QueryFilter::eq(
                    stringify!(#ident),
                    ::vitrail_pg::QueryFilterValue::value($value),
                )
            };
            (#ident : { in : $value:expr $(,)? }) => {
                ::vitrail_pg::QueryFilter::r#in(
                    stringify!(#ident),
                    ::vitrail_pg::QueryFilterValues::from($value),
                )
            };
            (#ident : { not : null $(,)? }) => {
                ::vitrail_pg::QueryFilter::is_not_null(stringify!(#ident))
            };
            (#ident : { not : $value:expr $(,)? }) => {
                ::vitrail_pg::QueryFilter::ne(
                    stringify!(#ident),
                    ::vitrail_pg::QueryFilterValue::value($value),
                )
            };
            (#ident : { $operator:ident : $value:tt $(,)? }) => {{
                compile_error!(concat!(
                    "unsupported `where` operator `",
                    stringify!($operator),
                    "` for scalar field `",
                    stringify!(#ident),
                    "` in ",
                    #operation_display,
                    " helper for model `",
                    #model_name,
                    "`; only `eq`, `in`, `null`, and `{ not: ... }` are currently supported"
                ))
            }};
            (#ident : $value:tt) => {{
                compile_error!(concat!(
                    "malformed filter for scalar field `",
                    stringify!(#ident),
                    "` in ",
                    #operation_display,
                    " helper for model `",
                    #model_name,
                    "`; expected `null`, `{ eq: ... }`, `{ in: ... }`, or `{ not: ... }`"
                ))
            }};
        }
    });

    let relation_where_field_arms = relation_fields
        .iter()
        .map(|field| {
            let ident = &field.name;
            let target = schema
                .models
                .iter()
                .find(|candidate| {
                    candidate.name == field.ty.name
                        || field.ty.name == to_pascal_case(&candidate.name.to_string())
                })
                .expect("validated relation target");
            let target_where_filter_macro_ident = format_ident!(
                "__vitrail_{}_where_filter_{}_{}",
                operation,
                module_name,
                target.name
            );

            Ok(quote! {
                (#ident : { }) => {{
                    compile_error!(concat!(
                        "relation field `",
                        stringify!(#ident),
                        "` in ",
                        #operation_display,
                        " helper for model `",
                        #model_name,
                        "` requires a nested filter object"
                    ))
                }};
                (#ident : null) => {{
                    compile_error!(concat!(
                        "relation field `",
                        stringify!(#ident),
                        "` in ",
                        #operation_display,
                        " helper for model `",
                        #model_name,
                        "` cannot use scalar null filter; provide a nested filter object instead"
                    ))
                }};
                (#ident : { eq : $value:expr $(,)? }) => {{
                    compile_error!(concat!(
                        "relation field `",
                        stringify!(#ident),
                        "` in ",
                        #operation_display,
                        " helper for model `",
                        #model_name,
                        "` cannot use scalar operator `eq`; provide a nested filter object instead"
                    ))
                }};
                (#ident : { in : $value:expr $(,)? }) => {{
                    compile_error!(concat!(
                        "relation field `",
                        stringify!(#ident),
                        "` in ",
                        #operation_display,
                        " helper for model `",
                        #model_name,
                        "` cannot use scalar operator `in`; provide a nested filter object instead"
                    ))
                }};
                (#ident : { not : null $(,)? }) => {{
                    compile_error!(concat!(
                        "relation field `",
                        stringify!(#ident),
                        "` in ",
                        #operation_display,
                        " helper for model `",
                        #model_name,
                        "` cannot use scalar null filter; provide a nested filter object instead"
                    ))
                }};
                (#ident : { not : $value:expr $(,)? }) => {{
                    compile_error!(concat!(
                        "relation field `",
                        stringify!(#ident),
                        "` in ",
                        #operation_display,
                        " helper for model `",
                        #model_name,
                        "` cannot use scalar operator `not`; provide a nested filter object instead"
                    ))
                }};
                (#ident : { $($nested_field:ident : $nested_value:tt),+ $(,)? }) => {
                    ::vitrail_pg::QueryFilter::relation(
                        stringify!(#ident),
                        #target_where_filter_macro_ident!({
                            $($nested_field : $nested_value),+
                        })
                        .expect("nested relation filter should contain at least one predicate"),
                    )
                };
                (#ident : $value:tt) => {{
                    compile_error!(concat!(
                        "malformed filter for relation field `",
                        stringify!(#ident),
                        "` in ",
                        #operation_display,
                        " helper for model `",
                        #model_name,
                        "`; expected a nested object like `{ nested_field: null }`, `{ nested_field: { eq: ... } }`, or `{ nested_field: { not: ... } }`"
                    ))
                }};
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(quote! {
        #[doc(hidden)]
        #[macro_export]
        macro_rules! #where_path_assert_ident {
            #(#scalar_where_path_arms)*
            #(#relation_where_path_arms)*
            ($other:ident $(. $rest:ident)*) => {
                compile_error!(concat!(
                    "unknown field `",
                    stringify!($other),
                    "` in ",
                    #operation_display,
                    " `where(...)` path for model `",
                    #model_name,
                    "`"
                ));
            };
        }

        #[doc(hidden)]
        #[macro_export]
        macro_rules! #where_field_filter_macro_ident {
            #(#scalar_where_field_arms)*
            #(#relation_where_field_arms)*
            ($other:ident : $value:tt) => {{
                compile_error!(concat!(
                    "unknown field `",
                    stringify!($other),
                    "` in ",
                    #operation_display,
                    " helper `where` for model `",
                    #model_name,
                    "`"
                ))
            }};
        }

        #[doc(hidden)]
        #[macro_export]
        macro_rules! #where_filter_macro_ident {
            ({}) => {{
                compile_error!(concat!(
                    "empty `where` blocks are not supported in ",
                    #operation_display,
                    " helper for model `",
                    #model_name,
                    "`"
                ))
            }};
            ({ $($where_field:ident : $where_value:tt),+ $(,)? }) => {{
                let __vitrail_filters = vec![
                    $(
                        #where_field_filter_macro_ident!($where_field : $where_value)
                    ),+
                ];

                if __vitrail_filters.len() == 1 {
                    Some(
                        __vitrail_filters
                            .into_iter()
                            .next()
                            .expect("single filter should exist"),
                    )
                } else {
                    Some(::vitrail_pg::QueryFilter::And(__vitrail_filters))
                }
            }};
        }
    })
}
