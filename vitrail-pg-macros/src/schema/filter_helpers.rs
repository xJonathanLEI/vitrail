use proc_macro2::{Ident, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{LitStr, Result};

use super::{ParsedModel, ParsedSchema, dollar_crate, rust_field_type_tokens, to_pascal_case};

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
    let dollar_crate = dollar_crate();
    let operation_type_prefix = to_pascal_case(operation);
    let filter_trait_module_ident = format_ident!(
        "__vitrail_{}_filter_traits_{}_{}",
        operation,
        module_name,
        model.name
    );
    let where_filter_value_assert_ident = format_ident!(
        "__vitrail_assert_{}_filter_value_type_{}_{}",
        operation,
        module_name,
        model.name
    );
    let where_variable_filter_macro_ident = format_ident!(
        "__vitrail_{}_where_variable_filter_{}_{}",
        operation,
        module_name,
        model.name
    );
    let where_field_variable_filter_macro_ident = format_ident!(
        "__vitrail_{}_where_field_variable_filter_{}_{}",
        operation,
        module_name,
        model.name
    );
    let where_variable_entries_macro_ident = format_ident!(
        "__vitrail_{}_where_variable_entries_{}_{}",
        operation,
        module_name,
        model.name
    );
    let where_variables_macro_ident = format_ident!(
        "__vitrail_{}_where_variables_{}_{}",
        operation,
        module_name,
        model.name
    );

    let scalar_where_path_arms = scalar_fields.iter().map(|field| {
        let ident = &field.name;
        quote! {
            (#ident) => {};
            (#ident . $($rest:ident).+) => {
                compile_error!(concat!(
                    "scalar field `",
                    ::core::stringify!(#ident),
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
                        ::core::stringify!(#ident),
                        "` cannot terminate a ",
                        #operation_display,
                        " `where(...)` path for model `",
                        #model_name,
                        "`"
                    ));
                };
                (#ident . $($rest:ident).+) => {
                    #dollar_crate::#module_name::#target_where_path_assert_ident!($($rest).+);
                };
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let scalar_filter_type_traits = scalar_fields
        .iter()
        .map(|field| {
            let ident = &field.name;
            let eq_trait_ident = format_ident!(
                "__Vitrail{}FilterEqType_{}_{}_{}",
                operation_type_prefix,
                module_name,
                model.name,
                ident
            );
            let in_trait_ident = format_ident!(
                "__Vitrail{}FilterInType_{}_{}_{}",
                operation_type_prefix,
                module_name,
                model.name,
                ident
            );
            let eq_ty = rust_field_type_tokens(field)?;
            let in_element_ty = if let Some(rust_ty) = field.rust_type() {
                quote! { #rust_ty }
            } else if field.has_db_uuid() {
                quote! { ::vitrail_pg::uuid::Uuid }
            } else {
                match field.ty.name.to_string().as_str() {
                    "Int" => quote! { i64 },
                    "BigInt" => quote! { i64 },
                    "String" => quote! { String },
                    "Boolean" => quote! { bool },
                    "DateTime" => quote! { ::chrono::DateTime<::chrono::Utc> },
                    "Float" => quote! { f64 },
                    "Decimal" => quote! { ::vitrail_pg::rust_decimal::Decimal },
                    "Bytes" => quote! { Vec<u8> },
                    other => unreachable!("unsupported scalar field type `{other}`"),
                }
            };
            let is_plain_string_field =
                field.rust_type().is_none() && !field.has_db_uuid() && field.ty.name == "String";

            let eq_impls = if is_plain_string_field {
                if field.ty.optional {
                    quote! {
                        impl #eq_trait_ident for Option<String> {}
                        impl #eq_trait_ident for &Option<String> {}
                        impl #eq_trait_ident for String {}
                        impl #eq_trait_ident for &String {}
                        impl #eq_trait_ident for &str {}
                        impl #eq_trait_ident for Option<&str> {}
                    }
                } else {
                    quote! {
                        impl #eq_trait_ident for String {}
                        impl #eq_trait_ident for &String {}
                        impl #eq_trait_ident for &str {}
                    }
                }
            } else {
                let eq_optional_impls = if field.ty.optional {
                    quote! {
                        impl #eq_trait_ident for #in_element_ty {}
                        impl #eq_trait_ident for &#in_element_ty {}
                    }
                } else {
                    quote! {}
                };

                quote! {
                    impl #eq_trait_ident for #eq_ty {}
                    impl #eq_trait_ident for &#eq_ty {}
                    #eq_optional_impls
                }
            };

            let in_impls = if is_plain_string_field {
                quote! {
                    impl #in_trait_ident for Vec<String> {}
                    impl #in_trait_ident for &Vec<String> {}
                    impl #in_trait_ident for Vec<&str> {}
                    impl #in_trait_ident for &Vec<&str> {}
                    impl<const N: usize> #in_trait_ident for [String; N] {}
                    impl<const N: usize> #in_trait_ident for &[String; N] {}
                    impl<const N: usize> #in_trait_ident for [&str; N] {}
                    impl<const N: usize> #in_trait_ident for &[&str; N] {}
                }
            } else {
                quote! {
                    impl #in_trait_ident for Vec<#in_element_ty> {}
                    impl #in_trait_ident for &Vec<#in_element_ty> {}
                    impl<const N: usize> #in_trait_ident for [#in_element_ty; N] {}
                    impl<const N: usize> #in_trait_ident for &[#in_element_ty; N] {}
                }
            };

            Ok(quote! {
                #[allow(non_camel_case_types)]
                #[doc(hidden)]
                pub trait #eq_trait_ident {}

                #eq_impls

                #[allow(non_camel_case_types)]
                #[doc(hidden)]
                pub trait #in_trait_ident {}

                #in_impls
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let scalar_filter_value_assert_arms = scalar_fields
        .iter()
        .map(|field| {
            let ident = &field.name;
            let eq_trait_ident = format_ident!(
                "__Vitrail{}FilterEqType_{}_{}_{}",
                operation_type_prefix,
                module_name,
                model.name,
                ident
            );
            let in_trait_ident = format_ident!(
                "__Vitrail{}FilterInType_{}_{}_{}",
                operation_type_prefix,
                module_name,
                model.name,
                ident
            );

            Ok(quote! {
                (#ident, eq, $value:expr) => {{
                    fn __vitrail_assert_filter_value_type<
                        T: #dollar_crate::#module_name::#filter_trait_module_ident::#eq_trait_ident,
                    >(_: &T) {}
                    __vitrail_assert_filter_value_type(&$value);
                }};
                (#ident, in, $value:expr) => {{
                    fn __vitrail_assert_filter_value_type<
                        T: #dollar_crate::#module_name::#filter_trait_module_ident::#in_trait_ident,
                    >(_: &T) {}
                    __vitrail_assert_filter_value_type(&$value);
                }};
                (#ident, not, $value:expr) => {{
                    fn __vitrail_assert_filter_value_type<
                        T: #dollar_crate::#module_name::#filter_trait_module_ident::#eq_trait_ident,
                    >(_: &T) {}
                    __vitrail_assert_filter_value_type(&$value);
                }};
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let relation_filter_value_assert_arms = relation_fields
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
            let target_where_filter_value_assert_ident = format_ident!(
                "__vitrail_assert_{}_filter_value_type_{}_{}",
                operation,
                module_name,
                target.name
            );

            Ok(quote! {
                (#ident . $next:ident $(. $rest:ident)*, $operator:ident, $value:expr) => {
                    #dollar_crate::#module_name::#target_where_filter_value_assert_ident!($next $(. $rest)*, $operator, $value)
                };
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let scalar_where_field_arms = scalar_fields.iter().map(|field| {
        let ident = &field.name;

        quote! {
            (#ident : null) => {
                ::vitrail_pg::QueryFilter::is_null(::core::stringify!(#ident))
            };
            (#ident : { eq : $value:expr $(,)? }) => {{
                #dollar_crate::#module_name::#where_filter_value_assert_ident!(#ident, eq, $value);
                ::vitrail_pg::QueryFilter::eq(
                    ::core::stringify!(#ident),
                    ::vitrail_pg::QueryFilterValue::value($value),
                )
            }};
            (#ident : { in : $value:expr $(,)? }) => {{
                #dollar_crate::#module_name::#where_filter_value_assert_ident!(#ident, in, $value);
                ::vitrail_pg::QueryFilter::r#in(
                    ::core::stringify!(#ident),
                    ::vitrail_pg::QueryFilterValues::from($value),
                )
            }};
            (#ident : { not : null $(,)? }) => {
                ::vitrail_pg::QueryFilter::is_not_null(::core::stringify!(#ident))
            };
            (#ident : { not : $value:expr $(,)? }) => {{
                #dollar_crate::#module_name::#where_filter_value_assert_ident!(#ident, not, $value);
                ::vitrail_pg::QueryFilter::ne(
                    ::core::stringify!(#ident),
                    ::vitrail_pg::QueryFilterValue::value($value),
                )
            }};
            (#ident : { $operator:ident : $value:tt $(,)? }) => {{
                compile_error!(concat!(
                    "unsupported `where` operator `",
                    ::core::stringify!($operator),
                    "` for scalar field `",
                    ::core::stringify!(#ident),
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
                    ::core::stringify!(#ident),
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
            let target_where_variable_filter_macro_ident = format_ident!(
                "__vitrail_{}_where_variable_filter_{}_{}",
                operation,
                module_name,
                target.name
            );
            let target_where_variable_entries_macro_ident = format_ident!(
                "__vitrail_{}_where_variable_entries_{}_{}",
                operation,
                module_name,
                target.name
            );

            Ok(quote! {
                (#ident : { }) => {{
                    compile_error!(concat!(
                        "relation field `",
                        ::core::stringify!(#ident),
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
                        ::core::stringify!(#ident),
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
                        ::core::stringify!(#ident),
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
                        ::core::stringify!(#ident),
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
                        ::core::stringify!(#ident),
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
                        ::core::stringify!(#ident),
                        "` in ",
                        #operation_display,
                        " helper for model `",
                        #model_name,
                        "` cannot use scalar operator `not`; provide a nested filter object instead"
                    ))
                }};
                (#ident : { $($nested_field:ident : $nested_value:tt),+ $(,)? }) => {
                    ::vitrail_pg::QueryFilter::relation(
                        ::core::stringify!(#ident),
                        #dollar_crate::#module_name::#target_where_filter_macro_ident!({
                            $($nested_field : $nested_value),+
                        })
                        .expect("nested relation filter should contain at least one predicate"),
                    )
                };
                (@variable_filter [ $($path:tt)* ] #ident : { $($nested_field:ident : $nested_value:tt),+ $(,)? }) => {
                    ::vitrail_pg::QueryFilter::relation(
                        ::core::stringify!(#ident),
                        #dollar_crate::#module_name::#target_where_variable_filter_macro_ident!(
                            @filter_block
                            [ $($path)* #ident . ]
                            { $($nested_field : $nested_value),+ }
                        )
                        .expect("nested relation filter should contain at least one predicate"),
                    )
                };
                (@variable_entries [ $($path:tt)* ] #ident : { $($nested_field:ident : $nested_value:tt),+ $(,)? }) => {
                    #dollar_crate::#module_name::#target_where_variable_entries_macro_ident!(
                        @entries_block
                        [ $($path)* #ident . ]
                        { $($nested_field : $nested_value),+ }
                    )
                };
                (#ident : $value:tt) => {{
                    compile_error!(concat!(
                        "malformed filter for relation field `",
                        ::core::stringify!(#ident),
                        "` in ",
                        #operation_display,
                        " helper for model `",
                        #model_name,
                        "`; expected a nested object like `{ nested_field: null }`, `{ nested_field: { eq: ... } }`, or `{ nested_field: { not: ... } }`"
                    ))
                }};
                (@variable_filter [ $($path:tt)* ] #ident : $value:tt) => {{
                    #dollar_crate::#module_name::#where_field_filter_macro_ident!(#ident : $value)
                }};
                (@variable_entries [ $($path:tt)* ] #ident : $value:tt) => {
                    #dollar_crate::#module_name::#where_field_filter_macro_ident!(#ident : $value)
                };
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let scalar_where_field_variable_filter_arms = scalar_fields.iter().map(|field| {
        let ident = &field.name;

        quote! {
            (@filter [ $($path:tt)* ] #ident : null) => {
                ::vitrail_pg::QueryFilter::is_null(::core::stringify!(#ident))
            };
            (@filter [ $($path:tt)* ] #ident : { eq : $value:tt $(,)? }) => {{
                ::vitrail_pg::QueryFilter::eq(
                    ::core::stringify!(#ident),
                    ::vitrail_pg::QueryFilterValue::variable(
                        ::std::string::String::from(::core::concat!(::core::stringify!($($path)* #ident), "::eq")),
                    ),
                )
            }};
            (@filter [ $($path:tt)* ] #ident : { in : $value:tt $(,)? }) => {{
                ::vitrail_pg::QueryFilter::r#in(
                    ::core::stringify!(#ident),
                    ::vitrail_pg::QueryFilterValues::variable(
                        ::std::string::String::from(::core::concat!(::core::stringify!($($path)* #ident), "::in")),
                    ),
                )
            }};
            (@filter [ $($path:tt)* ] #ident : { not : null $(,)? }) => {
                ::vitrail_pg::QueryFilter::is_not_null(::core::stringify!(#ident))
            };
            (@filter [ $($path:tt)* ] #ident : { not : $value:tt $(,)? }) => {{
                ::vitrail_pg::QueryFilter::ne(
                    ::core::stringify!(#ident),
                    ::vitrail_pg::QueryFilterValue::variable(
                        ::std::string::String::from(::core::concat!(::core::stringify!($($path)* #ident), "::not")),
                    ),
                )
            }};
            (@filter [ $($path:tt)* ] #ident : $value:tt) => {{
                #dollar_crate::#module_name::#where_field_filter_macro_ident!(#ident : $value)
            }};
        }
    });

    let scalar_where_variable_entry_arms = scalar_fields.iter().map(|field| {
        let ident = &field.name;

        quote! {
            (@entries [ $($path:tt)* ] #ident : null) => {
                ::std::vec::Vec::<(::std::string::String, ::vitrail_pg::QueryVariableValue)>::new()
            };
            (@entries [ $($path:tt)* ] #ident : { eq : $value:expr $(,)? }) => {
                ::std::vec![{
                    #dollar_crate::#module_name::#where_filter_value_assert_ident!(#ident, eq, $value);
                    (
                        ::std::string::String::from(::core::concat!(::core::stringify!($($path)* #ident), "::eq")),
                        ::vitrail_pg::QueryScalar::into_query_variable_value($value),
                    )
                }]
            };
            (@entries [ $($path:tt)* ] #ident : { in : $value:expr $(,)? }) => {
                ::std::vec![{
                    #dollar_crate::#module_name::#where_filter_value_assert_ident!(#ident, in, $value);
                    (
                        ::std::string::String::from(::core::concat!(::core::stringify!($($path)* #ident), "::in")),
                        ::vitrail_pg::QueryScalar::into_query_variable_value($value),
                    )
                }]
            };
            (@entries [ $($path:tt)* ] #ident : { not : null $(,)? }) => {
                ::std::vec::Vec::<(::std::string::String, ::vitrail_pg::QueryVariableValue)>::new()
            };
            (@entries [ $($path:tt)* ] #ident : { not : $value:expr $(,)? }) => {
                ::std::vec![{
                    #dollar_crate::#module_name::#where_filter_value_assert_ident!(#ident, not, $value);
                    (
                        ::std::string::String::from(::core::concat!(::core::stringify!($($path)* #ident), "::not")),
                        ::vitrail_pg::QueryScalar::into_query_variable_value($value),
                    )
                }]
            };
            (@entries [ $($path:tt)* ] #ident : $value:tt) => {{
                #dollar_crate::#module_name::#where_field_filter_macro_ident!(#ident : $value);
                ::std::vec::Vec::<(::std::string::String, ::vitrail_pg::QueryVariableValue)>::new()
            }};
        }
    });

    Ok(quote! {
        #[doc(hidden)]
        pub mod #filter_trait_module_ident {
            #(#scalar_filter_type_traits)*
        }

        #[doc(hidden)]
        #[macro_export]
        macro_rules! #where_filter_value_assert_ident {
            #(#scalar_filter_value_assert_arms)*
            #(#relation_filter_value_assert_arms)*
            ($other:ident $(. $rest:ident)*, $operator:ident, $value:expr) => {{
                let _ = &$value;
            }};
        }

        #[doc(hidden)]
        #[macro_export]
        macro_rules! #where_path_assert_ident {
            #(#scalar_where_path_arms)*
            #(#relation_where_path_arms)*
            ($other:ident $(. $rest:ident)*) => {
                compile_error!(concat!(
                    "unknown field `",
                    ::core::stringify!($other),
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
                    ::core::stringify!($other),
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
                let __vitrail_filters = ::std::vec![
                    $(
                        #dollar_crate::#module_name::#where_field_filter_macro_ident!($where_field : $where_value)
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

        #[doc(hidden)]
        #[macro_export]
        macro_rules! #where_field_variable_filter_macro_ident {
            #(#scalar_where_field_variable_filter_arms)*
            #(#relation_where_field_arms)*
            (@filter [ $($path:tt)* ] $other:ident : $value:tt) => {{
                #dollar_crate::#module_name::#where_field_filter_macro_ident!($other : $value)
            }};
            ($other:tt $($rest:tt)*) => {{
                compile_error!(concat!(
                    "unsupported ",
                    #operation_display,
                    " helper variable filter shape for model `",
                    #model_name,
                    "`"
                ))
            }};
        }

        #[doc(hidden)]
        #[macro_export]
        macro_rules! #where_variable_filter_macro_ident {
            (@filter_block [ $($path:tt)* ] { $where_field:ident : $where_value:tt, $($rest:tt)+ }) => {{
                let mut __vitrail_filters = ::std::vec![
                    #dollar_crate::#module_name::#where_field_variable_filter_macro_ident!(
                        @filter
                        [ $($path)* ]
                        $where_field : $where_value
                    )
                ];

                if let Some(__vitrail_rest_filter) = #dollar_crate::#module_name::#where_variable_filter_macro_ident!(
                    @filter_block
                    [ $($path)* ]
                    { $($rest)+ }
                ) {
                    match __vitrail_rest_filter {
                        ::vitrail_pg::QueryFilter::And(mut __vitrail_rest_filters) => {
                            __vitrail_filters.append(&mut __vitrail_rest_filters);
                        }
                        __vitrail_filter => __vitrail_filters.push(__vitrail_filter),
                    }
                }

                Some(if __vitrail_filters.len() == 1 {
                    __vitrail_filters
                        .into_iter()
                        .next()
                        .expect("single filter should exist")
                } else {
                    ::vitrail_pg::QueryFilter::And(__vitrail_filters)
                })
            }};
            (@filter_block [ $($path:tt)* ] { $where_field:ident : $where_value:tt $(,)? }) => {{
                Some(
                    #dollar_crate::#module_name::#where_field_variable_filter_macro_ident!(
                        @filter
                        [ $($path)* ]
                        $where_field : $where_value
                    )
                )
            }};
            ({}) => {{
                compile_error!(concat!(
                    "empty `where` blocks are not supported in ",
                    #operation_display,
                    " helper for model `",
                    #model_name,
                    "`"
                ))
            }};
            ({ $($where_tokens:tt)+ }) => {
                #dollar_crate::#module_name::#where_variable_filter_macro_ident!(
                    @filter_block
                    [ ]
                    { $($where_tokens)+ }
                )
            };
        }

        #[doc(hidden)]
        #[macro_export]
        macro_rules! #where_variable_entries_macro_ident {
            (@entries_block [ $($path:tt)* ] { $where_field:ident : $where_value:tt, $($rest:tt)+ }) => {{
                let mut __vitrail_entries = #dollar_crate::#module_name::#where_variable_entries_macro_ident!(
                    @entries
                    [ $($path)* ]
                    $where_field : $where_value
                );
                __vitrail_entries.extend(
                    #dollar_crate::#module_name::#where_variable_entries_macro_ident!(
                        @entries_block
                        [ $($path)* ]
                        { $($rest)+ }
                    )
                );
                __vitrail_entries
            }};
            (@entries_block [ $($path:tt)* ] { $where_field:ident : $where_value:tt $(,)? }) => {
                #dollar_crate::#module_name::#where_variable_entries_macro_ident!(
                    @entries
                    [ $($path)* ]
                    $where_field : $where_value
                )
            };
            #(#scalar_where_variable_entry_arms)*
            #(#relation_where_field_arms)*
            (@entries [ $($path:tt)* ] $other:ident : $value:tt) => {{
                #dollar_crate::#module_name::#where_field_filter_macro_ident!($other : $value);
                ::std::vec::Vec::<(::std::string::String, ::vitrail_pg::QueryVariableValue)>::new()
            }};
            ($other:tt $($rest:tt)*) => {{
                compile_error!(concat!(
                    "unsupported ",
                    #operation_display,
                    " helper variable binding shape for model `",
                    #model_name,
                    "`"
                ))
            }};
        }

        #[doc(hidden)]
        #[macro_export]
        macro_rules! #where_variables_macro_ident {
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
                ::vitrail_pg::QueryVariables::from_values(
                    #dollar_crate::#module_name::#where_variable_entries_macro_ident!(
                        @entries_block
                        [ ]
                        { $($where_field : $where_value),+ }
                    )
                )
            }};
        }
    })
}
