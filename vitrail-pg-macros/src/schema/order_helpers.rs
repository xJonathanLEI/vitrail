use proc_macro2::{Ident, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{LitStr, Result};

use super::{ParsedModel, ParsedSchema, to_pascal_case};

pub(super) fn generate_order_helper_items(
    schema: &ParsedSchema,
    module_name: &Ident,
    model: &ParsedModel,
    order_path_assert_ident: &Ident,
    order_entries_macro_ident: &Ident,
    order_field_entry_macro_ident: &Ident,
) -> Result<TokenStream2> {
    let model_name = LitStr::new(&model.name.to_string(), model.name.span());
    let scalar_fields = model.scalar_fields();
    let relation_fields = model.relation_fields();

    let scalar_order_path_arms = scalar_fields.iter().map(|field| {
        let ident = &field.name;
        quote! {
            (#ident) => {};
            (#ident . $($rest:ident).+) => {
                compile_error!(concat!(
                    "scalar field `",
                    ::core::stringify!(#ident),
                    "` cannot be traversed in query `order_by` for model `",
                    #model_name,
                    "`"
                ));
            };
        }
    });

    let relation_order_path_arms = relation_fields
        .iter()
        .map(|field| {
            let ident = &field.name;

            if field.ty.many {
                return Ok(quote! {
                    (#ident) => {
                        compile_error!(concat!(
                            "relation field `",
                            ::core::stringify!(#ident),
                            "` cannot terminate query `order_by` for model `",
                            #model_name,
                            "`"
                        ));
                    };
                    (#ident . $($rest:ident).+) => {
                        compile_error!(concat!(
                            "to-many relation field `",
                            ::core::stringify!(#ident),
                            "` cannot be traversed in query `order_by` for model `",
                            #model_name,
                            "`"
                        ));
                    };
                });
            }

            let target = schema
                .models
                .iter()
                .find(|candidate| {
                    candidate.name == field.ty.name
                        || field.ty.name == to_pascal_case(&candidate.name.to_string())
                })
                .expect("validated relation target");
            let target_order_path_assert_ident = format_ident!(
                "__vitrail_assert_query_order_path_{}_{}",
                module_name,
                target.name
            );

            Ok(quote! {
                (#ident) => {
                    compile_error!(concat!(
                        "relation field `",
                        ::core::stringify!(#ident),
                        "` cannot terminate query `order_by` for model `",
                        #model_name,
                        "`"
                    ));
                };
                (#ident . $($rest:ident).+) => {
                    #module_name::#target_order_path_assert_ident!($($rest).+);
                };
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let scalar_order_field_arms = scalar_fields.iter().map(|field| {
        let ident = &field.name;

        quote! {
            ({ #ident : asc $(,)? }) => {
                ::vitrail_pg::QueryOrder::scalar(
                    ::core::stringify!(#ident),
                    ::vitrail_pg::QueryOrderDirection::Asc,
                )
            };
            ({ #ident : desc $(,)? }) => {
                ::vitrail_pg::QueryOrder::scalar(
                    ::core::stringify!(#ident),
                    ::vitrail_pg::QueryOrderDirection::Desc,
                )
            };
        }
    });

    let relation_order_field_arms = relation_fields
        .iter()
        .map(|field| {
            let ident = &field.name;

            if field.ty.many {
                return Ok(quote! {
                    ({ #ident : { $($nested:tt)+ } }) => {{
                        compile_error!(concat!(
                            "to-many relation field `",
                            ::core::stringify!(#ident),
                            "` cannot be traversed in query `order_by` for model `",
                            #model_name,
                            "`"
                        ))
                    }};
                });
            }

            let target = schema
                .models
                .iter()
                .find(|candidate| {
                    candidate.name == field.ty.name
                        || field.ty.name == to_pascal_case(&candidate.name.to_string())
                })
                .expect("validated relation target");
            let target_order_entries_macro_ident = format_ident!(
                "__vitrail_query_order_entries_{}_{}",
                module_name,
                target.name
            );

            Ok(quote! {
                ({ #ident : { $($nested_field:ident : $nested_value:tt),+ $(,)? } }) => {{
                    ::vitrail_pg::QueryOrder::relation(
                        ::core::stringify!(#ident),
                        #module_name::#target_order_entries_macro_ident!({
                            $($nested_field : $nested_value),+
                        }),
                    )
                }};
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(quote! {
        #[doc(hidden)]
        #[macro_export]
        macro_rules! #order_path_assert_ident {
            #(#scalar_order_path_arms)*
            #(#relation_order_path_arms)*
            ($other:ident $(. $rest:ident)*) => {
                compile_error!(concat!(
                    "unknown field `",
                    ::core::stringify!($other $(. $rest)*),
                    "` in query `order_by` for model `",
                    #model_name,
                    "`"
                ));
            };
        }

        #[doc(hidden)]
        #[macro_export]
        macro_rules! #order_field_entry_macro_ident {
            #(#scalar_order_field_arms)*
            #(#relation_order_field_arms)*
            ({ $field:ident : $value:ident $(,)? }) => {{
                #module_name::#order_path_assert_ident!($field);
                compile_error!(concat!(
                    "unsupported query `order_by` direction `",
                    ::core::stringify!($value),
                    "`; expected `asc` or `desc`"
                ))
            }};
            ({ $field:ident : $value:tt $(,)? }) => {{
                #module_name::#order_path_assert_ident!($field);
                compile_error!(concat!(
                    "unsupported query `order_by` entry for field `",
                    ::core::stringify!($field),
                    "`; expected `asc`, `desc`, or a nested object"
                ))
            }};
            ($other:tt) => {{
                compile_error!("unsupported query `order_by` entry")
            }};
        }

        #[doc(hidden)]
        #[macro_export]
        macro_rules! #order_entries_macro_ident {
            ([ $entry:tt, $($rest:tt)+ ]) => {{
                let mut __vitrail_order_by = ::std::vec![
                    #module_name::#order_field_entry_macro_ident!($entry)
                ];
                __vitrail_order_by.extend(
                    #module_name::#order_entries_macro_ident!([ $($rest)+ ])
                );
                __vitrail_order_by
            }};
            ([ $entry:tt $(,)? ]) => {
                ::std::vec![#module_name::#order_field_entry_macro_ident!($entry)]
            };
            ({ $field:ident : $value:tt, $($rest:tt)+ }) => {{
                let mut __vitrail_order_by = ::std::vec![
                    #module_name::#order_field_entry_macro_ident!({ $field : $value })
                ];
                __vitrail_order_by.extend(
                    #module_name::#order_entries_macro_ident!({ $($rest)+ })
                );
                __vitrail_order_by
            }};
            ({ $field:ident : $value:tt $(,)? }) => {
                ::std::vec![#module_name::#order_field_entry_macro_ident!({ $field : $value })]
            };
            ({}) => {
                ::std::vec::Vec::<::vitrail_pg::QueryOrder>::new()
            };
            ([]) => {
                ::std::vec::Vec::<::vitrail_pg::QueryOrder>::new()
            };
        }
    })
}
