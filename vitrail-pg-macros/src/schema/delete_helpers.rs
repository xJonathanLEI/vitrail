use proc_macro2::{Ident, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{LitStr, Result};

use super::{ParsedSchema, dollar_crate, to_pascal_case};

impl ParsedSchema {
    pub(super) fn generate_delete_helper_items(&self, module_name: &Ident) -> Result<TokenStream2> {
        let main_macro_ident = format_ident!("__vitrail_delete_{}", module_name);
        let local_main_macro_ident = format_ident!("__vitrail_delete_local_{}", module_name);
        let mut helpers = TokenStream2::new();
        let mut main_arms = Vec::new();
        let dollar_crate = dollar_crate();

        for model in &self.models {
            let model_ident = &model.name;
            let model_name = LitStr::new(&model.name.to_string(), model.name.span());
            let where_path_assert_ident = format_ident!(
                "__vitrail_assert_delete_where_path_{}_{}",
                module_name,
                model.name
            );
            let where_filter_macro_ident = format_ident!(
                "__vitrail_delete_where_filter_{}_{}",
                module_name,
                model.name
            );
            let where_field_filter_macro_ident = format_ident!(
                "__vitrail_delete_where_field_filter_{}_{}",
                module_name,
                model.name
            );
            let trait_module_ident =
                format_ident!("__vitrail_delete_traits_{}_{}", module_name, model.name);
            let root_delete_ident =
                format_ident!("__VitrailDelete{}", to_pascal_case(&model.name.to_string()));

            let scalar_fields = model.scalar_fields();
            let relation_fields = model.relation_fields();

            let scalar_where_path_arms = scalar_fields.iter().map(|field| {
                let ident = &field.name;
                quote! {
                    (#ident) => {};
                    (#ident . $($rest:ident).+) => {
                        compile_error!(concat!(
                            "scalar field `",
                            stringify!(#ident),
                            "` cannot be traversed in delete `where(...)` for model `",
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
                    let target = self
                        .models
                        .iter()
                        .find(|candidate| {
                            candidate.name == field.ty.name
                                || field.ty.name == to_pascal_case(&candidate.name.to_string())
                        })
                        .expect("validated relation target");
                    let target_where_path_assert_ident = format_ident!(
                        "__vitrail_assert_delete_where_path_{}_{}",
                        module_name,
                        target.name
                    );

                    Ok(quote! {
                        (#ident) => {
                            compile_error!(concat!(
                                "relation field `",
                                stringify!(#ident),
                                "` cannot terminate a delete `where(...)` path for model `",
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
                            "` in delete helper for model `",
                            #model_name,
                            "`; only `eq`, `null`, and `{ not: ... }` are currently supported"
                        ))
                    }};
                    (#ident : $value:tt) => {{
                        compile_error!(concat!(
                            "malformed filter for scalar field `",
                            stringify!(#ident),
                            "` in delete helper for model `",
                            #model_name,
                            "`; expected `null`, `{ eq: ... }`, or `{ not: ... }`"
                        ))
                    }};
                }
            });

            let relation_where_field_arms = relation_fields
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
                    let target_where_filter_macro_ident = format_ident!(
                        "__vitrail_delete_where_filter_{}_{}",
                        module_name,
                        target.name
                    );

                    Ok(quote! {
                        (#ident : { }) => {{
                            compile_error!(concat!(
                                "relation field `",
                                stringify!(#ident),
                                "` in delete helper for model `",
                                #model_name,
                                "` requires a nested filter object"
                            ))
                        }};
                        (#ident : { eq : $value:expr $(,)? }) => {{
                            compile_error!(concat!(
                                "relation field `",
                                stringify!(#ident),
                                "` in delete helper for model `",
                                #model_name,
                                "` cannot use scalar operator `eq`; provide a nested filter object instead"
                            ))
                        }};
                        (#ident : null) => {{
                            compile_error!(concat!(
                                "relation field `",
                                stringify!(#ident),
                                "` in delete helper for model `",
                                #model_name,
                                "` cannot use scalar null filter; provide a nested filter object instead"
                            ))
                        }};
                        (#ident : { not : null $(,)? }) => {{
                            compile_error!(concat!(
                                "relation field `",
                                stringify!(#ident),
                                "` in delete helper for model `",
                                #model_name,
                                "` cannot use scalar null filter; provide a nested filter object instead"
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
                                "` in delete helper for model `",
                                #model_name,
                                "`; expected a nested object like `{ nested_field: null }`, `{ nested_field: { eq: ... } }`, or `{ nested_field: { not: ... } }`"
                            ))
                        }};
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            helpers.extend(quote! {
                #[doc(hidden)]
                #[macro_export]
                macro_rules! #where_path_assert_ident {
                    #(#scalar_where_path_arms)*
                    #(#relation_where_path_arms)*
                    ($other:ident $(. $rest:ident)*) => {
                        compile_error!(concat!(
                            "unknown field `",
                            stringify!($other),
                            "` in delete `where(...)` path for model `",
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
                            "` in delete helper `where` for model `",
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
                            "empty `where` blocks are not supported in delete helper for model `",
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

                #[doc(hidden)]
                pub mod #trait_module_ident {
                    #[allow(non_camel_case_types)]
                    #[doc(hidden)]
                    pub trait __VitrailDeleteModel {}
                }
            });

            main_arms.push(quote! {
                (
                    #model_ident {
                        where: {
                            $($where_field:ident : $where_value:tt),* $(,)?
                        }
                        $(,)?
                    }
                ) => {{
                    #[allow(dead_code)]
                    struct #root_delete_ident;

                    impl ::vitrail_pg::DeleteManyModel for #root_delete_ident {
                        type Schema = #dollar_crate::#module_name::Schema;
                        type Variables = ();

                        fn model_name() -> &'static str {
                            #model_name
                        }

                        fn filter() -> Option<::vitrail_pg::QueryFilter> {
                            #where_filter_macro_ident!({
                                $($where_field : $where_value),*
                            })
                        }
                    }

                    #dollar_crate::#module_name::delete_many::<#root_delete_ident>()
                }};

                (
                    #model_ident { $(,)? }
                ) => {{
                    #[allow(dead_code)]
                    struct #root_delete_ident;

                    impl ::vitrail_pg::DeleteManyModel for #root_delete_ident {
                        type Schema = #dollar_crate::#module_name::Schema;
                        type Variables = ();

                        fn model_name() -> &'static str {
                            #model_name
                        }
                    }

                    #dollar_crate::#module_name::delete_many::<#root_delete_ident>()
                }};
            });
        }

        helpers.extend(quote! {
            #[doc(hidden)]
            macro_rules! #local_main_macro_ident {
                #(#main_arms)*
                ($($tokens:tt)*) => {
                    compile_error!("unsupported delete shape");
                };
            }

            #[doc(hidden)]
            #[macro_export(local_inner_macros)]
            macro_rules! #main_macro_ident {
                #(#main_arms)*
                ($($tokens:tt)*) => {
                    compile_error!("unsupported delete shape");
                };
            }
        });

        Ok(helpers)
    }
}
