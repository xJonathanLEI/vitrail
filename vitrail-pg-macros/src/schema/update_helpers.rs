use proc_macro2::{Ident, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{LitStr, Result};

use super::{ParsedSchema, dollar_crate, rust_field_type_tokens, to_pascal_case};

impl ParsedSchema {
    pub(super) fn generate_update_helper_items(&self, module_name: &Ident) -> Result<TokenStream2> {
        let main_macro_ident = format_ident!("__vitrail_update_{}", module_name);
        let local_main_macro_ident = format_ident!("__vitrail_update_local_{}", module_name);
        let mut helpers = TokenStream2::new();
        let mut main_arms = Vec::new();
        let dollar_crate = dollar_crate();

        for model in &self.models {
            let model_ident = &model.name;
            let model_name = LitStr::new(&model.name.to_string(), model.name.span());
            let data_assert_ident = format_ident!(
                "__vitrail_assert_update_data_field_{}_{}",
                module_name,
                model.name
            );
            let data_type_assert_ident = format_ident!(
                "__vitrail_assert_update_data_type_{}_{}",
                module_name,
                model.name
            );
            let where_path_assert_ident = format_ident!(
                "__vitrail_assert_update_where_path_{}_{}",
                module_name,
                model.name
            );
            let where_filter_macro_ident = format_ident!(
                "__vitrail_update_where_filter_{}_{}",
                module_name,
                model.name
            );
            let where_field_filter_macro_ident = format_ident!(
                "__vitrail_update_where_field_filter_{}_{}",
                module_name,
                model.name
            );
            let data_struct_macro_ident = format_ident!(
                "__vitrail_update_data_struct_{}_{}",
                module_name,
                model.name
            );
            let data_value_macro_ident =
                format_ident!("__vitrail_update_data_value_{}_{}", module_name, model.name);
            let trait_module_ident =
                format_ident!("__vitrail_update_traits_{}_{}", module_name, model.name);
            let root_data_ident = format_ident!(
                "__VitrailUpdate{}Data",
                to_pascal_case(&model.name.to_string())
            );
            let root_update_ident =
                format_ident!("__VitrailUpdate{}", to_pascal_case(&model.name.to_string()));

            let scalar_fields = model.scalar_fields();
            let relation_fields = model.relation_fields();

            let data_assert_arms = scalar_fields.iter().map(|field| {
                let ident = &field.name;
                quote! { (#ident) => {}; }
            });

            let relation_data_arms = relation_fields.iter().map(|field| {
                let ident = &field.name;
                quote! {
                    (#ident) => {
                        compile_error!(concat!(
                            "relation field `",
                            stringify!(#ident),
                            "` cannot be used in update data for model `",
                            #model_name,
                            "`"
                        ));
                    };
                }
            });

            let data_type_assert_arms = scalar_fields
                .iter()
                .map(|field| {
                    let ident = &field.name;
                    let trait_ident = format_ident!(
                        "__VitrailUpdateDataType_{}_{}_{}",
                        module_name,
                        model.name,
                        field.name
                    );

                    Ok(quote! {
                        ($ty:ty, #ident) => {
                            {
                                fn __vitrail_assert_update_data_field_type<
                                    T: #dollar_crate::#module_name::#trait_module_ident::#trait_ident
                                >() {}
                                __vitrail_assert_update_data_field_type::<$ty>();
                            }
                        };
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            let relation_data_type_assert_arms = relation_fields.iter().map(|field| {
                let ident = &field.name;
                quote! {
                    ($ty:ty, #ident) => {
                        compile_error!(concat!(
                            "relation field `",
                            stringify!(#ident),
                            "` cannot be used in update data for model `",
                            #model_name,
                            "`"
                        ));
                    };
                }
            });

            let data_traits = scalar_fields
                .iter()
                .map(|field| {
                    let trait_ident = format_ident!(
                        "__VitrailUpdateDataType_{}_{}_{}",
                        module_name,
                        model.name,
                        field.name
                    );
                    let rust_ty = rust_field_type_tokens(field)?;

                    Ok(quote! {
                        #[allow(non_camel_case_types)]
                        #[doc(hidden)]
                        pub trait #trait_ident {}

                        impl #trait_ident for #rust_ty {}
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            let scalar_where_path_arms = scalar_fields.iter().map(|field| {
                let ident = &field.name;
                quote! {
                    (#ident) => {};
                    (#ident . $($rest:ident).+) => {
                        compile_error!(concat!(
                            "scalar field `",
                            stringify!(#ident),
                            "` cannot be traversed in update `where(...)` for model `",
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
                        "__vitrail_assert_update_where_path_{}_{}",
                        module_name,
                        target.name
                    );

                    Ok(quote! {
                        (#ident) => {
                            compile_error!(concat!(
                                "relation field `",
                                stringify!(#ident),
                                "` cannot terminate an update `where(...)` path for model `",
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

            let data_struct_arms = scalar_fields
                .iter()
                .map(|field| {
                    let ident = &field.name;
                    let ty = rust_field_type_tokens(field)?;

                    Ok(quote! {
                        (
                            @struct
                            $data_ident:ident
                            [ $($fields:tt)* ]
                            [ #ident : $value:expr, $($rest_field:ident : $rest_value:expr,)* ]
                        ) => {
                            #data_struct_macro_ident! {
                                @struct
                                $data_ident
                                [ $($fields)* pub #ident: #ty, ]
                                [ $($rest_field : $rest_value,)* ]
                            }
                        };
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            let data_value_arms = scalar_fields
                .iter()
                .map(|field| {
                    let ident = &field.name;

                    Ok(quote! {
                        (
                            @value
                            $data_ident:ident
                            [ $($bindings:tt)* ]
                            [ $($initializers:tt)* ]
                            [ #ident : $value:expr, $($rest_field:ident : $rest_value:expr,)* ]
                        ) => {
                            #data_value_macro_ident! {
                                @value
                                $data_ident
                                [
                                    $($bindings)*
                                    let #ident = $value;
                                ]
                                [
                                    $($initializers)*
                                    #ident,
                                ]
                                [ $($rest_field : $rest_value,)* ]
                            }
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
                    (#ident : { $operator:ident : $value:tt $(,)? }) => {{
                        compile_error!(concat!(
                            "unsupported `where` operator `",
                            stringify!($operator),
                            "` for scalar field `",
                            stringify!(#ident),
                            "` in update helper for model `",
                            #model_name,
                            "`; only `eq`, `null`, and `{ not: null }` are currently supported"
                        ))
                    }};
                    (#ident : $value:tt) => {{
                        compile_error!(concat!(
                            "malformed filter for scalar field `",
                            stringify!(#ident),
                            "` in update helper for model `",
                            #model_name,
                            "`; expected `null`, `{ eq: ... }`, or `{ not: null }`"
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
                        "__vitrail_update_where_filter_{}_{}",
                        module_name,
                        target.name
                    );

                    Ok(quote! {
                        (#ident : { }) => {{
                            compile_error!(concat!(
                                "relation field `",
                                stringify!(#ident),
                                "` in update helper for model `",
                                #model_name,
                                "` requires a nested filter object"
                            ))
                        }};
                        (#ident : null) => {{
                            compile_error!(concat!(
                                "relation field `",
                                stringify!(#ident),
                                "` in update helper for model `",
                                #model_name,
                                "` cannot use scalar null filter; provide a nested filter object instead"
                            ))
                        }};
                        (#ident : { eq : $value:expr $(,)? }) => {{
                            compile_error!(concat!(
                                "relation field `",
                                stringify!(#ident),
                                "` in update helper for model `",
                                #model_name,
                                "` cannot use scalar operator `eq`; provide a nested filter object instead"
                            ))
                        }};
                        (#ident : { not : null $(,)? }) => {{
                            compile_error!(concat!(
                                "relation field `",
                                stringify!(#ident),
                                "` in update helper for model `",
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
                                "` in update helper for model `",
                                #model_name,
                                "`; expected a nested object like `{ nested_field: null }`, `{ nested_field: { eq: ... } }`, or `{ nested_field: { not: null } }`"
                            ))
                        }};
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            helpers.extend(quote! {
                #[doc(hidden)]
                #[macro_export]
                macro_rules! #data_assert_ident {
                    #(#data_assert_arms)*
                    #(#relation_data_arms)*
                    ($other:ident) => {
                        compile_error!(concat!(
                            "unknown field `",
                            stringify!($other),
                            "` in update data for model `",
                            #model_name,
                            "`"
                        ));
                    };
                }

                #[doc(hidden)]
                #[macro_export]
                macro_rules! #data_type_assert_ident {
                    #(#data_type_assert_arms)*
                    #(#relation_data_type_assert_arms)*
                    ($ty:ty, $other:ident) => {
                        compile_error!(concat!(
                            "unknown field `",
                            stringify!($other),
                            "` in update data for model `",
                            #model_name,
                            "`"
                        ));
                    };
                }

                #[doc(hidden)]
                #[macro_export]
                macro_rules! #where_path_assert_ident {
                    #(#scalar_where_path_arms)*
                    #(#relation_where_path_arms)*
                    ($other:ident $(. $rest:ident)*) => {
                        compile_error!(concat!(
                            "unknown field `",
                            stringify!($other),
                            "` in update `where(...)` path for model `",
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
                            "` in update helper `where` for model `",
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
                            "empty `where` blocks are not supported in update helper for model `",
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
                #[macro_export]
                macro_rules! #data_struct_macro_ident {
                    #(#data_struct_arms)*
                    (
                        @struct
                        $data_ident:ident
                        [ $($fields:tt)* ]
                        [ $other:ident : $value:expr, $($rest_field:ident : $rest_value:expr,)* ]
                    ) => {
                        #data_assert_ident!($other);
                        #data_struct_macro_ident! {
                            @struct
                            $data_ident
                            [ $($fields)* ]
                            [ $($rest_field : $rest_value,)* ]
                        }
                    };
                    (
                        $data_ident:ident;
                        { $($data_field:ident : $data_value:expr),* $(,)? }
                    ) => {
                        #data_struct_macro_ident! {
                            @struct
                            $data_ident
                            [ ]
                            [ $($data_field : $data_value,)* ]
                        }
                    };
                    (
                        @struct
                        $data_ident:ident
                        [ $($fields:tt)* ]
                        [ ]
                    ) => {
                        #[allow(dead_code)]
                        #[derive(::vitrail_pg::UpdateData)]
                        #[vitrail(schema = #dollar_crate::#module_name::Schema, model = #model_name)]
                        struct $data_ident {
                            $($fields)*
                        }
                    };
                }

                #[doc(hidden)]
                #[macro_export]
                macro_rules! #data_value_macro_ident {
                    #(#data_value_arms)*
                    (
                        @value
                        $data_ident:ident
                        [ $($bindings:tt)* ]
                        [ $($initializers:tt)* ]
                        [ $other:ident : $value:expr, $($rest_field:ident : $rest_value:expr,)* ]
                    ) => {
                        #data_value_macro_ident! {
                            @value
                            $data_ident
                            [ $($bindings)* ]
                            [ $($initializers)* ]
                            [ $($rest_field : $rest_value,)* ]
                        }
                    };
                    (
                        @value
                        $data_ident:ident
                        [ $($bindings:tt)* ]
                        [ $($initializers:tt)* ]
                        [ ]
                    ) => {{
                        $($bindings)*
                        $data_ident {
                            $($initializers)*
                        }
                    }};
                    (
                        $data_ident:ident;
                        { $($data_field:ident : $data_value:expr),* $(,)? }
                    ) => {{
                        #data_value_macro_ident! {
                            @value
                            $data_ident
                            [ ]
                            [ ]
                            [ $($data_field : $data_value,)* ]
                        }
                    }};
                }

                #[doc(hidden)]
                pub mod #trait_module_ident {
                    #[allow(non_camel_case_types)]
                    #[doc(hidden)]
                    pub trait __VitrailUpdateDataModel {}

                    #(#data_traits)*
                }
            });

            main_arms.push(quote! {
                (
                    #model_ident {
                        data: {
                            $($data_field:ident : $data_value:expr),* $(,)?
                        },
                        where: {
                            $($where_field:ident : $where_value:tt),* $(,)?
                        }
                        $(,)?
                    }
                ) => {{
                    #data_struct_macro_ident! {
                        #root_data_ident;
                        { $($data_field : $data_value),* }
                    }

                    #[allow(dead_code)]
                    struct #root_update_ident;

                    impl ::vitrail_pg::UpdateManyModel for #root_update_ident {
                        type Schema = #dollar_crate::#module_name::Schema;
                        type Values = #root_data_ident;
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

                    #dollar_crate::#module_name::update_many::<#root_update_ident>(#data_value_macro_ident! {
                        #root_data_ident;
                        { $($data_field : $data_value),* }
                    })
                }};

                (
                    #model_ident {
                        data: {
                            $($data_field:ident : $data_value:expr),* $(,)?
                        }
                        $(,)?
                    }
                ) => {{
                    #data_struct_macro_ident! {
                        #root_data_ident;
                        { $($data_field : $data_value),* }
                    }

                    #[allow(dead_code)]
                    struct #root_update_ident;

                    impl ::vitrail_pg::UpdateManyModel for #root_update_ident {
                        type Schema = #dollar_crate::#module_name::Schema;
                        type Values = #root_data_ident;
                        type Variables = ();

                        fn model_name() -> &'static str {
                            #model_name
                        }
                    }

                    #dollar_crate::#module_name::update_many::<#root_update_ident>(#data_value_macro_ident! {
                        #root_data_ident;
                        { $($data_field : $data_value),* }
                    })
                }};
            });
        }

        helpers.extend(quote! {
            #[doc(hidden)]
            macro_rules! #local_main_macro_ident {
                #(#main_arms)*
                ($($tokens:tt)*) => {
                    compile_error!("unsupported update shape");
                };
            }

            #[doc(hidden)]
            #[macro_export(local_inner_macros)]
            macro_rules! #main_macro_ident {
                #(#main_arms)*
                ($($tokens:tt)*) => {
                    compile_error!("unsupported update shape");
                };
            }
        });

        Ok(helpers)
    }
}
