use proc_macro2::{Ident, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{LitStr, Result};

use super::{ParsedSchema, dollar_crate, rust_field_type_tokens, to_pascal_case};

impl ParsedSchema {
    pub(super) fn generate_insert_helper_items(&self, module_name: &Ident) -> Result<TokenStream2> {
        let main_macro_ident = format_ident!("__vitrail_insert_{}", module_name);
        let local_main_macro_ident = format_ident!("__vitrail_insert_local_{}", module_name);
        let mut helpers = TokenStream2::new();
        let mut main_arms = Vec::new();
        let dollar_crate = dollar_crate();

        for model in &self.models {
            let model_name = LitStr::new(&model.name.to_string(), model.name.span());
            let input_assert_ident = format_ident!(
                "__vitrail_assert_insert_input_field_{}_{}",
                module_name,
                model.name
            );
            let input_type_assert_ident = format_ident!(
                "__vitrail_assert_insert_input_type_{}_{}",
                module_name,
                model.name
            );
            let input_complete_assert_ident = format_ident!(
                "__vitrail_assert_insert_input_complete_{}_{}",
                module_name,
                model.name
            );
            let result_assert_ident = format_ident!(
                "__vitrail_assert_insert_result_field_{}_{}",
                module_name,
                model.name
            );
            let result_type_assert_ident = format_ident!(
                "__vitrail_assert_insert_result_type_{}_{}",
                module_name,
                model.name
            );
            let trait_module_ident =
                format_ident!("__vitrail_insert_traits_{}_{}", module_name, model.name);
            let input_struct_macro_ident = format_ident!(
                "__vitrail_insert_input_struct_{}_{}",
                module_name,
                model.name
            );
            let result_struct_macro_ident = format_ident!(
                "__vitrail_insert_result_struct_{}_{}",
                module_name,
                model.name
            );
            let root_input_ident = format_ident!(
                "__VitrailInsert{}Input",
                to_pascal_case(&model.name.to_string())
            );
            let root_result_ident = format_ident!(
                "__VitrailInsert{}Result",
                to_pascal_case(&model.name.to_string())
            );
            let model_ident = &model.name;

            let scalar_fields = model.scalar_fields();
            let relation_fields = model.relation_fields();
            let required_scalar_fields = scalar_fields
                .iter()
                .filter(|field| !field.can_be_omitted_in_insert())
                .copied()
                .collect::<Vec<_>>();
            let all_scalar_field_idents = scalar_fields.iter().map(|field| {
                let ident = &field.name;
                quote! { #ident }
            });

            let input_assert_arms = scalar_fields.iter().map(|field| {
                let ident = &field.name;
                quote! { (#ident) => {}; }
            });
            let result_assert_arms = scalar_fields.iter().map(|field| {
                let ident = &field.name;
                quote! { (#ident) => {}; }
            });
            let relation_input_arms = relation_fields.iter().map(|field| {
                let ident = &field.name;
                quote! {
                    (#ident) => {
                        compile_error!(concat!(
                            "relation field `",
                            stringify!(#ident),
                            "` cannot be used in insert input for model `",
                            #model_name,
                            "`"
                        ));
                    };
                }
            });
            let relation_input_type_assert_arms = relation_fields.iter().map(|field| {
                let ident = &field.name;
                quote! {
                    ($ty:ty, #ident) => {
                        compile_error!(concat!(
                            "relation field `",
                            stringify!(#ident),
                            "` cannot be used in insert input for model `",
                            #model_name,
                            "`"
                        ));
                    };
                }
            });
            let relation_result_arms = relation_fields.iter().map(|field| {
                let ident = &field.name;
                quote! {
                    (#ident) => {
                        compile_error!(concat!(
                            "relation field `",
                            stringify!(#ident),
                            "` cannot be returned from scalar insert for model `",
                            #model_name,
                            "`"
                        ));
                    };
                }
            });
            let relation_result_type_assert_arms = relation_fields.iter().map(|field| {
                let ident = &field.name;
                quote! {
                    ($ty:ty, #ident) => {
                        compile_error!(concat!(
                            "relation field `",
                            stringify!(#ident),
                            "` cannot be returned from scalar insert for model `",
                            #model_name,
                            "`"
                        ));
                    };
                }
            });

            let input_traits = scalar_fields
                .iter()
                .map(|field| {
                    let trait_ident = format_ident!(
                        "__VitrailInsertInputType_{}_{}_{}",
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

            let result_traits = scalar_fields
                .iter()
                .map(|field| {
                    let trait_ident = format_ident!(
                        "__VitrailInsertResultType_{}_{}_{}",
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

            let required_input_scanner_idents = required_scalar_fields
                .iter()
                .map(|field| {
                    format_ident!(
                        "__vitrail_scan_insert_input_field_{}_{}_{}",
                        module_name,
                        model.name,
                        field.name
                    )
                })
                .collect::<Vec<_>>();
            let required_input_scanner_defs = required_scalar_fields
                .iter()
                .zip(required_input_scanner_idents.iter())
                .map(|(field, scanner_ident)| {
                    let ident = &field.name;

                    quote! {
                        #[doc(hidden)]
                        #[macro_export]
                        macro_rules! #scanner_ident {
                            (#ident $(, $rest:ident)*) => {};
                            ($other:ident $(, $rest:ident)*) => {
                                #scanner_ident!($($rest),*);
                            };
                            () => {
                                compile_error!(concat!(
                                    "missing required field `",
                                    stringify!(#ident),
                                    "` in insert input for model `",
                                    #model_name,
                                    "`"
                                ));
                            };
                        }
                    }
                })
                .collect::<Vec<_>>();

            let input_type_assert_arms = scalar_fields
                .iter()
                .map(|field| {
                    let ident = &field.name;
                    let trait_ident = format_ident!(
                        "__VitrailInsertInputType_{}_{}_{}",
                        module_name,
                        model.name,
                        field.name
                    );

                    Ok(quote! {
                        ($ty:ty, #ident) => {
                            {
                                fn __vitrail_assert_insert_input_field_type<
                                    T: #dollar_crate::#module_name::#trait_module_ident::#trait_ident
                                >() {}
                                __vitrail_assert_insert_input_field_type::<$ty>();
                            }
                        };
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            let result_type_assert_arms = scalar_fields
                .iter()
                .map(|field| {
                    let ident = &field.name;
                    let trait_ident = format_ident!(
                        "__VitrailInsertResultType_{}_{}_{}",
                        module_name,
                        model.name,
                        field.name
                    );

                    Ok(quote! {
                        ($ty:ty, #ident) => {
                            {
                                fn __vitrail_assert_insert_result_field_type<
                                    T: #dollar_crate::#module_name::#trait_module_ident::#trait_ident
                                >() {}
                                __vitrail_assert_insert_result_field_type::<$ty>();
                            }
                        };
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            let input_struct_arms = scalar_fields
                .iter()
                .map(|field| {
                    let ident = &field.name;
                    let ty = rust_field_type_tokens(field)?;

                    Ok(quote! {
                        (
                            @struct
                            $input_ident:ident
                            [ $($fields:tt)* ]
                            [ #ident : $value:expr, $($rest_field:ident : $rest_value:expr,)* ]
                        ) => {
                            #input_struct_macro_ident! {
                                @struct
                                $input_ident
                                [ $($fields)* pub #ident: #ty, ]
                                [ $($rest_field : $rest_value,)* ]
                            }
                        };
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            let result_struct_arms = scalar_fields
                .iter()
                .map(|field| {
                    let ident = &field.name;
                    let ty = rust_field_type_tokens(field)?;

                    Ok(quote! {
                        (
                            @struct
                            $result_ident:ident
                            [ $($fields:tt)* ]
                            [ #ident, $($rest_field:ident,)* ]
                            [ $input_ident:ident ]
                        ) => {
                            #result_struct_macro_ident! {
                                @struct
                                $result_ident
                                [ $($fields)* pub #ident: #ty, ]
                                [ $($rest_field,)* ]
                                [ $input_ident ]
                            }
                        };
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            helpers.extend(quote! {
                #[doc(hidden)]
                #[macro_export]
                macro_rules! #input_assert_ident {
                    #(#input_assert_arms)*
                    #(#relation_input_arms)*
                    ($other:ident) => {
                        compile_error!(concat!(
                            "unknown field `",
                            stringify!($other),
                            "` in insert input for model `",
                            #model_name,
                            "`"
                        ));
                    };
                }

                #[doc(hidden)]
                #[macro_export]
                macro_rules! #result_assert_ident {
                    #(#result_assert_arms)*
                    #(#relation_result_arms)*
                    ($other:ident) => {
                        compile_error!(concat!(
                            "unknown field `",
                            stringify!($other),
                            "` in insert result for model `",
                            #model_name,
                            "`"
                        ));
                    };
                }

                #[doc(hidden)]
                #[macro_export]
                macro_rules! #input_type_assert_ident {
                    #(#input_type_assert_arms)*
                    #(#relation_input_type_assert_arms)*
                    ($ty:ty, $other:ident) => {
                        compile_error!(concat!(
                            "unknown field `",
                            stringify!($other),
                            "` in insert input for model `",
                            #model_name,
                            "`"
                        ));
                    };
                }

                #[doc(hidden)]
                #[macro_export]
                macro_rules! #result_type_assert_ident {
                    #(#result_type_assert_arms)*
                    #(#relation_result_type_assert_arms)*
                    ($ty:ty, $other:ident) => {
                        compile_error!(concat!(
                            "unknown field `",
                            stringify!($other),
                            "` in insert result for model `",
                            #model_name,
                            "`"
                        ));
                    };
                }

                #(#required_input_scanner_defs)*

                #[doc(hidden)]
                #[macro_export]
                macro_rules! #input_complete_assert_ident {
                    ( $($provided:ident),* $(,)? ) => {
                        #( #required_input_scanner_idents!($($provided),*); )*
                    };
                }

                #[doc(hidden)]
                pub mod #trait_module_ident {
                    #[allow(non_camel_case_types)]
                    #[doc(hidden)]
                    pub trait __VitrailInsertInputModel {}

                    #(#input_traits)*
                    #(#result_traits)*
                }

                #[doc(hidden)]
                #[macro_export]
                macro_rules! #input_struct_macro_ident {
                    #(#input_struct_arms)*
                    (
                        $input_ident:ident;
                        { $($data_field:ident : $data_value:expr),* $(,)? }
                    ) => {
                        $( #input_assert_ident!($data_field); )*
                        #input_complete_assert_ident!($($data_field),*);

                        #input_struct_macro_ident! {
                            @struct
                            $input_ident
                            [ ]
                            [ $($data_field : $data_value,)* ]
                        }
                    };
                    (
                        @struct
                        $input_ident:ident
                        [ $($fields:tt)* ]
                        [ ]
                    ) => {
                        #[allow(dead_code)]
                        #[derive(::vitrail_pg::InsertInput)]
                        #[vitrail(schema = #dollar_crate::#module_name::Schema, model = #model_name)]
                        struct $input_ident {
                            $($fields)*
                        }
                    };
                }

                #[doc(hidden)]
                #[macro_export]
                macro_rules! #result_struct_macro_ident {
                    #(#result_struct_arms)*
                    (
                        $result_ident:ident;
                        $input_ident:ident;
                        { $($select_field:ident : true),* $(,)? }
                    ) => {
                        $( #result_assert_ident!($select_field); )*

                        #result_struct_macro_ident! {
                            @struct
                            $result_ident
                            [ ]
                            [ $($select_field,)* ]
                            [ $input_ident ]
                        }
                    };
                    (
                        @struct
                        $result_ident:ident
                        [ $($fields:tt)* ]
                        [ ]
                        [ $input_ident:ident ]
                    ) => {
                        #[allow(dead_code)]
                        #[derive(::vitrail_pg::InsertResult)]
                        #[vitrail(
                            schema = #dollar_crate::#module_name::Schema,
                            model = #model_name,
                            input = $input_ident
                        )]
                        struct $result_ident {
                            $($fields)*
                        }
                    };
                }
            });

            main_arms.push(quote! {
                (
                    #model_ident {
                        data: {
                            $($data_field:ident : $data_value:expr),* $(,)?
                        },
                        select: {
                            $($select_field:ident : true),* $(,)?
                        }
                        $(,)?
                    }
                ) => {{
                    #input_struct_macro_ident! {
                        #root_input_ident;
                        { $($data_field : $data_value),* }
                    }

                    #result_struct_macro_ident! {
                        #root_result_ident;
                        #root_input_ident;
                        { $($select_field : true),* }
                    }

                    #dollar_crate::#module_name::insert::<#root_result_ident>(#root_input_ident {
                        $($data_field : $data_value),*
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
                    #input_struct_macro_ident! {
                        #root_input_ident;
                        { $($data_field : $data_value),* }
                    }

                    #result_struct_macro_ident! {
                        #root_result_ident;
                        #root_input_ident;
                        { #( #all_scalar_field_idents : true ),* }
                    }

                    #dollar_crate::#module_name::insert::<#root_result_ident>(#root_input_ident {
                        $($data_field : $data_value),*
                    })
                }};
            });
        }

        helpers.extend(quote! {
            #[doc(hidden)]
            macro_rules! #local_main_macro_ident {
                #(#main_arms)*
                ($($tokens:tt)*) => {
                    compile_error!("unsupported insert shape");
                };
            }

            #[doc(hidden)]
            #[macro_export(local_inner_macros)]
            macro_rules! #main_macro_ident {
                #(#main_arms)*
                ($($tokens:tt)*) => {
                    compile_error!("unsupported insert shape");
                };
            }
        });

        Ok(helpers)
    }
}
