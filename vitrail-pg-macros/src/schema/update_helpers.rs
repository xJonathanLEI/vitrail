use proc_macro2::{Ident, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{LitStr, Result};

use super::{ParsedSchema, dollar_crate, rust_type_tokens, to_pascal_case};

impl ParsedSchema {
    pub(super) fn generate_update_helper_items(&self, module_name: &Ident) -> Result<TokenStream2> {
        let mut helpers = TokenStream2::new();
        let dollar_crate = dollar_crate();

        for model in &self.models {
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
            let trait_module_ident =
                format_ident!("__vitrail_update_traits_{}_{}", module_name, model.name);

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
                    let rust_ty = rust_type_tokens(&field.ty)?;

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
                pub mod #trait_module_ident {
                    #[allow(non_camel_case_types)]
                    #[doc(hidden)]
                    pub trait __VitrailUpdateDataModel {}

                    #(#data_traits)*
                }
            });
        }

        Ok(helpers)
    }
}
