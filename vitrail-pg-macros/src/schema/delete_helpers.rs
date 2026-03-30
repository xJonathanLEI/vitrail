use proc_macro2::{Ident, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{LitStr, Result};

use super::{
    ParsedSchema, dollar_crate, filter_helpers::generate_filter_helper_items, to_pascal_case,
};

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

            let filter_helper_items = generate_filter_helper_items(
                self,
                module_name,
                model,
                "delete",
                &where_path_assert_ident,
                &where_filter_macro_ident,
                &where_field_filter_macro_ident,
            )?;

            helpers.extend(quote! {
                #filter_helper_items

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
