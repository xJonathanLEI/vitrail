use proc_macro2::{Ident, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{LitStr, Result};

use vitrail_core::schema::Dialect;

use super::{
    ParsedSchema, SchemaMacroConfig,
    filter_helpers::{FilterHelperIdents, generate_filter_helper_items},
    to_pascal_case,
};

impl ParsedSchema {
    pub(super) fn generate_delete_helper_items<D: Dialect>(
        &self,
        module_name: &Ident,
        config: &SchemaMacroConfig<D>,
    ) -> Result<TokenStream2> {
        let runtime_path = config.runtime_path();
        let main_macro_ident = format_ident!("__vitrail_delete_{}", module_name);
        let mut helpers = TokenStream2::new();
        let mut main_arms = Vec::new();

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
            let where_variable_filter_macro_ident = format_ident!(
                "__vitrail_delete_where_variable_filter_{}_{}",
                module_name,
                model.name
            );
            let where_variables_macro_ident = format_ident!(
                "__vitrail_delete_where_variables_{}_{}",
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
                FilterHelperIdents {
                    where_path_assert_ident: &where_path_assert_ident,
                    where_filter_macro_ident: &where_filter_macro_ident,
                    where_field_filter_macro_ident: &where_field_filter_macro_ident,
                },
                config,
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
                    #[derive(#runtime_path::DeleteMany)]
                    #[vitrail(
                        schema = #module_name::Schema,
                        model = #model_name,
                        variables = #runtime_path::QueryVariables,
                        __helper_filter = #module_name::#where_variable_filter_macro_ident!({
                            $($where_field : $where_value),*
                        })
                    )]
                    struct #root_delete_ident;

                    #module_name::delete_many_with_variables::<#root_delete_ident>(
                        #module_name::#where_variables_macro_ident!({
                            $($where_field : $where_value),*
                        })
                    )
                }};

                (
                    #model_ident { $(,)? }
                ) => {{
                    #[allow(dead_code)]
                    #[derive(#runtime_path::DeleteMany)]
                    #[vitrail(
                        schema = #module_name::Schema,
                        model = #model_name
                    )]
                    struct #root_delete_ident;

                    #module_name::delete_many::<#root_delete_ident>()
                }};
            });
        }

        helpers.extend(quote! {
            #[doc(hidden)]
            #[macro_export]
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
