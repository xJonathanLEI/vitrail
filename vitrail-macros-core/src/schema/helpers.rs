use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::Result;

use vitrail_core::schema::Dialect;

use super::{ParsedSchema, SchemaMacroConfig, rust_type_alias_items};

impl ParsedSchema {
    pub(super) fn generate_named_schema<D: Dialect>(
        &self,
        config: &SchemaMacroConfig<D>,
    ) -> Result<TokenStream2> {
        let runtime_path = config.runtime_path();
        let operation_families = config.operation_families();
        let module_name = &self.module_name;
        let schema = self.generate_schema(config)?;
        let query_helper_macros = if operation_families.query() {
            self.generate_query_helper_macros(module_name, config)?
        } else {
            TokenStream2::new()
        };
        let insert_helper_items = if operation_families.insert() {
            self.generate_insert_helper_items(module_name, config)?
        } else {
            TokenStream2::new()
        };
        let delete_helper_items = if operation_families.delete() {
            self.generate_delete_helper_items(module_name, config)?
        } else {
            TokenStream2::new()
        };
        let update_helper_items = if operation_families.update() {
            self.generate_update_helper_items(module_name, config)?
        } else {
            TokenStream2::new()
        };
        let exported_query_macro_ident = format_ident!("__vitrail_query_{}", module_name);
        let exported_insert_macro_ident = format_ident!("__vitrail_insert_{}", module_name);
        let exported_delete_macro_ident = format_ident!("__vitrail_delete_{}", module_name);
        let exported_update_macro_ident = format_ident!("__vitrail_update_{}", module_name);
        let rust_type_alias_modules = if operation_families.any() {
            self.models
                .iter()
                .map(|model| rust_type_alias_items(module_name, model))
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let query_macro_reexport = if operation_families.query() {
            quote! {
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #exported_query_macro_ident;
            }
        } else {
            TokenStream2::new()
        };
        let insert_macro_reexport = if operation_families.insert() {
            quote! {
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #exported_insert_macro_ident;
            }
        } else {
            TokenStream2::new()
        };
        let delete_macro_reexport = if operation_families.delete() {
            quote! {
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #exported_delete_macro_ident;
            }
        } else {
            TokenStream2::new()
        };
        let update_macro_reexport = if operation_families.update() {
            quote! {
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #exported_update_macro_ident;
            }
        } else {
            TokenStream2::new()
        };
        let top_level_macro_reexports = quote! {
            #query_macro_reexport
            #insert_macro_reexport
            #delete_macro_reexport
            #update_macro_reexport
        };
        let query_macro_reexports = self.models.iter().map(|model| {
            let select_assert_ident =
                format_ident!("__vitrail_assert_select_{}_{}", module_name, model.name);
            let include_assert_ident =
                format_ident!("__vitrail_assert_include_{}_{}", module_name, model.name);
            let where_path_assert_ident = format_ident!(
                "__vitrail_assert_query_where_path_{}_{}",
                module_name,
                model.name
            );
            let where_filter_value_assert_ident = format_ident!(
                "__vitrail_assert_query_filter_value_type_{}_{}",
                module_name,
                model.name
            );
            let where_filter_macro_ident = format_ident!(
                "__vitrail_query_where_filter_{}_{}",
                module_name,
                model.name
            );
            let where_field_filter_ident = format_ident!(
                "__vitrail_query_where_field_filter_{}_{}",
                module_name,
                model.name
            );
            let order_path_assert_ident = format_ident!(
                "__vitrail_assert_query_order_path_{}_{}",
                module_name,
                model.name
            );
            let order_entries_macro_ident = format_ident!(
                "__vitrail_query_order_entries_{}_{}",
                module_name,
                model.name
            );
            let order_field_entry_macro_ident = format_ident!(
                "__vitrail_query_order_field_entry_{}_{}",
                module_name,
                model.name
            );
            let include_struct_ident =
                format_ident!("__vitrail_include_struct_{}_{}", module_name, model.name);
            let include_selection_ident =
                format_ident!("__vitrail_include_selection_{}_{}", module_name, model.name);
            let root_struct_macro_ident =
                format_ident!("__vitrail_root_struct_{}_{}", module_name, model.name);
            let selection_macro_ident =
                format_ident!("__vitrail_selection_{}_{}", module_name, model.name);

            quote! {
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #select_assert_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #include_assert_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #where_path_assert_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #where_filter_value_assert_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #where_filter_macro_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #where_field_filter_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #order_path_assert_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #order_entries_macro_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #order_field_entry_macro_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #include_struct_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #include_selection_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #root_struct_macro_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #selection_macro_ident;
            }
        });
        let insert_macro_reexports = self.models.iter().map(|model| {
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
            let required_input_scanner_idents = model
                .scalar_fields()
                .iter()
                .filter(|field| !field.can_be_omitted_in_insert())
                .map(|field| {
                    format_ident!(
                        "__vitrail_scan_insert_input_field_{}_{}_{}",
                        module_name,
                        model.name,
                        field.name
                    )
                })
                .collect::<Vec<_>>();

            quote! {
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #input_assert_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #input_type_assert_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #input_complete_assert_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #result_assert_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #result_type_assert_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #input_struct_macro_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #result_struct_macro_ident;
                #(
                    #[doc(hidden)]
                    #[allow(unused_imports)]
                    pub use #required_input_scanner_idents;
                )*
            }
        });
        let delete_macro_reexports = self.models.iter().map(|model| {
            let where_path_assert_ident = format_ident!(
                "__vitrail_assert_delete_where_path_{}_{}",
                module_name,
                model.name
            );
            let where_filter_value_assert_ident = format_ident!(
                "__vitrail_assert_delete_filter_value_type_{}_{}",
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
            let where_field_variable_filter_macro_ident = format_ident!(
                "__vitrail_delete_where_field_variable_filter_{}_{}",
                module_name,
                model.name
            );
            let where_variable_filter_macro_ident = format_ident!(
                "__vitrail_delete_where_variable_filter_{}_{}",
                module_name,
                model.name
            );
            let where_variable_entries_macro_ident = format_ident!(
                "__vitrail_delete_where_variable_entries_{}_{}",
                module_name,
                model.name
            );
            let where_variables_macro_ident = format_ident!(
                "__vitrail_delete_where_variables_{}_{}",
                module_name,
                model.name
            );

            quote! {
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #where_path_assert_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #where_filter_value_assert_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #where_filter_macro_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #where_field_filter_macro_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #where_field_variable_filter_macro_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #where_variable_filter_macro_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #where_variable_entries_macro_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #where_variables_macro_ident;
            }
        });
        let update_macro_reexports = self.models.iter().map(|model| {
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
            let where_filter_value_assert_ident = format_ident!(
                "__vitrail_assert_update_filter_value_type_{}_{}",
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
            let where_field_variable_filter_macro_ident = format_ident!(
                "__vitrail_update_where_field_variable_filter_{}_{}",
                module_name,
                model.name
            );
            let where_variable_filter_macro_ident = format_ident!(
                "__vitrail_update_where_variable_filter_{}_{}",
                module_name,
                model.name
            );
            let where_variable_entries_macro_ident = format_ident!(
                "__vitrail_update_where_variable_entries_{}_{}",
                module_name,
                model.name
            );
            let where_variables_macro_ident = format_ident!(
                "__vitrail_update_where_variables_{}_{}",
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

            quote! {
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #data_assert_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #data_type_assert_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #where_path_assert_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #where_filter_value_assert_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #where_filter_macro_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #where_field_filter_macro_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #where_field_variable_filter_macro_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #where_variable_filter_macro_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #where_variable_entries_macro_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #where_variables_macro_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #data_struct_macro_ident;
                #[doc(hidden)]
                #[allow(unused_imports)]
                pub use #data_value_macro_ident;
            }
        });
        let query_trait_reexports = self.models.iter().map(|model| {
            let trait_module_ident =
                format_ident!("__vitrail_query_traits_{}_{}", module_name, model.name);
            let filter_trait_module_ident = format_ident!(
                "__vitrail_query_filter_traits_{}_{}",
                module_name,
                model.name
            );
            quote! {
                #[doc(hidden)]
                pub use super::#trait_module_ident;
                #[doc(hidden)]
                pub use super::#filter_trait_module_ident;
            }
        });
        let insert_trait_reexports = self.models.iter().map(|model| {
            let trait_module_ident =
                format_ident!("__vitrail_insert_traits_{}_{}", module_name, model.name);
            quote! {
                #[doc(hidden)]
                pub use super::#trait_module_ident;
            }
        });
        let delete_trait_reexports = self.models.iter().map(|model| {
            let trait_module_ident =
                format_ident!("__vitrail_delete_traits_{}_{}", module_name, model.name);
            let filter_trait_module_ident = format_ident!(
                "__vitrail_delete_filter_traits_{}_{}",
                module_name,
                model.name
            );
            quote! {
                #[doc(hidden)]
                pub use super::#trait_module_ident;
                #[doc(hidden)]
                pub use super::#filter_trait_module_ident;
            }
        });
        let update_trait_reexports = self.models.iter().map(|model| {
            let trait_module_ident =
                format_ident!("__vitrail_update_traits_{}_{}", module_name, model.name);
            let filter_trait_module_ident = format_ident!(
                "__vitrail_update_filter_traits_{}_{}",
                module_name,
                model.name
            );
            quote! {
                #[doc(hidden)]
                pub use super::#trait_module_ident;
                #[doc(hidden)]
                pub use super::#filter_trait_module_ident;
            }
        });

        let query_macro_reexports = if operation_families.query() {
            quote! { #(#query_macro_reexports)* }
        } else {
            TokenStream2::new()
        };
        let insert_macro_reexports = if operation_families.insert() {
            quote! { #(#insert_macro_reexports)* }
        } else {
            TokenStream2::new()
        };
        let delete_macro_reexports = if operation_families.delete() {
            quote! { #(#delete_macro_reexports)* }
        } else {
            TokenStream2::new()
        };
        let update_macro_reexports = if operation_families.update() {
            quote! { #(#update_macro_reexports)* }
        } else {
            TokenStream2::new()
        };
        let query_trait_reexports = if operation_families.query() {
            quote! { #(#query_trait_reexports)* }
        } else {
            TokenStream2::new()
        };
        let insert_trait_reexports = if operation_families.insert() {
            quote! { #(#insert_trait_reexports)* }
        } else {
            TokenStream2::new()
        };
        let delete_trait_reexports = if operation_families.delete() {
            quote! { #(#delete_trait_reexports)* }
        } else {
            TokenStream2::new()
        };
        let update_trait_reexports = if operation_families.update() {
            quote! { #(#update_trait_reexports)* }
        } else {
            TokenStream2::new()
        };

        let query_functions = if operation_families.query() {
            quote! {
                pub fn query<T>() -> #runtime_path::Query<Schema, T>
                where
                    T: #runtime_path::QueryModel<Schema = Schema, Variables = ()> + Sync,
                {
                    #runtime_path::Query::new()
                }

                pub fn query_with_variables<T>(
                    variables: T::Variables,
                ) -> #runtime_path::Query<Schema, T, T::Variables>
                where
                    T: #runtime_path::QueryModel<Schema = Schema> + Sync,
                {
                    #runtime_path::Query::<Schema, T, ()>::new_with_variables(variables)
                }
            }
        } else {
            TokenStream2::new()
        };

        let insert_functions = if operation_families.insert() {
            quote! {
                pub fn insert<T>(values: T::Values) -> #runtime_path::Insert<Schema, T>
                where
                    T: #runtime_path::InsertModel<Schema = Schema>,
                {
                    #runtime_path::Insert::new(values)
                }
            }
        } else {
            TokenStream2::new()
        };

        let delete_functions = if operation_families.delete() {
            quote! {
                pub fn delete_many<T>() -> #runtime_path::DeleteMany<Schema, T>
                where
                    T: #runtime_path::DeleteManyModel<Schema = Schema, Variables = ()>,
                {
                    #runtime_path::DeleteMany::new()
                }

                pub fn delete_many_with_variables<T>(
                    variables: T::Variables,
                ) -> #runtime_path::DeleteMany<Schema, T, T::Variables>
                where
                    T: #runtime_path::DeleteManyModel<Schema = Schema>,
                {
                    #runtime_path::DeleteMany::<Schema, T, ()>::new_with_variables(variables)
                }
            }
        } else {
            TokenStream2::new()
        };

        let update_functions = if operation_families.update() {
            quote! {
                pub fn update_many<T>(values: T::Values) -> #runtime_path::UpdateMany<Schema, T>
                where
                    T: #runtime_path::UpdateManyModel<Schema = Schema, Variables = ()>,
                {
                    #runtime_path::UpdateMany::new(values)
                }

                pub fn update_many_with_variables<T>(
                    variables: T::Variables,
                    values: T::Values,
                ) -> #runtime_path::UpdateMany<Schema, T, T::Variables>
                where
                    T: #runtime_path::UpdateManyModel<Schema = Schema>,
                {
                    #runtime_path::UpdateMany::<Schema, T, ()>::new_with_variables(variables, values)
                }
            }
        } else {
            TokenStream2::new()
        };

        Ok(quote! {
            #query_helper_macros
            #insert_helper_items
            #delete_helper_items
            #update_helper_items

            pub mod #module_name {
                static __SCHEMA: ::std::sync::OnceLock<#runtime_path::Schema> =
                    ::std::sync::OnceLock::new();

                #[derive(Clone, Copy, Debug, Default)]
                pub struct Schema;

                impl #runtime_path::SchemaAccess for Schema {
                    fn schema() -> &'static #runtime_path::Schema {
                        __SCHEMA.get_or_init(|| #schema)
                    }
                }

                #query_functions
                #insert_functions
                #delete_functions
                #update_functions

                #(#rust_type_alias_modules)*
                #top_level_macro_reexports
                #query_macro_reexports
                #insert_macro_reexports
                #delete_macro_reexports
                #update_macro_reexports
                #query_trait_reexports
                #insert_trait_reexports
                #delete_trait_reexports
                #update_trait_reexports

            }
        })
    }

    fn generate_schema<D: Dialect>(&self, config: &SchemaMacroConfig<D>) -> Result<TokenStream2> {
        let runtime_path = config.runtime_path();
        let mut models = Vec::with_capacity(self.models.len());
        let external_tables = self
            .external_tables
            .iter()
            .map(|table| syn::LitStr::new(&table.value(), table.span()))
            .collect::<Vec<_>>();

        for model in &self.models {
            models.push(model.generate_schema_model(config)?);
        }

        Ok(quote! {
            #runtime_path::Schema::builder()
                .models(vec![#(#models),*])
                .external_tables(vec![#(::std::string::String::from(#external_tables)),*])
                .build()
                .expect("schema was validated during macro expansion")
        })
    }
}
