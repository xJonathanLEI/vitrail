use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{Error, Result};

use super::{ParsedField, ParsedModel, ParsedSchema, rust_type_alias_items};

impl ParsedSchema {
    pub(super) fn generate_named_schema(&self) -> Result<TokenStream2> {
        let module_name = &self.module_name;
        let schema = self.generate_schema()?;
        let query_helper_macros = self.generate_query_helper_macros(module_name)?;
        let insert_helper_items = self.generate_insert_helper_items(module_name)?;
        let delete_helper_items = self.generate_delete_helper_items(module_name)?;
        let update_helper_items = self.generate_update_helper_items(module_name)?;
        let exported_query_macro_ident = format_ident!("__vitrail_query_{}", module_name);
        let exported_insert_macro_ident = format_ident!("__vitrail_insert_{}", module_name);
        let exported_delete_macro_ident = format_ident!("__vitrail_delete_{}", module_name);
        let exported_update_macro_ident = format_ident!("__vitrail_update_{}", module_name);
        let rust_type_alias_modules = self
            .models
            .iter()
            .map(|model| rust_type_alias_items(module_name, model));
        let top_level_macro_reexports = quote! {
            #[doc(hidden)]
            #[allow(unused_imports)]
            pub use #exported_query_macro_ident;
            #[doc(hidden)]
            #[allow(unused_imports)]
            pub use #exported_insert_macro_ident;
            #[doc(hidden)]
            #[allow(unused_imports)]
            pub use #exported_delete_macro_ident;
            #[doc(hidden)]
            #[allow(unused_imports)]
            pub use #exported_update_macro_ident;
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

        Ok(quote! {
            #query_helper_macros
            #insert_helper_items
            #delete_helper_items
            #update_helper_items

            pub mod #module_name {
                static __SCHEMA: ::std::sync::OnceLock<::vitrail_pg::Schema> =
                    ::std::sync::OnceLock::new();

                #[derive(Clone, Copy, Debug, Default)]
                pub struct Schema;

                impl ::vitrail_pg::SchemaAccess for Schema {
                    fn schema() -> &'static ::vitrail_pg::Schema {
                        __SCHEMA.get_or_init(|| #schema)
                    }
                }

                pub fn query<T>() -> ::vitrail_pg::Query<Schema, T>
                where
                    T: ::vitrail_pg::QueryModel<Schema = Schema, Variables = ()> + Sync,
                {
                    ::vitrail_pg::Query::new()
                }

                pub fn query_with_variables<T>(
                    variables: T::Variables,
                ) -> ::vitrail_pg::Query<Schema, T, T::Variables>
                where
                    T: ::vitrail_pg::QueryModel<Schema = Schema> + Sync,
                {
                    ::vitrail_pg::Query::<Schema, T, ()>::new_with_variables(variables)
                }

                pub fn insert<T>(values: T::Values) -> ::vitrail_pg::Insert<Schema, T>
                where
                    T: ::vitrail_pg::InsertModel<Schema = Schema>,
                {
                    ::vitrail_pg::Insert::new(values)
                }

                pub fn delete_many<T>() -> ::vitrail_pg::DeleteMany<Schema, T>
                where
                    T: ::vitrail_pg::DeleteManyModel<Schema = Schema, Variables = ()>,
                {
                    ::vitrail_pg::DeleteMany::new()
                }

                pub fn delete_many_with_variables<T>(
                    variables: T::Variables,
                ) -> ::vitrail_pg::DeleteMany<Schema, T, T::Variables>
                where
                    T: ::vitrail_pg::DeleteManyModel<Schema = Schema>,
                {
                    ::vitrail_pg::DeleteMany::<Schema, T, ()>::new_with_variables(variables)
                }

                pub fn update_many<T>(values: T::Values) -> ::vitrail_pg::UpdateMany<Schema, T>
                where
                    T: ::vitrail_pg::UpdateManyModel<Schema = Schema, Variables = ()>,
                {
                    ::vitrail_pg::UpdateMany::new(values)
                }

                pub fn update_many_with_variables<T>(
                    variables: T::Variables,
                    values: T::Values,
                ) -> ::vitrail_pg::UpdateMany<Schema, T, T::Variables>
                where
                    T: ::vitrail_pg::UpdateManyModel<Schema = Schema>,
                {
                    ::vitrail_pg::UpdateMany::<Schema, T, ()>::new_with_variables(variables, values)
                }

                #(#rust_type_alias_modules)*
                #top_level_macro_reexports
                #(#query_macro_reexports)*
                #(#insert_macro_reexports)*
                #(#delete_macro_reexports)*
                #(#update_macro_reexports)*
                #(#query_trait_reexports)*
                #(#insert_trait_reexports)*
                #(#delete_trait_reexports)*
                #(#update_trait_reexports)*

            }
        })
    }

    fn generate_schema(&self) -> Result<TokenStream2> {
        let mut models = Vec::with_capacity(self.models.len());
        let external_tables = self
            .external_tables
            .iter()
            .map(|table| syn::LitStr::new(&table.value(), table.span()))
            .collect::<Vec<_>>();

        for model in &self.models {
            models.push(model.generate_schema_model(self)?);
        }

        Ok(quote! {
            ::vitrail_pg::Schema::builder()
                .models(vec![#(#models),*])
                .external_tables(vec![#(::std::string::String::from(#external_tables)),*])
                .build()
                .expect("schema was validated during macro expansion")
        })
    }

    fn find_model(&self, name: &str) -> Option<&ParsedModel> {
        self.models
            .iter()
            .find(|model| self.model_names_match(&model.name.to_string(), name))
    }

    fn model_names_match(&self, left: &str, right: &str) -> bool {
        left.eq_ignore_ascii_case(right)
    }

    fn infer_relation_fields(
        &self,
        model: &ParsedModel,
        field: &ParsedField,
        target_model: &ParsedModel,
    ) -> Result<(Vec<syn::LitStr>, Vec<syn::LitStr>)> {
        let reverse_relation = target_model
            .fields
            .iter()
            .find(|candidate| {
                self.model_names_match(&candidate.ty.name.to_string(), &model.name.to_string())
                    && candidate.relation().is_some()
            })
            .ok_or_else(|| {
                Error::new(
                    field.ty.name.span(),
                    format!(
                        "could not infer relation metadata for `{}.{}`",
                        model.name, field.name
                    ),
                )
            })?;

        let reverse_relation = reverse_relation
            .relation()
            .expect("reverse relation existence checked above");

        let local_fields = reverse_relation
            .references
            .iter()
            .map(|ident| syn::LitStr::new(&ident.to_string(), ident.span()))
            .collect::<Vec<_>>();
        let referenced_fields = reverse_relation
            .fields
            .iter()
            .map(|ident| syn::LitStr::new(&ident.to_string(), ident.span()))
            .collect::<Vec<_>>();

        Ok((local_fields, referenced_fields))
    }

    pub(super) fn generate_relation_attribute(
        &self,
        model: &ParsedModel,
        field: &ParsedField,
    ) -> Result<TokenStream2> {
        let (fields, references) = match field.relation() {
            Some(relation) => (
                relation
                    .fields
                    .iter()
                    .map(|ident| syn::LitStr::new(&ident.to_string(), ident.span()))
                    .collect::<Vec<_>>(),
                relation
                    .references
                    .iter()
                    .map(|ident| syn::LitStr::new(&ident.to_string(), ident.span()))
                    .collect::<Vec<_>>(),
            ),
            None => {
                let target_model =
                    self.find_model(&field.ty.name.to_string()).ok_or_else(|| {
                        Error::new(
                            field.ty.name.span(),
                            format!(
                                "unknown relation target model `{}` for field `{}`",
                                field.ty.name, field.name
                            ),
                        )
                    })?;

                self.infer_relation_fields(model, field, target_model)?
            }
        };

        Ok(quote! {
            ::vitrail_pg::Attribute::Relation(
                ::vitrail_pg::RelationAttribute::builder()
                    .fields(vec![#(#fields.to_owned()),*])
                    .references(vec![#(#references.to_owned()),*])
                    .build()
                    .expect("relation attribute was validated during macro expansion")
            )
        })
    }
}
