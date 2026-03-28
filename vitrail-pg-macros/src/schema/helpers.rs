use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{Error, Result};

use super::{ParsedField, ParsedModel, ParsedSchema};

impl ParsedSchema {
    pub(super) fn generate_named_schema(&self) -> Result<TokenStream2> {
        let module_name = &self.module_name;
        let schema = self.generate_schema()?;
        let query_helper_macros = self.generate_query_helper_macros(module_name)?;
        let insert_helper_items = self.generate_insert_helper_items(module_name)?;
        let update_helper_items = self.generate_update_helper_items(module_name)?;
        let local_query_macro_ident = format_ident!("__vitrail_query_local_{}", module_name);
        let local_insert_macro_ident = format_ident!("__vitrail_insert_local_{}", module_name);
        let local_update_macro_ident = format_ident!("__vitrail_update_local_{}", module_name);
        let insert_trait_reexports = self.models.iter().map(|model| {
            let trait_module_ident =
                format_ident!("__vitrail_insert_traits_{}_{}", module_name, model.name);
            quote! {
                #[doc(hidden)]
                pub use super::#trait_module_ident;
            }
        });
        let update_trait_reexports = self.models.iter().map(|model| {
            let trait_module_ident =
                format_ident!("__vitrail_update_traits_{}_{}", module_name, model.name);
            quote! {
                #[doc(hidden)]
                pub use super::#trait_module_ident;
            }
        });

        Ok(quote! {
            #query_helper_macros
            #insert_helper_items
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

                #(#insert_trait_reexports)*
                #(#update_trait_reexports)*

                pub(crate) use #local_query_macro_ident as __query;
                pub(crate) use #local_insert_macro_ident as __insert;
                pub(crate) use #local_update_macro_ident as __update;
            }
        })
    }

    fn generate_schema(&self) -> Result<TokenStream2> {
        let mut models = Vec::with_capacity(self.models.len());

        for model in &self.models {
            models.push(model.generate_schema_model(self)?);
        }

        Ok(quote! {
            ::vitrail_pg::Schema::builder()
                .models(vec![#(#models),*])
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
