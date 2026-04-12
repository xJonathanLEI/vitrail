use proc_macro2::{Ident, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{LitStr, Result};

use super::{
    ParsedSchema, dollar_crate, filter_helpers::generate_filter_helper_items,
    order_helpers::generate_order_helper_items, rust_field_type_tokens,
    schema_owned_rust_field_type_tokens, to_pascal_case,
};

impl ParsedSchema {
    pub(super) fn generate_query_helper_macros(&self, module_name: &Ident) -> Result<TokenStream2> {
        let main_macro_ident = format_ident!("__vitrail_query_{}", module_name);
        let mut helpers = TokenStream2::new();
        let mut main_arms = Vec::new();
        let dollar_crate = dollar_crate();

        for model in &self.models {
            let model_ident = &model.name;
            let model_name = LitStr::new(&model.name.to_string(), model.name.span());
            let root_struct_ident =
                format_ident!("__VitrailQuery{}", to_pascal_case(&model.name.to_string()));
            let root_struct_macro_ident =
                format_ident!("__vitrail_root_struct_{}_{}", module_name, model.name);
            let selection_macro_ident =
                format_ident!("__vitrail_selection_{}_{}", module_name, model.name);
            let select_assert_ident =
                format_ident!("__vitrail_assert_select_{}_{}", module_name, model.name);
            let include_assert_ident =
                format_ident!("__vitrail_assert_include_{}_{}", module_name, model.name);
            let where_path_assert_ident = format_ident!(
                "__vitrail_assert_query_where_path_{}_{}",
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
            let trait_module_ident =
                format_ident!("__vitrail_query_traits_{}_{}", module_name, model.name);

            let scalar_fields = model.scalar_fields();
            let relation_fields = model.relation_fields();

            let select_assert_arms = scalar_fields.iter().map(|field| {
                let ident = &field.name;
                quote! { (#ident) => {}; }
            });

            let include_assert_arms = relation_fields.iter().map(|field| {
                let ident = &field.name;
                quote! { (#ident) => {}; }
            });

            let filter_helper_items = generate_filter_helper_items(
                self,
                module_name,
                model,
                "query",
                &where_path_assert_ident,
                &where_filter_macro_ident,
                &where_field_filter_ident,
            )?;
            let order_helper_items = generate_order_helper_items(
                self,
                module_name,
                model,
                &order_path_assert_ident,
                &order_entries_macro_ident,
                &order_field_entry_macro_ident,
            )?;

            let query_result_traits = scalar_fields
                .iter()
                .map(|field| {
                    let trait_ident = format_ident!(
                        "__VitrailQueryResultType_{}_{}_{}",
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

            let include_struct_arms = relation_fields
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
                    let target_model_name =
                        LitStr::new(&target.name.to_string(), target.name.span());
                    let target_scalar_fields = target
                        .scalar_fields()
                        .iter()
                        .map(|target_field| {
                            let field_ident = &target_field.name;
                            let field_ty = schema_owned_rust_field_type_tokens(
                                module_name,
                                target,
                                target_field,
                            )?;
                            Ok(quote! { pub #field_ident: #field_ty, })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let target_root_struct_macro_ident =
                        format_ident!("__vitrail_root_struct_{}_{}", module_name, target.name);

                    Ok(quote! {
                        (#ident, $nested_ident:ident, true) => {
                            #[allow(dead_code)]
                            #[derive(::vitrail_pg::QueryResult)]
                            #[vitrail(schema = #dollar_crate::#module_name::Schema, model = #target_model_name)]
                            struct $nested_ident {
                                #(#target_scalar_fields)*
                            }
                        };
                        (#ident, $nested_ident:ident, $nested_query:tt) => {
                            #dollar_crate::#module_name::#target_root_struct_macro_ident! {
                                $nested_ident;
                                $nested_query
                            }
                        };
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            let include_selection_arms = relation_fields
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
                    let target_selection_macro_ident =
                        format_ident!("__vitrail_selection_{}_{}", module_name, target.name);
                    let target_scalar_fields = target.scalar_fields();
                    let target_scalar_field_idents =
                        target_scalar_fields.iter().map(|target_field| {
                            let field_ident = &target_field.name;
                            quote! { #field_ident }
                        });

                    Ok(quote! {
                        (#ident, true) => {
                            #dollar_crate::#module_name::#target_selection_macro_ident! {
                                select { #( #target_scalar_field_idents : true ),* }
                            }
                        };
                        (#ident, $nested_query:tt) => {
                            #dollar_crate::#module_name::#target_selection_macro_ident! {
                                $nested_query
                            }
                        };
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            let root_struct_arms = scalar_fields
                .iter()
                .map(|field| {
                    let ident = &field.name;
                    let ty = schema_owned_rust_field_type_tokens(module_name, model, field)?;
                    Ok(quote! {
                        (
                            @struct
                            $root_ident:ident
                            [ $($attrs:tt)* ]
                            [ $($fields:tt)* ]
                            [ #ident, $($rest_select:ident,)* ]
                            [ $($include_field:ident => $include_value:tt,)* ]
                        ) => {
                            #dollar_crate::#module_name::#root_struct_macro_ident! {
                                @struct
                                $root_ident
                                [ $($attrs)* ]
                                [ $($fields)* pub #ident: #ty, ]
                                [ $($rest_select,)* ]
                                [ $($include_field => $include_value,)* ]
                            }
                        };
                    })
                })
                .chain(relation_fields.iter().map(|field| {
                    let ident = &field.name;
                    let nested_ident = format_ident!(
                        "__VitrailQuery{}{}",
                        to_pascal_case(&model.name.to_string()),
                        to_pascal_case(&field.name.to_string())
                    );
                    let ty = if field.ty.many {
                        quote! { Vec<#nested_ident> }
                    } else if field.ty.optional {
                        quote! { Option<#nested_ident> }
                    } else {
                        quote! { #nested_ident }
                    };
                    Ok(quote! {
                        (
                            @struct
                            $root_ident:ident
                            [ $($attrs:tt)* ]
                            [ $($fields:tt)* ]
                            [ ]
                            [ #ident => $include_value:tt, $($rest_include:ident => $rest_include_value:tt,)* ]
                        ) => {
                            #dollar_crate::#module_name::#include_struct_ident!(#ident, #nested_ident, $include_value);

                            #dollar_crate::#module_name::#root_struct_macro_ident! {
                                @struct
                                $root_ident
                                [ $($attrs)* ]
                                [
                                    $($fields)*
                                    #[vitrail(include)]
                                    pub #ident: #ty,
                                ]
                                [ ]
                                [ $($rest_include => $rest_include_value,)* ]
                            }
                        };
                    })
                }))
                .collect::<Result<Vec<_>>>()?;

            helpers.extend(quote! {
                #[doc(hidden)]
                #[macro_export]
                macro_rules! #select_assert_ident {
                    #(#select_assert_arms)*
                    ($other:ident) => {
                        compile_error!(concat!("unknown scalar field `", ::core::stringify!($other), "` in model `", #model_name, "`"));
                    };
                }

                #[doc(hidden)]
                #[macro_export]
                macro_rules! #include_assert_ident {
                    #(#include_assert_arms)*
                    ($other:ident) => {
                        compile_error!(concat!("unknown relation field `", ::core::stringify!($other), "` in model `", #model_name, "`"));
                    };
                }

                #filter_helper_items
                #order_helper_items

                #[doc(hidden)]
                pub mod #trait_module_ident {
                    #(#query_result_traits)*
                }

                #[doc(hidden)]
                #[macro_export]
                macro_rules! #include_struct_ident {
                    #(#include_struct_arms)*
                    ($other:ident, $nested_ident:ident, $($tokens:tt)*) => {
                        compile_error!(concat!("unknown relation field `", ::core::stringify!($other), "` in model `", #model_name, "`"));
                    };
                }

                #[doc(hidden)]
                #[macro_export]
                macro_rules! #include_selection_ident {
                    #(#include_selection_arms)*
                    ($other:ident, $($tokens:tt)*) => {
                        compile_error!(concat!("unknown relation field `", ::core::stringify!($other), "` in model `", #model_name, "`"));
                    };
                }

                #[doc(hidden)]
                #[macro_export]
                macro_rules! #selection_macro_ident {
                    (
                        {
                            select: {
                                $($select_field:ident : true),* $(,)?
                            }
                            $(,
                                include: {
                                    $($include_field:ident : $include_value:tt),* $(,)?
                                }
                            )?
                            $(,
                                where: {
                                    $($where_field:ident : $where_value:tt),* $(,)?
                                }
                            )?
                            $(,
                                order_by: $order_by:tt
                            )?
                            $(,
                                skip: $skip:expr
                            )?
                            $(,
                                limit: $limit:expr
                            )?
                            $(,)?
                        }
                    ) => {
                        #dollar_crate::#module_name::#selection_macro_ident! {
                            select { $($select_field : true),* }
                            $(, include { $($include_field : $include_value),* })?
                            $(, where { $($where_field : $where_value),* })?
                            $(, order_by $order_by)?
                            $(, skip $skip)?
                            $(, limit $limit)?
                        }
                    };
                    (
                        select { $($select_field:ident : true),* $(,)? }
                        $(, include { $($include_field:ident : $include_value:tt),* $(,)? })?
                        $(, where { $($where_field:ident : $where_value:tt),* $(,)? })?
                        $(, order_by $order_by:tt)?
                        $(, skip $skip:expr)?
                        $(, limit $limit:expr)?
                        $(,)?
                    ) => {{
                        $( #dollar_crate::#module_name::#select_assert_ident!($select_field); )*
                        $( $( #dollar_crate::#module_name::#include_assert_ident!($include_field); )* )?
                        ::vitrail_pg::QuerySelection {
                            model: #model_name,
                            scalar_fields: ::std::vec![$( ::core::stringify!($select_field) ),*],
                            relations: ::std::vec![
                                $(
                                    $(
                                        ::vitrail_pg::QueryRelationSelection {
                                            field: ::core::stringify!($include_field),
                                            selection: #dollar_crate::#module_name::#include_selection_ident!($include_field, $include_value),
                                        }
                                    ),*
                                )?
                            ],
                            filter: None $(.or_else(|| {
                                #dollar_crate::#module_name::#where_filter_macro_ident!({
                                    $($where_field : $where_value),*
                                })
                            }))?,
                            order_by: ::std::vec![] $(
                                .into_iter()
                                .chain(#dollar_crate::#module_name::#order_entries_macro_ident!($order_by))
                                .collect()
                            )?,
                            skip: None $(.or(Some({
                                let __vitrail_skip: i64 = $skip;
                                ::vitrail_pg::QueryPagination::value(__vitrail_skip)
                            })))?,
                            limit: None $(.or(Some({
                                let __vitrail_limit: i64 = $limit;
                                ::vitrail_pg::QueryPagination::value(__vitrail_limit)
                            })))?,
                        }
                    }};
                }

                #[doc(hidden)]
                #[macro_export]
                macro_rules! #root_struct_macro_ident {
                    #(#root_struct_arms)*
                    (
                        $root_ident:ident;
                        {
                            select: {
                                $($select_field:ident : true),* $(,)?
                            }
                            $(,
                                include: {
                                    $($include_field:ident : $include_value:tt),* $(,)?
                                }
                            )?
                            $(,
                                where: {
                                    $($where_field:ident : $where_value:tt),* $(,)?
                                }
                            )?
                            $(,
                                order_by: $order_by:tt
                            )?
                            $(,
                                skip: $skip:expr
                            )?
                            $(,
                                limit: $limit:expr
                            )?
                            $(,)?
                        }
                    ) => {
                        #dollar_crate::#module_name::#root_struct_macro_ident! {
                            $root_ident;
                            select { $($select_field),* }
                            $(, include { $($include_field : $include_value),* } )?
                            $(, where { $($where_field : $where_value),* } )?
                            $(, order_by $order_by )?
                            $(, skip $skip )?
                            $(, limit $limit )?
                        }
                    };
                    (
                        $root_ident:ident;
                        select { $($select_field:ident),* $(,)? }
                        $(, include { $($include_field:ident : $include_value:tt),* $(,)? } )?
                        $(, where { $($where_field:ident : $where_value:tt),* $(,)? } )?
                        $(, order_by $order_by:tt )?
                        $(, skip $skip:expr )?
                        $(, limit $limit:expr )?
                    ) => {
                        $( #dollar_crate::#module_name::#select_assert_ident!($select_field); )*
                        $( $( #dollar_crate::#module_name::#include_assert_ident!($include_field); )* )?

                        #dollar_crate::#module_name::#root_struct_macro_ident! {
                            @struct
                            $root_ident
                            [ ]
                            [ ]
                            [ $($select_field,)* ]
                            [ $($( $include_field => $include_value, )*)? ]
                        }
                    };
                    (
                        @struct
                        $root_ident:ident
                        [ $($attrs:tt)* ]
                        [ $($fields:tt)* ]
                        [ ]
                        [ ]
                    ) => {
                        #[allow(dead_code)]
                        #[derive(::vitrail_pg::QueryResult)]
                        #[vitrail(schema = #dollar_crate::#module_name::Schema, model = #model_name)]
                        $($attrs)*
                        struct $root_ident {
                            $($fields)*
                        }
                    };
                }
            });

            main_arms.push(quote! {
                (
                    #model_ident $query_body:tt
                ) => {{
                    #dollar_crate::#module_name::#root_struct_macro_ident! {
                        #root_struct_ident;
                        $query_body
                    }

                    ::vitrail_pg::Query::<#dollar_crate::#module_name::Schema, #root_struct_ident>::with_selection(
                        #dollar_crate::#module_name::#selection_macro_ident! {
                            $query_body
                        }
                    )
                }};
            });
        }

        helpers.extend(quote! {
            #[doc(hidden)]
            #[macro_export]
            macro_rules! #main_macro_ident {
                #(#main_arms)*
                ($($tokens:tt)*) => {
                    compile_error!("unsupported query shape");
                };
            }
        });

        Ok(helpers)
    }
}
