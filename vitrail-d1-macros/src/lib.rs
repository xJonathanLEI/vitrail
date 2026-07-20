use proc_macro::TokenStream;

use vitrail_macros_core::{
    NativeAttributeKind, NativeAttributeMapping, OperationFamilies, QueryMacroConfig,
    SchemaMacroConfig, WriteMacroConfig, expand_delete, expand_delete_many, expand_insert,
    expand_insert_input, expand_insert_result, expand_query, expand_query_result,
    expand_query_variables, expand_schema, expand_update, expand_update_data, expand_update_many,
};

fn expand_d1_schema(input: proc_macro2::TokenStream) -> syn::Result<proc_macro2::TokenStream> {
    let config = SchemaMacroConfig::new(
        syn::parse_quote!(::vitrail_d1),
        vitrail_sqlite_dialect::Schema::__macro_dialect(),
        vec![NativeAttributeMapping::new(
            "db",
            "Uuid",
            NativeAttributeKind::DbUuid,
        )],
        OperationFamilies::all(),
    )
    .with_platform_limit_validation(
        vitrail_sqlite_dialect::validate_d1_schema_for_macro,
        syn::parse_quote!(with_d1_platform_limits),
    );

    expand_schema(input, &config)
}

fn d1_query_macro_config() -> QueryMacroConfig {
    QueryMacroConfig::new(
        syn::parse_quote!(::vitrail_d1),
        syn::parse_quote!(::vitrail_d1::D1Row),
        syn::parse_quote!(::vitrail_d1::Error),
    )
}

fn d1_write_macro_config() -> WriteMacroConfig {
    WriteMacroConfig::new(
        syn::parse_quote!(::vitrail_d1),
        syn::parse_quote!(::vitrail_d1::D1Row),
        syn::parse_quote!(::vitrail_d1::Error),
    )
}

/// Validates a Cloudflare D1 schema DSL declaration at compile time.
#[proc_macro]
pub fn schema(input: TokenStream) -> TokenStream {
    match expand_d1_schema(input.into()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Expands an ad hoc D1 read query through its schema-generated helper.
#[proc_macro]
pub fn query(input: TokenStream) -> TokenStream {
    match expand_query(input.into()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Expands an ad hoc D1 insert through its schema-generated helper.
#[proc_macro]
pub fn insert(input: TokenStream) -> TokenStream {
    match expand_insert(input.into()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Expands an ad hoc D1 bulk update through its schema-generated helper.
#[proc_macro]
pub fn update(input: TokenStream) -> TokenStream {
    match expand_update(input.into()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Expands an ad hoc D1 bulk delete through its schema-generated helper.
#[proc_macro]
pub fn delete(input: TokenStream) -> TokenStream {
    match expand_delete(input.into()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Derives a model-first D1 query result.
#[proc_macro_derive(QueryResult, attributes(vitrail))]
pub fn derive_query_result(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    match expand_query_result(input, &d1_query_macro_config()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Derives a D1 query variable set.
#[proc_macro_derive(QueryVariables)]
pub fn derive_query_variables(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    match expand_query_variables(input, &d1_query_macro_config()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Derives a model-first D1 insert input.
#[proc_macro_derive(InsertInput, attributes(vitrail))]
pub fn derive_insert_input(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    match expand_insert_input(input, &d1_write_macro_config()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Derives a model-first D1 insert result.
#[proc_macro_derive(InsertResult, attributes(vitrail))]
pub fn derive_insert_result(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    match expand_insert_result(input, &d1_write_macro_config()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Derives a model-first D1 update data input.
#[proc_macro_derive(UpdateData, attributes(vitrail))]
pub fn derive_update_data(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    match expand_update_data(input, &d1_write_macro_config()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Derives a model-first D1 bulk update.
#[proc_macro_derive(UpdateMany, attributes(vitrail))]
pub fn derive_update_many(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    match expand_update_many(input, &d1_write_macro_config()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Derives a model-first D1 bulk delete.
#[proc_macro_derive(DeleteMany, attributes(vitrail))]
pub fn derive_delete_many(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    match expand_delete_many(input, &d1_write_macro_config()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_no_backend_path_leaks(generated: &str) {
        for leaked_path in [
            "vitrail_pg",
            "vitrail_sqlite",
            "vitrail_d1_core",
            "vitrail_macros_core",
            "sqlx",
        ] {
            assert!(
                !generated.contains(leaked_path),
                "generated D1 expansion leaked `{leaked_path}`"
            );
        }
    }

    #[test]
    fn d1_schema_adapter_emits_every_operation_family_and_platform_limits() {
        let generated = expand_d1_schema(quote::quote! {
            name adapter_schema

            model user {
                id          Int    @id @default(autoincrement())
                postal_code String @rust_ty(PostalCode)
            }
        })
        .expect("D1 schema should expand")
        .to_string();

        for expected in [
            "pub mod adapter_schema",
            "vitrail_d1 :: Schema",
            "vitrail_d1 :: SchemaAccess for Schema",
            "with_d1_platform_limits",
            "static __SCHEMA",
            "pub fn query < T >",
            "pub fn query_with_variables < T >",
            "pub fn insert < T >",
            "vitrail_d1 :: InsertModel",
            "vitrail_d1 :: Insert :: new",
            "vitrail_d1 :: InsertInput",
            "vitrail_d1 :: InsertResult",
            "pub fn update_many < T >",
            "pub fn update_many_with_variables < T >",
            "vitrail_d1 :: UpdateManyModel",
            "vitrail_d1 :: UpdateMany :: new",
            "vitrail_d1 :: UpdateData",
            "vitrail_d1 :: UpdateMany",
            "pub fn delete_many < T >",
            "pub fn delete_many_with_variables < T >",
            "vitrail_d1 :: DeleteManyModel",
            "vitrail_d1 :: DeleteMany :: new",
            "vitrail_d1 :: DeleteMany",
            "macro_rules ! __vitrail_query_adapter_schema",
            "macro_rules ! __vitrail_insert_adapter_schema",
            "macro_rules ! __vitrail_update_adapter_schema",
            "macro_rules ! __vitrail_delete_adapter_schema",
            "__vitrail_query_traits_adapter_schema_user",
            "__vitrail_query_filter_traits_adapter_schema_user",
            "__vitrail_insert_traits_adapter_schema_user",
            "__vitrail_update_traits_adapter_schema_user",
            "__vitrail_update_filter_traits_adapter_schema_user",
            "__vitrail_delete_traits_adapter_schema_user",
            "__vitrail_delete_filter_traits_adapter_schema_user",
            "__vitrail_rust_types_adapter_schema_user",
            "__VitrailRustType_adapter_schema_user_postal_code",
        ] {
            assert!(
                generated.contains(expected),
                "generated D1 operation support is missing `{expected}`"
            );
        }

        assert_no_backend_path_leaks(&generated);
    }

    #[test]
    fn d1_query_adapter_uses_d1_row_and_error_paths() {
        let input = syn::parse2(quote::quote! {
            #[vitrail(schema = crate::adapter_schema::Schema, model = user)]
            struct User {
                id: i64,
                postal_code: String,
            }
        })
        .expect("query result should parse");

        let generated = expand_query_result(input, &d1_query_macro_config())
            .expect("D1 query result should expand")
            .to_string();

        for expected in [
            "vitrail_d1 :: QueryValue",
            "vitrail_d1 :: QueryModel",
            "vitrail_d1 :: D1Row",
            "vitrail_d1 :: Error",
            "vitrail_d1 :: row_value",
        ] {
            assert!(
                generated.contains(expected),
                "generated D1 query expansion is missing `{expected}`"
            );
        }

        assert_no_backend_path_leaks(&generated);
    }

    #[test]
    fn d1_insert_adapter_uses_d1_row_and_error_paths() {
        let input = syn::parse2(quote::quote! {
            #[vitrail(schema = crate::adapter_schema::Schema, model = user)]
            struct NewUser {
                postal_code: String,
            }
        })
        .expect("insert input should parse");
        let result = syn::parse2(quote::quote! {
            #[vitrail(
                schema = crate::adapter_schema::Schema,
                model = user,
                input = NewUser
            )]
            struct User {
                id: i64,
                postal_code: String,
            }
        })
        .expect("insert result should parse");

        let generated_input = expand_insert_input(input, &d1_write_macro_config())
            .expect("D1 insert input should expand")
            .to_string();
        let generated_result = expand_insert_result(result, &d1_write_macro_config())
            .expect("D1 insert result should expand")
            .to_string();
        let generated = format!("{generated_input} {generated_result}");

        for expected in [
            "vitrail_d1 :: InsertScalar",
            "vitrail_d1 :: InsertValueSet",
            "vitrail_d1 :: InsertModel",
            "vitrail_d1 :: D1Row",
            "vitrail_d1 :: Error",
            "vitrail_d1 :: row_value",
        ] {
            assert!(
                generated.contains(expected),
                "generated D1 insert expansion is missing `{expected}`"
            );
        }

        assert_no_backend_path_leaks(&generated);
    }
}
