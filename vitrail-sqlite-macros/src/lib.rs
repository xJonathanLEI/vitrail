use proc_macro::TokenStream;

use vitrail_macros_core::{
    NativeAttributeKind, NativeAttributeMapping, OperationFamilies, QueryMacroConfig,
    SchemaMacroConfig, WriteMacroConfig, expand_delete, expand_delete_many,
    expand_embedded_migrations, expand_insert, expand_insert_input, expand_insert_result,
    expand_query, expand_query_result, expand_query_variables, expand_schema, expand_update,
    expand_update_data, expand_update_many,
};

fn expand_sqlite_schema(input: proc_macro2::TokenStream) -> syn::Result<proc_macro2::TokenStream> {
    let config = SchemaMacroConfig::new(
        syn::parse_quote!(::vitrail_sqlite),
        vitrail_sqlite_core::Schema::__macro_dialect(),
        vec![NativeAttributeMapping::new(
            "db",
            "Uuid",
            NativeAttributeKind::DbUuid,
        )],
        OperationFamilies::all(),
    );

    expand_schema(input, &config)
}

fn sqlite_query_macro_config() -> QueryMacroConfig {
    QueryMacroConfig::new(
        syn::parse_quote!(::vitrail_sqlite),
        syn::parse_quote!(::vitrail_sqlite::sqlx::sqlite::SqliteRow),
    )
}

fn sqlite_write_macro_config() -> WriteMacroConfig {
    WriteMacroConfig::new(
        syn::parse_quote!(::vitrail_sqlite),
        syn::parse_quote!(::vitrail_sqlite::sqlx::sqlite::SqliteRow),
    )
}

fn expand_sqlite_embedded_migrations(
    input: proc_macro2::TokenStream,
) -> syn::Result<proc_macro2::TokenStream> {
    let runtime_path: syn::Path = syn::parse_quote!(::vitrail_sqlite);
    expand_embedded_migrations(input, &runtime_path)
}

/// Validates a SQLite schema DSL declaration at compile time.
#[proc_macro]
pub fn schema(input: TokenStream) -> TokenStream {
    match expand_sqlite_schema(input.into()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Expands an ad hoc SQLite read query through its schema-generated helper.
#[proc_macro]
pub fn query(input: TokenStream) -> TokenStream {
    match expand_query(input.into()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Expands an ad hoc SQLite insert through its schema-generated helper.
#[proc_macro]
pub fn insert(input: TokenStream) -> TokenStream {
    match expand_insert(input.into()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Expands an ad hoc SQLite bulk update through its schema-generated helper.
#[proc_macro]
pub fn update(input: TokenStream) -> TokenStream {
    match expand_update(input.into()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Expands an ad hoc SQLite bulk delete through its schema-generated helper.
#[proc_macro]
pub fn delete(input: TokenStream) -> TokenStream {
    match expand_delete(input.into()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Embeds a SQLite migration directory into the compiled application.
#[proc_macro]
pub fn embed_migrations(input: TokenStream) -> TokenStream {
    match expand_sqlite_embedded_migrations(input.into()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Derives a model-first SQLite query result.
#[proc_macro_derive(QueryResult, attributes(vitrail))]
pub fn derive_query_result(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    match expand_query_result(input, &sqlite_query_macro_config()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Derives a SQLite query variable set.
#[proc_macro_derive(QueryVariables)]
pub fn derive_query_variables(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    match expand_query_variables(input, &sqlite_query_macro_config()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Derives a model-first SQLite insert input.
#[proc_macro_derive(InsertInput, attributes(vitrail))]
pub fn derive_insert_input(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    match expand_insert_input(input, &sqlite_write_macro_config()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Derives a model-first SQLite insert result.
#[proc_macro_derive(InsertResult, attributes(vitrail))]
pub fn derive_insert_result(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    match expand_insert_result(input, &sqlite_write_macro_config()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Derives a model-first SQLite update data input.
#[proc_macro_derive(UpdateData, attributes(vitrail))]
pub fn derive_update_data(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    match expand_update_data(input, &sqlite_write_macro_config()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Derives a model-first SQLite bulk update.
#[proc_macro_derive(UpdateMany, attributes(vitrail))]
pub fn derive_update_many(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    match expand_update_many(input, &sqlite_write_macro_config()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Derives a model-first SQLite bulk delete.
#[proc_macro_derive(DeleteMany, attributes(vitrail))]
pub fn derive_delete_many(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    match expand_delete_many(input, &sqlite_write_macro_config()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_schema_adapter_emits_complete_operation_support() {
        let generated = expand_sqlite_schema(quote::quote! {
            name adapter_schema

            model user {
                id          Int    @id @default(autoincrement())
                postal_code String @rust_ty(PostalCode)
            }
        })
        .expect("SQLite schema should expand")
        .to_string();

        for expected in [
            "pub mod adapter_schema",
            "vitrail_sqlite :: Schema",
            "vitrail_sqlite :: SchemaAccess for Schema",
            "static __SCHEMA",
            "pub fn query < T >",
            "pub fn query_with_variables < T >",
            "pub fn insert < T >",
            "vitrail_sqlite :: InsertModel",
            "vitrail_sqlite :: Insert :: new",
            "vitrail_sqlite :: InsertInput",
            "vitrail_sqlite :: InsertResult",
            "pub fn update_many < T >",
            "pub fn update_many_with_variables < T >",
            "vitrail_sqlite :: UpdateManyModel",
            "vitrail_sqlite :: UpdateMany :: new",
            "vitrail_sqlite :: UpdateData",
            "vitrail_sqlite :: UpdateMany",
            "pub fn delete_many < T >",
            "pub fn delete_many_with_variables < T >",
            "vitrail_sqlite :: DeleteManyModel",
            "vitrail_sqlite :: DeleteMany :: new",
            "vitrail_sqlite :: DeleteMany",
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
                "generated SQLite operation support is missing `{expected}`"
            );
        }

        for leaked_path in ["vitrail_pg", "vitrail_core", "vitrail_macros_core"] {
            assert!(
                !generated.contains(leaked_path),
                "generated SQLite operation support leaked `{leaked_path}`"
            );
        }
    }
}
