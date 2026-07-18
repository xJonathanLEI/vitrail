use proc_macro::TokenStream;

use vitrail_macros_core::{
    NativeAttributeKind, NativeAttributeMapping, OperationFamilies, SchemaMacroConfig,
    expand_schema,
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
        OperationFamilies::none(),
    );

    expand_schema(input, &config)
}

/// Validates a SQLite schema DSL declaration at compile time.
#[proc_macro]
pub fn schema(input: TokenStream) -> TokenStream {
    match expand_sqlite_schema(input.into()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_schema_adapter_emits_only_schema_support_items() {
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
        ] {
            assert!(
                generated.contains(expected),
                "generated SQLite schema support is missing `{expected}`"
            );
        }

        for unsupported_item in [
            "pub fn query <",
            "pub fn query_with_variables <",
            "pub fn insert <",
            "pub fn delete_many <",
            "pub fn delete_many_with_variables <",
            "pub fn update_many <",
            "pub fn update_many_with_variables <",
            "__vitrail_query_adapter_schema",
            "__vitrail_insert_adapter_schema",
            "__vitrail_delete_adapter_schema",
            "__vitrail_update_adapter_schema",
            "__vitrail_query_traits",
            "__vitrail_insert_traits",
            "__vitrail_delete_traits",
            "__vitrail_update_traits",
            "__vitrail_rust_types",
        ] {
            assert!(
                !generated.contains(unsupported_item),
                "schema-only SQLite expansion unexpectedly emitted `{unsupported_item}`"
            );
        }

        for leaked_path in ["vitrail_pg", "vitrail_core", "vitrail_macros_core"] {
            assert!(
                !generated.contains(leaked_path),
                "generated SQLite schema support leaked `{leaked_path}`"
            );
        }
    }
}
