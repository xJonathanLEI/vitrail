use proc_macro2::TokenStream as TokenStream2;
use syn::parse::{Parse, ParseStream};
use syn::{Path, Result, Token};

use crate::delete::DeleteManyDerive;
use crate::helper_macro::expand_helper_macro;
use crate::insert::{InsertInputDerive, InsertResultDerive};
use crate::update::{UpdateDataDerive, UpdateManyDerive};

/// Dialect-specific paths used by shared write procedural macro expansion.
pub struct WriteMacroConfig {
    runtime_path: Path,
    row_path: Path,
}

impl WriteMacroConfig {
    pub fn new(runtime_path: Path, row_path: Path) -> Self {
        Self {
            runtime_path,
            row_path,
        }
    }

    pub fn runtime_path(&self) -> &Path {
        &self.runtime_path
    }

    pub fn row_path(&self) -> &Path {
        &self.row_path
    }
}

/// Expands the user-facing `insert!` macro into its schema-generated helper.
pub fn expand_insert(input: TokenStream2) -> Result<TokenStream2> {
    expand_write_helper(input, "insert")
}

/// Expands an `InsertInput` derive using a dialect-specific runtime path.
pub fn expand_insert_input(
    input: syn::DeriveInput,
    config: &WriteMacroConfig,
) -> Result<TokenStream2> {
    InsertInputDerive::parse(input)?.expand(config)
}

/// Expands an `InsertResult` derive using dialect-specific runtime and row paths.
pub fn expand_insert_result(
    input: syn::DeriveInput,
    config: &WriteMacroConfig,
) -> Result<TokenStream2> {
    InsertResultDerive::parse(input)?.expand(config)
}

/// Expands the user-facing `update!` macro into its schema-generated helper.
pub fn expand_update(input: TokenStream2) -> Result<TokenStream2> {
    expand_write_helper(input, "update")
}

/// Expands an `UpdateData` derive using a dialect-specific runtime path.
pub fn expand_update_data(
    input: syn::DeriveInput,
    config: &WriteMacroConfig,
) -> Result<TokenStream2> {
    UpdateDataDerive::parse(input)?.expand(config)
}

/// Expands an `UpdateMany` derive using a dialect-specific runtime path.
pub fn expand_update_many(
    input: syn::DeriveInput,
    config: &WriteMacroConfig,
) -> Result<TokenStream2> {
    UpdateManyDerive::parse(input)?.expand(config)
}

/// Expands the user-facing `delete!` macro into its schema-generated helper.
pub fn expand_delete(input: TokenStream2) -> Result<TokenStream2> {
    expand_write_helper(input, "delete")
}

/// Expands a `DeleteMany` derive using a dialect-specific runtime path.
pub fn expand_delete_many(
    input: syn::DeriveInput,
    config: &WriteMacroConfig,
) -> Result<TokenStream2> {
    DeleteManyDerive::parse(input)?.expand(config)
}

fn expand_write_helper(input: TokenStream2, macro_prefix: &str) -> Result<TokenStream2> {
    let input = syn::parse2::<WriteMacroInput>(input)?;
    Ok(expand_helper_macro(
        input.schema_path,
        input.body,
        macro_prefix,
    ))
}

struct WriteMacroInput {
    schema_path: Path,
    body: TokenStream2,
}

impl Parse for WriteMacroInput {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let schema_path = input.parse()?;
        input.parse::<Token![,]>()?;
        let body = input.parse()?;

        Ok(Self { schema_path, body })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    fn custom_config() -> WriteMacroConfig {
        WriteMacroConfig::new(
            syn::parse_quote!(::custom_facade),
            syn::parse_quote!(::custom_backend::CustomRow),
        )
    }

    fn assert_no_dialect_path_leaks(generated: &str) {
        for hardcoded_facade in ["vitrail_pg", "vitrail_sqlite"] {
            assert!(
                !generated.contains(hardcoded_facade),
                "generated write expansion leaked `{hardcoded_facade}`"
            );
        }
    }

    #[test]
    fn write_helpers_route_to_schema_generated_macros() {
        let cases = [
            (
                expand_insert(quote! {
                    crate::custom_schema,
                    user {
                        data: {
                            email: "user@example.com",
                        },
                    }
                })
                .expect("insert helper should expand"),
                "__vitrail_insert_custom_schema",
            ),
            (
                expand_update(quote! {
                    crate::custom_schema,
                    user {
                        data: {
                            email: "updated@example.com",
                        },
                    }
                })
                .expect("update helper should expand"),
                "__vitrail_update_custom_schema",
            ),
            (
                expand_delete(quote! {
                    crate::custom_schema,
                    user
                })
                .expect("delete helper should expand"),
                "__vitrail_delete_custom_schema",
            ),
        ];

        for (generated, expected) in cases {
            let generated = generated.to_string();
            assert!(
                generated.contains(expected),
                "generated write helper is missing `{expected}`"
            );
        }
    }

    #[test]
    fn insert_expansion_uses_configured_facade_and_row_paths() {
        let input = syn::parse2(quote! {
            #[vitrail(schema = crate::custom_schema::Schema, model = user)]
            struct NewUser {
                email: String,
            }
        })
        .expect("insert input should parse");
        let result = syn::parse2(quote! {
            #[vitrail(
                schema = crate::custom_schema::Schema,
                model = user,
                input = NewUser
            )]
            struct User {
                id: i64,
                email: String,
            }
        })
        .expect("insert result should parse");

        let generated_input = expand_insert_input(input, &custom_config())
            .expect("insert input should expand")
            .to_string();
        let generated_result = expand_insert_result(result, &custom_config())
            .expect("insert result should expand")
            .to_string();
        let generated = format!("{generated_input} {generated_result}");

        for expected in [
            "custom_facade :: InsertScalar",
            "custom_facade :: InsertValueSet",
            "custom_facade :: InsertValues",
            "custom_facade :: InsertModel",
            "custom_facade :: alias_name",
            "custom_facade :: row_value",
            "custom_facade :: sqlx :: Error",
            "custom_facade :: sqlx :: Row",
            "custom_backend :: CustomRow",
        ] {
            assert!(
                generated.contains(expected),
                "generated insert expansion is missing `{expected}`"
            );
        }

        assert!(
            !generated.contains("PgRow"),
            "generated insert expansion leaked `PgRow`"
        );
        assert_no_dialect_path_leaks(&generated);
    }

    #[test]
    fn update_expansion_uses_configured_facade_path() {
        let data = syn::parse2(quote! {
            #[vitrail(schema = crate::custom_schema::Schema, model = user)]
            struct UpdateUser {
                email: String,
            }
        })
        .expect("update data should parse");
        let update = syn::parse2(quote! {
            #[vitrail(
                schema = crate::custom_schema::Schema,
                model = user,
                data = UpdateUser,
                variables = Variables,
                where(profile.email = eq(email))
            )]
            struct UpdateUsers;
        })
        .expect("update many input should parse");

        let generated_data = expand_update_data(data, &custom_config())
            .expect("update data should expand")
            .to_string();
        let generated_update = expand_update_many(update, &custom_config())
            .expect("update many should expand")
            .to_string();
        let generated = format!("{generated_data} {generated_update}");

        for expected in [
            "custom_facade :: UpdateScalar",
            "custom_facade :: UpdateValueSet",
            "custom_facade :: UpdateValues",
            "custom_facade :: UpdateManyModel",
            "custom_facade :: QueryVariables",
            "custom_facade :: QueryFilter",
        ] {
            assert!(
                generated.contains(expected),
                "generated update expansion is missing `{expected}`"
            );
        }

        assert!(!generated.contains("custom_backend"));
        assert_no_dialect_path_leaks(&generated);
    }

    #[test]
    fn delete_expansion_uses_configured_facade_path() {
        let input = syn::parse2(quote! {
            #[vitrail(
                schema = crate::custom_schema::Schema,
                model = user,
                variables = Variables,
                where(profile.email = eq(email))
            )]
            struct DeleteUsers;
        })
        .expect("delete many input should parse");

        let generated = expand_delete_many(input, &custom_config())
            .expect("delete many should expand")
            .to_string();

        for expected in [
            "custom_facade :: DeleteManyModel",
            "custom_facade :: QueryVariables",
            "custom_facade :: QueryFilter",
        ] {
            assert!(
                generated.contains(expected),
                "generated delete expansion is missing `{expected}`"
            );
        }

        assert!(!generated.contains("custom_backend"));
        assert_no_dialect_path_leaks(&generated);
    }
}
