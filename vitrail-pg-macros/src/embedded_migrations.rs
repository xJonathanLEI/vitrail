use std::fs;
use std::path::{Path, PathBuf};

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{LitStr, Result, Token};

const MIGRATION_SQL_FILE_NAME: &str = "migration.sql";

pub struct EmbeddedMigrationsInput {
    directory: LitStr,
}

impl Parse for EmbeddedMigrationsInput {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let directory = input.parse::<LitStr>()?;

        if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
        }

        Ok(Self { directory })
    }
}

impl EmbeddedMigrationsInput {
    pub fn expand(&self) -> Result<TokenStream> {
        let directory = resolve_directory(&self.directory)?;
        let migrations = read_migrations(&directory, &self.directory)?;

        if migrations.is_empty() {
            return Ok(quote! {
                ::vitrail_pg::EmbeddedMigrations::new(::std::iter::empty::<(&'static str, &'static str)>())
            });
        }

        let migration_entries = migrations.into_iter().map(|migration| {
            let name = LitStr::new(&migration.name, self.directory.span());
            let sql_path =
                LitStr::new(&migration.sql_path.to_string_lossy(), self.directory.span());

            quote! {
                (#name, include_str!(#sql_path))
            }
        });

        Ok(quote! {
            ::vitrail_pg::EmbeddedMigrations::new([
                #(#migration_entries),*
            ])
        })
    }
}

struct EmbeddedMigration {
    name: String,
    sql_path: PathBuf,
}

fn resolve_directory(directory: &LitStr) -> Result<PathBuf> {
    let path = PathBuf::from(directory.value());

    if path.is_absolute() {
        return Ok(path);
    }

    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").map_err(|error| {
        syn::Error::new_spanned(
            directory,
            format!("failed to read CARGO_MANIFEST_DIR while embedding migrations: {error}"),
        )
    })?;

    Ok(Path::new(&manifest_dir).join(path))
}

fn read_migrations(directory: &Path, original_literal: &LitStr) -> Result<Vec<EmbeddedMigration>> {
    let mut entries = fs::read_dir(directory)
        .map_err(|error| {
            syn::Error::new_spanned(
                original_literal,
                format!(
                    "failed to read migrations directory `{}`: {error}",
                    directory.display()
                ),
            )
        })?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| {
            syn::Error::new_spanned(
                original_literal,
                format!(
                    "failed to read an entry from migrations directory `{}`: {error}",
                    directory.display()
                ),
            )
        })?
        .into_iter()
        .filter(|entry| entry.file_type().is_ok_and(|file_type| file_type.is_dir()))
        .collect::<Vec<_>>();

    entries.sort_by_key(|entry| entry.file_name());

    let mut migrations = Vec::with_capacity(entries.len());

    for entry in entries {
        let migration_directory = entry.path();
        let sql_path = migration_directory.join(MIGRATION_SQL_FILE_NAME);

        if !sql_path.is_file() {
            return Err(syn::Error::new_spanned(
                original_literal,
                format!(
                    "migration directory `{}` does not contain `{MIGRATION_SQL_FILE_NAME}`",
                    migration_directory.display()
                ),
            ));
        }

        migrations.push(EmbeddedMigration {
            name: entry.file_name().to_string_lossy().into_owned(),
            sql_path,
        });
    }

    Ok(migrations)
}
