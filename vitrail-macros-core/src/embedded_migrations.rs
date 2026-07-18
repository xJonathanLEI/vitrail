use std::fs;
use std::path::{Path as FsPath, PathBuf};

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{LitStr, Path, Result, Token};

use vitrail_core::migrations::MIGRATION_SQL_FILE_NAME;

/// Parses and expands an embedded migration directory for a dialect facade.
pub fn expand_embedded_migrations(input: TokenStream, runtime_path: &Path) -> Result<TokenStream> {
    syn::parse2::<EmbeddedMigrationsInput>(input)?.expand(runtime_path)
}

struct EmbeddedMigrationsInput {
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
    fn expand(&self, runtime_path: &Path) -> Result<TokenStream> {
        let directory = resolve_directory(&self.directory)?;
        let migrations = read_migrations(&directory, &self.directory)?;

        if migrations.is_empty() {
            return Ok(quote! {
                #runtime_path::EmbeddedMigrations::new(::std::iter::empty::<(&'static str, &'static str)>())
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
            #runtime_path::EmbeddedMigrations::new([
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

    Ok(FsPath::new(&manifest_dir).join(path))
}

fn read_migrations(
    directory: &FsPath,
    original_literal: &LitStr,
) -> Result<Vec<EmbeddedMigration>> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use proc_macro2::Span;
    use quote::quote;

    #[test]
    fn expands_sorted_migrations_with_configured_facade_path() {
        let root = temporary_path("sorted");
        for (name, sql) in [
            ("20240102000000_second", "SELECT 2;"),
            ("20240101000000_first", "SELECT 1;"),
        ] {
            let migration_directory = root.join(name);
            fs::create_dir_all(&migration_directory)
                .expect("migration directory should be creatable");
            fs::write(migration_directory.join(MIGRATION_SQL_FILE_NAME), sql)
                .expect("migration script should be writable");
        }

        let directory = root.to_string_lossy().into_owned();
        let directory = LitStr::new(&directory, Span::call_site());
        let runtime_path: Path = syn::parse_quote!(::custom_runtime);
        let generated = expand_embedded_migrations(quote!(#directory), &runtime_path)
            .expect("embedded migrations should expand")
            .to_string();

        assert!(generated.contains("custom_runtime :: EmbeddedMigrations :: new"));
        assert!(!generated.contains("vitrail_pg"));

        let first = generated
            .find("20240101000000_first")
            .expect("first migration should be embedded");
        let second = generated
            .find("20240102000000_second")
            .expect("second migration should be embedded");
        assert!(first < second, "embedded migrations should be sorted");

        fs::remove_dir_all(root).expect("temporary migration directory should be removable");
    }

    #[test]
    fn expands_an_empty_migration_directory() {
        let root = temporary_path("empty");
        fs::create_dir_all(&root).expect("migration directory should be creatable");

        let directory = root.to_string_lossy().into_owned();
        let directory = LitStr::new(&directory, Span::call_site());
        let runtime_path: Path = syn::parse_quote!(::custom_runtime);
        let generated = expand_embedded_migrations(quote!(#directory), &runtime_path)
            .expect("empty embedded migrations should expand")
            .to_string();

        assert!(generated.contains("custom_runtime :: EmbeddedMigrations :: new"));
        assert!(generated.contains("std :: iter :: empty"));

        fs::remove_dir_all(root).expect("temporary migration directory should be removable");
    }

    fn temporary_path(label: &str) -> PathBuf {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock should be after the Unix epoch")
            .as_nanos();

        std::env::temp_dir().join(format!(
            "vitrail_macros_core_embedded_migrations_{label}_{}_{}",
            std::process::id(),
            suffix
        ))
    }
}
