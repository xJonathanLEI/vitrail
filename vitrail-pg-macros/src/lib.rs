use proc_macro::TokenStream;

mod insert;
mod macro_inputs;
mod query;
mod schema;

use insert::{InsertInputDerive, InsertResultDerive};
use macro_inputs::{InsertMacroInput, QueryMacroInput};
use query::{QueryResultDerive, QueryVariablesDerive};
use schema::ParsedSchema;

/// Validates a schema DSL declaration at compile time.
#[proc_macro]
pub fn schema(input: TokenStream) -> TokenStream {
    let schema = syn::parse_macro_input!(input as ParsedSchema);

    match schema.expand() {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

#[proc_macro]
pub fn query(input: TokenStream) -> TokenStream {
    let query = syn::parse_macro_input!(input as QueryMacroInput);
    query.expand().into()
}

#[proc_macro]
pub fn insert(input: TokenStream) -> TokenStream {
    let insert = syn::parse_macro_input!(input as InsertMacroInput);
    insert.expand().into()
}

#[proc_macro_derive(QueryResult, attributes(vitrail))]
pub fn derive_query_result(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    match QueryResultDerive::parse(input).and_then(|derive| derive.expand()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

#[proc_macro_derive(QueryVariables)]
pub fn derive_query_variables(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    match QueryVariablesDerive::parse(input).and_then(|derive| derive.expand()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

#[proc_macro_derive(InsertInput, attributes(vitrail))]
pub fn derive_insert_input(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    match InsertInputDerive::parse(input).and_then(|derive| derive.expand()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

#[proc_macro_derive(InsertResult, attributes(vitrail))]
pub fn derive_insert_result(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    match InsertResultDerive::parse(input).and_then(|derive| derive.expand()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}
