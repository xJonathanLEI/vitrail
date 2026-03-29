use proc_macro::TokenStream;

mod delete;
mod insert;
mod macro_inputs;
mod query;
mod schema;
mod update;

use delete::DeleteManyDerive;
use insert::{InsertInputDerive, InsertResultDerive};
use macro_inputs::{DeleteMacroInput, InsertMacroInput, QueryMacroInput, UpdateMacroInput};
use query::{QueryResultDerive, QueryVariablesDerive};
use schema::ParsedSchema;
use update::{UpdateDataDerive, UpdateManyDerive};

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

#[proc_macro]
pub fn delete(input: TokenStream) -> TokenStream {
    let delete = syn::parse_macro_input!(input as DeleteMacroInput);
    delete.expand().into()
}

#[proc_macro]
pub fn update(input: TokenStream) -> TokenStream {
    let update = syn::parse_macro_input!(input as UpdateMacroInput);
    update.expand().into()
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

#[proc_macro_derive(UpdateData, attributes(vitrail))]
pub fn derive_update_data(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    match UpdateDataDerive::parse(input).and_then(|derive| derive.expand()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

#[proc_macro_derive(UpdateMany, attributes(vitrail))]
pub fn derive_update_many(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    match UpdateManyDerive::parse(input).and_then(|derive| derive.expand()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

#[proc_macro_derive(DeleteMany, attributes(vitrail))]
pub fn derive_delete_many(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    match DeleteManyDerive::parse(input).and_then(|derive| derive.expand()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}
