use proc_macro2::TokenStream as TokenStream2;
use syn::parse::{Parse, ParseStream};
use syn::{Path, Result, Token};
use vitrail_macros_core::expand_helper_macro;

pub(crate) struct InsertMacroInput {
    schema_path: Path,
    body: TokenStream2,
}

impl Parse for InsertMacroInput {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let schema_path = input.parse()?;
        input.parse::<Token![,]>()?;
        let body: TokenStream2 = input.parse()?;
        Ok(Self { schema_path, body })
    }
}

impl InsertMacroInput {
    pub(crate) fn expand(self) -> TokenStream2 {
        expand_helper_macro(self.schema_path, self.body, "insert")
    }
}

pub(crate) struct UpdateMacroInput {
    schema_path: Path,
    body: TokenStream2,
}

impl Parse for UpdateMacroInput {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let schema_path = input.parse()?;
        input.parse::<Token![,]>()?;
        let body: TokenStream2 = input.parse()?;
        Ok(Self { schema_path, body })
    }
}

impl UpdateMacroInput {
    pub(crate) fn expand(self) -> TokenStream2 {
        expand_helper_macro(self.schema_path, self.body, "update")
    }
}

pub(crate) struct DeleteMacroInput {
    schema_path: Path,
    body: TokenStream2,
}

impl Parse for DeleteMacroInput {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let schema_path = input.parse()?;
        input.parse::<Token![,]>()?;
        let body: TokenStream2 = input.parse()?;
        Ok(Self { schema_path, body })
    }
}

impl DeleteMacroInput {
    pub(crate) fn expand(self) -> TokenStream2 {
        expand_helper_macro(self.schema_path, self.body, "delete")
    }
}
