use proc_macro2::{TokenStream as TokenStream2, TokenTree};
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::{Path, Result, Token};

fn normalize_macro_body(tokens: TokenStream2) -> TokenStream2 {
    let mut normalized = TokenStream2::new();
    let mut iter = tokens.into_iter().peekable();

    while let Some(token) = iter.next() {
        match token {
            TokenTree::Group(group) => {
                let mut normalized_group = proc_macro2::Group::new(
                    group.delimiter(),
                    normalize_macro_body(group.stream()),
                );
                normalized_group.set_span(group.span());
                normalized.extend([TokenTree::Group(normalized_group)]);
            }
            TokenTree::Punct(punct) if punct.as_char() == '$' => {
                if let Some(TokenTree::Ident(ident)) = iter.peek() {
                    let ident = ident.clone();
                    iter.next();
                    normalized.extend([TokenTree::Ident(ident)]);
                } else {
                    normalized.extend([TokenTree::Punct(punct)]);
                }
            }
            other => normalized.extend([other]),
        }
    }

    normalized
}

fn expand_helper_macro(schema_path: Path, body: TokenStream2, macro_prefix: &str) -> TokenStream2 {
    let body = normalize_macro_body(body);
    let segments = schema_path.segments.iter().collect::<Vec<_>>();
    let module_segment = segments
        .last()
        .expect("schema path should contain at least one segment");
    let macro_ident = format_ident!("__vitrail_{}_{}", macro_prefix, module_segment.ident);

    if segments.len() == 1
        || segments
            .first()
            .is_some_and(|segment| segment.ident == "crate")
        || segments
            .first()
            .is_some_and(|segment| segment.ident == "self")
    {
        quote! {
            #macro_ident! {
                #body
            }
        }
    } else {
        quote! {
            #schema_path::#macro_ident! {
                #body
            }
        }
    }
}

pub(crate) struct QueryMacroInput {
    schema_path: Path,
    body: TokenStream2,
}

impl Parse for QueryMacroInput {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let schema_path = input.parse()?;
        input.parse::<Token![,]>()?;
        let body: TokenStream2 = input.parse()?;
        Ok(Self { schema_path, body })
    }
}

impl QueryMacroInput {
    pub(crate) fn expand(self) -> TokenStream2 {
        expand_helper_macro(self.schema_path, self.body, "query")
    }
}

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
