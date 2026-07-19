use proc_macro2::{TokenStream as TokenStream2, TokenTree};
use quote::{format_ident, quote};
use syn::Path;

/// Expands a dialect facade macro into its schema-generated operation helper.
pub(crate) fn expand_helper_macro(
    schema_path: Path,
    body: TokenStream2,
    macro_prefix: &str,
) -> TokenStream2 {
    let body = normalize_macro_body(body);
    let module_segment = schema_path
        .segments
        .last()
        .expect("schema path should contain at least one segment");
    let macro_ident = format_ident!("__vitrail_{}_{}", macro_prefix, module_segment.ident);
    let schema_module_ident = &module_segment.ident;

    quote! {{
        use #schema_path as #schema_module_ident;

        #schema_module_ident::#macro_ident! {
            #body
        }
    }}
}

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
