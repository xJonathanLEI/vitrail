use proc_macro2::{Ident, TokenStream as TokenStream2};
use quote::{ToTokens, quote};
use syn::ext::IdentExt;
use syn::parse::{Parse, ParseStream};
use syn::{Error, Result, Token, parenthesized};

pub(crate) struct RootFilter {
    path: Vec<Ident>,
    filter: ScalarFilter,
}

enum ScalarFilter {
    Eq { variable: Ident },
    In { variable: Ident },
    Ne { variable: Ident },
    IsNull,
    IsNotNull,
}

impl RootFilter {
    pub(crate) fn expand(&self) -> TokenStream2 {
        let final_field = self.path.last().expect("filter path should never be empty");

        let mut filter = match &self.filter {
            ScalarFilter::Eq { variable } => quote! {
                ::vitrail_pg::QueryFilter::eq(
                    stringify!(#final_field),
                    ::vitrail_pg::QueryFilterValue::variable(stringify!(#variable)),
                )
            },
            ScalarFilter::In { variable } => quote! {
                ::vitrail_pg::QueryFilter::r#in(
                    stringify!(#final_field),
                    ::vitrail_pg::QueryFilterValues::variable(stringify!(#variable)),
                )
            },
            ScalarFilter::Ne { variable } => quote! {
                ::vitrail_pg::QueryFilter::ne(
                    stringify!(#final_field),
                    ::vitrail_pg::QueryFilterValue::variable(stringify!(#variable)),
                )
            },
            ScalarFilter::IsNull => quote! {
                ::vitrail_pg::QueryFilter::is_null(stringify!(#final_field))
            },
            ScalarFilter::IsNotNull => quote! {
                ::vitrail_pg::QueryFilter::is_not_null(stringify!(#final_field))
            },
        };

        for segment in self.path[..self.path.len() - 1].iter().rev() {
            filter = quote! {
                ::vitrail_pg::QueryFilter::relation(stringify!(#segment), #filter)
            };
        }

        filter
    }

    pub(crate) fn validation_tokens(
        &self,
        where_path_assert_macro: &impl ToTokens,
    ) -> TokenStream2 {
        let segments = &self.path;
        quote! {
            #where_path_assert_macro!(#(#segments).*);
        }
    }

    pub(crate) fn type_validation_tokens(
        &self,
        where_filter_value_assert_macro: &impl ToTokens,
    ) -> Option<TokenStream2> {
        let segments = &self.path;

        match &self.filter {
            ScalarFilter::Eq { variable } => Some(quote! {
                #where_filter_value_assert_macro!(#(#segments).*, eq, &__vitrail_variables.#variable);
            }),
            ScalarFilter::In { variable } => Some(quote! {
                #where_filter_value_assert_macro!(#(#segments).*, in, &__vitrail_variables.#variable);
            }),
            ScalarFilter::Ne { variable } => Some(quote! {
                #where_filter_value_assert_macro!(#(#segments).*, not, &__vitrail_variables.#variable);
            }),
            ScalarFilter::IsNull | ScalarFilter::IsNotNull => None,
        }
    }

    pub(crate) fn variable(&self) -> Option<&Ident> {
        match &self.filter {
            ScalarFilter::Eq { variable }
            | ScalarFilter::In { variable }
            | ScalarFilter::Ne { variable } => Some(variable),
            ScalarFilter::IsNull | ScalarFilter::IsNotNull => None,
        }
    }
}

impl Parse for RootFilter {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut path = vec![input.call(Ident::parse_any)?];

        while input.peek(Token![.]) {
            input.parse::<Token![.]>()?;
            path.push(input.call(Ident::parse_any)?);
        }

        input.parse::<Token![=]>()?;
        let operator = input.call(Ident::parse_any)?;

        let filter = if operator == "eq" {
            let operator_args;
            parenthesized!(operator_args in input);
            let variable = operator_args.call(Ident::parse_any)?;

            if !operator_args.is_empty() {
                return Err(Error::new(
                    operator_args.span(),
                    "unexpected tokens in `where(... = eq(...))`",
                ));
            }

            ScalarFilter::Eq { variable }
        } else if operator == "in" {
            let operator_args;
            parenthesized!(operator_args in input);
            let variable = operator_args.call(Ident::parse_any)?;

            if !operator_args.is_empty() {
                return Err(Error::new(
                    operator_args.span(),
                    "unexpected tokens in `where(... = in(...))`",
                ));
            }

            ScalarFilter::In { variable }
        } else if operator == "null" {
            ScalarFilter::IsNull
        } else if operator == "not" {
            let operator_args;
            parenthesized!(operator_args in input);
            let value = operator_args.call(Ident::parse_any)?;

            if !operator_args.is_empty() {
                return Err(Error::new(
                    operator_args.span(),
                    "unexpected tokens in `where(... = not(...))`",
                ));
            }

            if value == "null" {
                ScalarFilter::IsNotNull
            } else {
                ScalarFilter::Ne { variable: value }
            }
        } else {
            return Err(Error::new(
                operator.span(),
                "unsupported `where` operator; only `eq`, `in`, `null`, and `not(...)` are currently supported",
            ));
        };

        if !input.is_empty() {
            return Err(Error::new(
                input.span(),
                "unexpected tokens in `where(...)`",
            ));
        }

        Ok(Self { path, filter })
    }
}

pub(crate) fn parse_root_filter(input: ParseStream<'_>) -> Result<RootFilter> {
    let content;
    parenthesized!(content in input);
    content.parse()
}
