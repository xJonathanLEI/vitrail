use proc_macro2::{Ident, TokenStream as TokenStream2};
use quote::{ToTokens, quote};
use syn::ext::IdentExt;
use syn::parse::{Parse, ParseStream};
use syn::{Error, Result, Token, parenthesized};

pub(crate) struct RootOrder {
    path: Vec<Ident>,
    direction: OrderDirection,
}

#[derive(Clone, Copy)]
enum OrderDirection {
    Asc,
    Desc,
}

impl RootOrder {
    pub(crate) fn expand(&self) -> TokenStream2 {
        let final_field = self.path.last().expect("order path should never be empty");
        let direction = match self.direction {
            OrderDirection::Asc => quote! { ::vitrail_pg::QueryOrderDirection::Asc },
            OrderDirection::Desc => quote! { ::vitrail_pg::QueryOrderDirection::Desc },
        };

        let mut order = quote! {
            ::vitrail_pg::QueryOrder::scalar(stringify!(#final_field), #direction)
        };

        for segment in self.path[..self.path.len() - 1].iter().rev() {
            order = quote! {
                ::vitrail_pg::QueryOrder::relation(stringify!(#segment), vec![#order])
            };
        }

        order
    }

    pub(crate) fn validation_tokens(
        &self,
        order_path_assert_macro: &impl ToTokens,
    ) -> TokenStream2 {
        let segments = &self.path;
        quote! {
            #order_path_assert_macro!(#(#segments).*);
        }
    }
}

impl Parse for RootOrder {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut path = vec![input.call(Ident::parse_any)?];

        while input.peek(Token![.]) {
            input.parse::<Token![.]>()?;
            path.push(input.call(Ident::parse_any)?);
        }

        input.parse::<Token![=]>()?;
        let direction = input.call(Ident::parse_any)?;
        let direction = match direction.to_string().as_str() {
            "asc" => OrderDirection::Asc,
            "desc" => OrderDirection::Desc,
            _ => {
                return Err(Error::new(
                    direction.span(),
                    "unsupported `order_by` direction; only `asc` and `desc` are currently supported",
                ));
            }
        };

        Ok(Self { path, direction })
    }
}

pub(crate) fn parse_root_orders(input: ParseStream<'_>) -> Result<Vec<RootOrder>> {
    let content;
    parenthesized!(content in input);
    let mut orders = Vec::new();

    while !content.is_empty() {
        orders.push(content.parse()?);

        if content.is_empty() {
            break;
        }

        content.parse::<Token![,]>()?;
    }

    Ok(orders)
}
