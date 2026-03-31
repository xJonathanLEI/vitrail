use proc_macro2::{Ident, Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use std::collections::HashSet;
use syn::spanned::Spanned;
use syn::{Attribute, Data, DataStruct, Error, Fields, LitStr, Path, Result, Type};

pub(crate) struct InsertInputDerive {
    ident: Ident,
    generics: syn::Generics,
    fields: Vec<InsertField>,
    schema_path: Path,
    model_name: LitStr,
}

impl InsertInputDerive {
    pub(crate) fn parse(input: syn::DeriveInput) -> Result<Self> {
        let ident = input.ident;
        let generics = input.generics;
        let (schema_path, model_name) = parse_insert_input_container_attrs(&input.attrs)?;

        let Data::Struct(DataStruct {
            fields: Fields::Named(fields),
            ..
        }) = input.data
        else {
            return Err(Error::new(
                ident.span(),
                "`InsertInput` can only be derived for structs with named fields",
            ));
        };

        let fields = fields
            .named
            .into_iter()
            .map(|field| InsertField::parse(field, "insert input"))
            .collect::<Result<Vec<_>>>()?;

        validate_unique_insert_fields(&fields, &ident, "insert input")?;

        Ok(Self {
            ident,
            generics,
            fields,
            schema_path,
            model_name,
        })
    }

    pub(crate) fn expand(self) -> Result<TokenStream2> {
        let ident = self.ident;
        let mut generics = self.generics;
        let fields = self.fields;
        let schema_path = self.schema_path;
        let model_name = self.model_name;
        let schema_module_ident = schema_module_ident(&schema_path, "InsertInput")?;
        let model_ident = syn::parse_str::<Ident>(&model_name.value()).map_err(|_| {
            Error::new(
                model_name.span(),
                "`#[vitrail(model = ...)]` must be a valid identifier for `InsertInput`",
            )
        })?;
        let field_type_assert_ident = format_ident!(
            "__vitrail_assert_insert_input_type_{}_{}",
            schema_module_ident,
            model_ident
        );
        let input_complete_assert_ident = format_ident!(
            "__vitrail_assert_insert_input_complete_{}_{}",
            schema_module_ident,
            model_ident
        );
        let schema_module_path = schema_module_path(&schema_path, "InsertInput")?;
        let input_type_assert_macro = quote! {
            #schema_module_path::#field_type_assert_ident
        };
        let input_complete_assert_macro = quote! {
            #schema_module_path::#input_complete_assert_ident
        };
        let model_trait_module_ident = format_ident!(
            "__vitrail_insert_traits_{}_{}",
            schema_module_ident,
            model_ident
        );

        for field in &fields {
            let field_ty = &field.ty;
            generics
                .make_where_clause()
                .predicates
                .push(syn::parse_quote!(#field_ty: ::vitrail_pg::InsertScalar));
        }

        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        let field_idents = fields
            .iter()
            .map(|field| field.schema_field_ident())
            .collect::<Result<Vec<_>>>()?;

        let validation_tokens = fields
            .iter()
            .map(|field| {
                let field_ident = field.schema_field_ident()?;
                let field_ty = &field.ty;

                Ok(quote! {
                    #input_type_assert_macro!(#field_ty, #field_ident);
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let insert_values = fields.iter().map(|field| {
            let ident = &field.ident;
            let field_name = &field.field_name;
            quote! {
                __vitrail_values
                    .push(#field_name, ::vitrail_pg::InsertScalar::into_insert_value(self.#ident))
                    .expect("insert input field names should be unique after derive validation");
            }
        });

        Ok(quote! {
            impl #impl_generics #ident #ty_generics
            #where_clause
            {
                #[doc(hidden)]
                fn __vitrail_validate_insert_input() {
                    let _ = stringify!(#model_name);
                    #(#validation_tokens)*
                    #input_complete_assert_macro!(#(#field_idents),*);
                }
            }

            impl #impl_generics #schema_module_path::#model_trait_module_ident::__VitrailInsertInputModel
                for #ident #ty_generics
            #where_clause
            {
            }

            impl #impl_generics ::vitrail_pg::InsertValueSet for #ident #ty_generics
            #where_clause
            {
                fn into_insert_values(self) -> ::vitrail_pg::InsertValues {
                    Self::__vitrail_validate_insert_input();

                    let mut __vitrail_values = ::vitrail_pg::InsertValues::new();
                    #(#insert_values)*
                    __vitrail_values
                }
            }
        })
    }
}

pub(crate) struct InsertResultDerive {
    ident: Ident,
    generics: syn::Generics,
    fields: Vec<InsertField>,
    schema_path: Path,
    model_name: LitStr,
    input_ty: Type,
}

impl InsertResultDerive {
    pub(crate) fn parse(input: syn::DeriveInput) -> Result<Self> {
        let ident = input.ident;
        let generics = input.generics;
        let (schema_path, model_name, input_ty) =
            parse_insert_result_container_attrs(&input.attrs)?;

        let Data::Struct(DataStruct {
            fields: Fields::Named(fields),
            ..
        }) = input.data
        else {
            return Err(Error::new(
                ident.span(),
                "`InsertResult` can only be derived for structs with named fields",
            ));
        };

        let fields = fields
            .named
            .into_iter()
            .map(|field| InsertField::parse(field, "insert result"))
            .collect::<Result<Vec<_>>>()?;

        validate_unique_insert_fields(&fields, &ident, "insert result")?;

        Ok(Self {
            ident,
            generics,
            fields,
            schema_path,
            model_name,
            input_ty,
        })
    }

    pub(crate) fn expand(self) -> Result<TokenStream2> {
        let ident = self.ident;
        let generics = self.generics;
        let fields = self.fields;
        let schema_path = self.schema_path;
        let model_name = self.model_name;
        let input_ty = self.input_ty;
        let schema_module_ident = schema_module_ident(&schema_path, "InsertResult")?;
        let model_ident = syn::parse_str::<Ident>(&model_name.value()).map_err(|_| {
            Error::new(
                model_name.span(),
                "`#[vitrail(model = ...)]` must be a valid identifier for `InsertResult`",
            )
        })?;
        let field_type_assert_ident = format_ident!(
            "__vitrail_assert_insert_result_type_{}_{}",
            schema_module_ident,
            model_ident
        );
        let schema_module_path = schema_module_path(&schema_path, "InsertResult")?;
        let result_type_assert_macro = quote! {
            #schema_module_path::#field_type_assert_ident
        };
        let model_trait_module_ident = format_ident!(
            "__vitrail_insert_traits_{}_{}",
            schema_module_ident,
            model_ident
        );
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        let validation_tokens = fields
            .iter()
            .map(|field| {
                let field_ident = field.schema_field_ident()?;
                let field_ty = &field.ty;

                Ok(quote! {
                    #result_type_assert_macro!(#field_ty, #field_ident);
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let returning_fields = fields.iter().map(|field| {
            let field_name = &field.field_name;
            quote! { #field_name }
        });

        let decode_fields = fields.iter().map(|field| {
            let ident = &field.ident;
            let field_name = &field.field_name;
            let field_ty = &field.ty;

            quote! {
                #ident: {
                    let __vitrail_alias = ::vitrail_pg::alias_name(prefix, #field_name);
                    ::vitrail_pg::row_value::<#field_ty>(row, __vitrail_alias.as_str())?
                }
            }
        });

        Ok(quote! {
            impl #impl_generics #ident #ty_generics
            #where_clause
            {
                #[doc(hidden)]
                fn __vitrail_validate_insert_result() {
                    #(#validation_tokens)*
                    fn __vitrail_assert_insert_values<
                        T: ::vitrail_pg::InsertValueSet
                            + #schema_module_path::#model_trait_module_ident::__VitrailInsertInputModel,
                    >() {
                    }
                    __vitrail_assert_insert_values::<#input_ty>();
                }
            }

            impl #impl_generics ::vitrail_pg::InsertModel for #ident #ty_generics
            #where_clause
            {
                type Schema = #schema_path;
                type Values = #input_ty;

                fn model_name() -> &'static str {
                    #model_name
                }

                fn returning_fields() -> &'static [&'static str] {
                    Self::__vitrail_validate_insert_result();
                    &[#(#returning_fields),*]
                }

                fn from_row(
                    row: &::vitrail_pg::sqlx::postgres::PgRow,
                    prefix: &str,
                ) -> Result<Self, ::vitrail_pg::sqlx::Error> {
                    use ::vitrail_pg::sqlx::Row as _;

                    Self::__vitrail_validate_insert_result();

                    Ok(Self {
                        #(#decode_fields),*
                    })
                }
            }
        })
    }
}

struct InsertField {
    ident: Ident,
    ty: Type,
    field_name: LitStr,
}

impl InsertField {
    fn parse(field: syn::Field, derive_target: &str) -> Result<Self> {
        let span = field.span();
        let ident = field
            .ident
            .ok_or_else(|| Error::new(span, "expected a named field"))?;
        let mut rename = None;

        for attribute in &field.attrs {
            if !attribute.path().is_ident("vitrail") {
                continue;
            }

            attribute.parse_nested_meta(|meta| {
                if meta.path.is_ident("field") {
                    rename = Some(meta.value()?.parse::<LitStr>()?);
                    return Ok(());
                }

                Err(meta.error(format!(
                    "unsupported `#[vitrail(...)]` field attribute for {derive_target}"
                )))
            })?;
        }

        let field_name = rename.unwrap_or_else(|| LitStr::new(&ident.to_string(), ident.span()));

        Ok(Self {
            ident,
            ty: field.ty,
            field_name,
        })
    }

    fn schema_field_ident(&self) -> Result<Ident> {
        syn::parse_str::<Ident>(&self.field_name.value()).map_err(|_| {
            Error::new(
                self.field_name.span(),
                "insert field names must be valid identifiers",
            )
        })
    }
}

fn validate_unique_insert_fields(
    fields: &[InsertField],
    ident: &Ident,
    derive_target: &str,
) -> Result<()> {
    let mut seen = HashSet::new();

    for field in fields {
        let field_name = field.field_name.value();
        if !seen.insert(field_name.clone()) {
            return Err(Error::new(
                ident.span(),
                format!("duplicate field `{field_name}` in {derive_target} derive"),
            ));
        }
    }

    Ok(())
}

fn parse_insert_input_container_attrs(attrs: &[Attribute]) -> Result<(Path, LitStr)> {
    let mut schema_path = None;
    let mut model_name = None;

    for attribute in attrs {
        if !attribute.path().is_ident("vitrail") {
            continue;
        }

        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("schema") {
                schema_path = Some(meta.value()?.parse()?);
                return Ok(());
            }
            if meta.path.is_ident("model") {
                let value = meta.value()?;
                if value.peek(LitStr) {
                    model_name = Some(value.parse::<LitStr>()?);
                } else {
                    let ident = value.parse::<Ident>()?;
                    model_name = Some(LitStr::new(&ident.to_string(), ident.span()));
                }
                return Ok(());
            }
            Err(meta.error("unsupported `#[vitrail(...)]` container attribute"))
        })?;
    }

    let schema_path = schema_path.ok_or_else(|| {
        Error::new(
            Span::call_site(),
            "`#[derive(InsertInput)]` requires `#[vitrail(schema = ...)]`",
        )
    })?;
    let model_name = model_name.ok_or_else(|| {
        Error::new(
            Span::call_site(),
            "`#[derive(InsertInput)]` requires `#[vitrail(model = ...)]`",
        )
    })?;

    Ok((schema_path, model_name))
}

fn parse_insert_result_container_attrs(attrs: &[Attribute]) -> Result<(Path, LitStr, Type)> {
    let mut schema_path = None;
    let mut model_name = None;
    let mut input_ty = None;

    for attribute in attrs {
        if !attribute.path().is_ident("vitrail") {
            continue;
        }

        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("schema") {
                schema_path = Some(meta.value()?.parse()?);
                return Ok(());
            }
            if meta.path.is_ident("model") {
                let value = meta.value()?;
                if value.peek(LitStr) {
                    model_name = Some(value.parse::<LitStr>()?);
                } else {
                    let ident = value.parse::<Ident>()?;
                    model_name = Some(LitStr::new(&ident.to_string(), ident.span()));
                }
                return Ok(());
            }
            if meta.path.is_ident("input") {
                input_ty = Some(meta.value()?.parse()?);
                return Ok(());
            }
            Err(meta.error("unsupported `#[vitrail(...)]` container attribute"))
        })?;
    }

    let schema_path = schema_path.ok_or_else(|| {
        Error::new(
            Span::call_site(),
            "`#[derive(InsertResult)]` requires `#[vitrail(schema = ...)]`",
        )
    })?;
    let model_name = model_name.ok_or_else(|| {
        Error::new(
            Span::call_site(),
            "`#[derive(InsertResult)]` requires `#[vitrail(model = ...)]`",
        )
    })?;
    let input_ty = input_ty.ok_or_else(|| {
        Error::new(
            Span::call_site(),
            "`#[derive(InsertResult)]` requires `#[vitrail(input = ...)]`",
        )
    })?;

    Ok((schema_path, model_name, input_ty))
}

fn schema_module_path(schema_path: &Path, derive_name: &str) -> Result<Path> {
    if schema_path.segments.len() < 2 {
        return Err(Error::new(
            schema_path.span(),
            format!(
                "`#[vitrail(schema = ...)]` for `{derive_name}` must point to a schema type like `crate::my_schema::Schema`"
            ),
        ));
    }

    Ok(Path {
        leading_colon: schema_path.leading_colon,
        segments: schema_path
            .segments
            .iter()
            .take(schema_path.segments.len() - 1)
            .cloned()
            .collect(),
    })
}

fn schema_module_ident(schema_path: &Path, derive_name: &str) -> Result<Ident> {
    schema_path
        .segments
        .iter()
        .rev()
        .nth(1)
        .map(|segment| segment.ident.clone())
        .ok_or_else(|| {
            Error::new(
                schema_path.span(),
                format!(
                    "`#[vitrail(schema = ...)]` for `{derive_name}` must point to a schema type like `crate::my_schema::Schema`"
                ),
            )
        })
}
