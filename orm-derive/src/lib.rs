use proc_macro::TokenStream;
// use quote::{quote, ToTokens};
// use syn::{parse_macro_input, Attribute, Data, DeriveInput, FieldsNamed, Ident, LitStr, Type};
use quote::quote;
use syn::{parse_macro_input, Attribute, DeriveInput, MetaList};

fn extract_attribute(attrs: &[Attribute], name: &str, default: String) -> String {
    attrs
        .iter()
        .find(|attr| attr.path.is_ident(name))
        .map(|attr| match attr.parse_meta() {
            Ok(syn::Meta::List(MetaList { nested, .. })) => {
                if nested.len() != 1 {
                    panic!("expected exactly one value for #[{}]", name)
                } else if let syn::NestedMeta::Lit(syn::Lit::Str(ref attr_value)) =
                    nested.first().unwrap()
                {
                    attr_value.value()
                } else {
                    panic!("expected string literal for #[{}]", name)
                }
            }
            _ => panic!("expected list for #[{}]", name),
        })
        .unwrap_or(default)
}

#[proc_macro_derive(Object, attributes(table_name, column_name))]
pub fn derive_object(input: TokenStream) -> TokenStream {
    let input: DeriveInput = parse_macro_input!(input);
    let input_ident = input.ident;
    let table_name = extract_attribute(&input.attrs, "table_name", input_ident.to_string());
    let struct_ = match input.data {
        syn::Data::Struct(struct_) => struct_,
        _ => panic!("only structs are supported"),
    };
    let named_fields = match struct_.fields {
        syn::Fields::Named(fields) => fields.named,
        syn::Fields::Unit => syn::punctuated::Punctuated::default(),
        _ => panic!("unnamed fields are unsupported"),
    };
    let fields_count = named_fields.len();
    let field_idents: Vec<_> = named_fields
        .iter()
        .map(|field| field.ident.as_ref().unwrap())
        .collect();
    let to_row = field_idents
        .iter()
        .map(|ident| quote! { (&self.#ident).into() });
    let column_names = named_fields.iter().map(|field| {
        extract_attribute(
            &field.attrs,
            "column_name",
            field.ident.as_ref().unwrap().to_string(),
        )
    });
    let types = named_fields.iter().map(|field| &field.ty);
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let type_name = input_ident.to_string();
    let output = quote! {
        impl #impl_generics orm::object::Object for #input_ident #ty_generics
        #where_clause
        {
            fn from_row(row: orm::storage::Row) -> Self {
                let row: [orm::data::Value; #fields_count] = row.try_into().ok().unwrap();
                match row {
                    [#(#field_idents,)*] => Self {
                        #(#field_idents: #field_idents.into(),)*
                    },
                }
            }

            fn to_row(&self) -> orm::storage::Row {
                vec![#(#to_row,)*]
            }

            const SCHEMA: orm::object::Schema = orm::object::Schema {
                table_name: #table_name,
                fields: &[#(orm::object::Field {
                    column_name: #column_names,
                    data_type: <#types as orm::data::ToDataType>::DATA_TYPE,
                    attr_name: stringify!(#field_idents),
                },)*],
                type_name: #type_name,
            };
        }
    };
    output.into()
}
