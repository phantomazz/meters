use proc_macro::TokenStream;
use syn::{Ident, Type};

fn generate_table_name(struct_name: &str) -> String {
    let re = regex::Regex::new(r"([A-Z][a-z]+)").unwrap();
    re.find_iter(struct_name)
        .map(|x| x.as_str().to_lowercase())
        .collect::<Vec<String>>()
        .join("_")
}

#[proc_macro_derive(TableName)]
pub fn table_name_derive_macro(tokens: TokenStream) -> TokenStream {
    let ast = syn::parse::<syn::DeriveInput>(tokens).unwrap();

    let struct_name = match ast.data {
        syn::Data::Struct(_) => ast.ident,
        _ => panic!(),
    };
    let table_name = generate_table_name(&struct_name.to_string());

    quote::quote! {
        impl TableName for #struct_name {
            const TABLE_NAME: &'static str = #table_name;
        }
    }
    .into()
}

#[proc_macro_derive(FieldNames)]
pub fn field_names_derive_macro(tokens: TokenStream) -> TokenStream {
    let ast = syn::parse::<syn::DeriveInput>(tokens).unwrap();

    let (struct_name, fields) = match ast.data {
        syn::Data::Struct(data) => (
            ast.ident,
            data.fields
                .into_iter()
                .filter_map(|x| x.ident)
                .collect::<Vec<Ident>>(),
        ),
        _ => panic!(),
    };
    let string_fields = fields
        .iter()
        .map(|x| x.to_string())
        .collect::<Vec<String>>();

    quote::quote! {
        impl FieldNames for #struct_name {
            fn get_field_names() -> Vec<&'static str> {
                vec![#(#string_fields), *]
            }
        }
    }
    .into()
}

#[proc_macro_derive(InsertValues)]
pub fn insert_values_derive_macro(tokens: TokenStream) -> TokenStream {
    let ast = syn::parse::<syn::DeriveInput>(tokens).unwrap();

    let (struct_name, fields) = match ast.data {
        syn::Data::Struct(data) => (ast.ident, data.fields),
        _ => panic!(),
    };

    let mut names = Vec::<Ident>::new();
    let mut types = Vec::<Type>::new();

    for field in fields.into_iter().skip(1) {
        names.push(field.ident.unwrap());
        types.push(field.ty);
    }

    quote::quote! {
        impl InsertValues for #struct_name {
            type Values = (#(#types,)*);

            fn get_insert_values(&self) -> Self::Values {
                (#(self.#names.clone(),)*)
            }
        }
    }
    .into()
}

#[cfg(test)]
mod test {
    use crate::generate_table_name;

    #[test]
    fn test_generate_name() {
        assert_eq!(generate_table_name("OneTwoThree"), "one_two_three");
    }
}

#[proc_macro_derive(FromRow)]
pub fn from_row_derive_macro(tokens: TokenStream) -> TokenStream {
    let ast = syn::parse::<syn::DeriveInput>(tokens).unwrap();

    let (struct_name, names) = match ast.data {
        syn::Data::Struct(data) => (
            ast.ident,
            data.fields
                .into_iter()
                .filter_map(|x| x.ident)
                .collect::<Vec<Ident>>(),
        ),
        _ => panic!(),
    };

    let indexes = 0..names.len();

    quote::quote! {
        impl FromRow for #struct_name {
            fn from_row(row: &Row) -> Self {
                #struct_name {
                    #(#names: row.get(#indexes).unwrap(),)*
                }
            }
        }
    }
    .into()
}
