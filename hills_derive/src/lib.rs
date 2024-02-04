mod evolve;
mod reflect;

use proc_macro::TokenStream;
use proc_macro_error::__export::proc_macro2::Span;
use proc_macro_error::abort;
use quote::{quote, TokenStreamExt};
use syn::spanned::Spanned;
use syn::{parse_macro_input, Data, DeriveInput};

#[proc_macro_derive(Reflect)]
pub fn reflect_fn(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match input.span() {
        Span { .. } => {}
    }
    if input.generics.lt_token.is_some() {
        abort!(input.generics.span(), "Generics are not supported");
    }
    // eprintln!("{:?}", input.attrs);
    let mut non_std_types = Vec::new();

    let reflect_ts = match input.data {
        Data::Struct(ds) => reflect::process_struct(&mut non_std_types, ds),
        Data::Enum(de) => reflect::process_enum(&mut non_std_types, de),
        Data::Union(_) => {
            abort!(input.span(), "Unions are not supported");
        }
    };
    // eprintln!("{ts}");

    let mut reflect_non_std_ts = quote!();
    for ty in non_std_types {
        reflect_non_std_ts.append_all(quote!(
            #ty::reflect(to);
        ));
    }

    let ident = input.ident;
    let ident_str = ident.to_string();
    // let ty_name = format!("{}::{}", module_path!(), #ident_str);
    quote!(
        impl hills_base::Reflect for #ident {
            fn reflect(to: &mut hills_base::TypeCollection) {
                let ty_name = #ident_str.to_string();
                if to.root.is_empty() {
                    to.root = ty_name.clone();
                }
                #reflect_ts
                to.refs.insert(ty_name, self_reflect);
                #reflect_non_std_ts
            }
        }
    )
    .into()
}

#[proc_macro_attribute]
pub fn evolve(attr: TokenStream, item: TokenStream) -> TokenStream {
    println!("attr: \"{}\"", attr.to_string());
    println!("item: \"{}\"", item.to_string());
    item
}

#[proc_macro_attribute]
pub fn rkyv_common_derives(_args: TokenStream, input: TokenStream) -> TokenStream {
    let mut output = TokenStream::from(quote! {
        #[derive(
            rkyv::Archive,
            rkyv::Serialize,
            rkyv::Deserialize,
            serde::Serialize,
            serde::Deserialize,
            hills_derive::Reflect,
            Clone,
            Debug,
            PartialEq,
        )]
        #[archive(check_bytes)]
        #[archive_attr(derive(Debug))]
    });
    output.extend(input);
    output
}

// fn collect_docs(attrs: &[Attribute]) -> String {
//     let mut doc = String::new();
//     for attr in attrs {
//         if attr.meta.path().to_token_stream().to_string() == "doc" {
//             doc.push_str(format!("{:?}", attr.meta).as_str());
//         }
//     }
//     doc
// }
