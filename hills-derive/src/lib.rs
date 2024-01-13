use proc_macro::TokenStream;
use proc_macro_error::__export::proc_macro2::Span;
use proc_macro_error::abort;
use quote::{quote, TokenStreamExt, ToTokens};
use syn::{parse_macro_input, DeriveInput, Data, Attribute, Type};
use syn::spanned::Spanned;

#[proc_macro_derive(Reflect)]
pub fn reflect_fn(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match input.span() { Span { .. } => {} }
    if input.generics.lt_token.is_some() {
        abort!(input.generics.span(), "Generics are not supported");
    }
    // eprintln!("{:?}", input.attrs);
    let doc = collect_docs(&input.attrs);
    let ts = match input.data {
        Data::Struct(ds) => {
            let mut ts = quote!(
                let mut fields = Vec::new();
            );
            let mut non_std_types = Vec::new();
            for (idx, f) in ds.fields.into_iter().enumerate() {
                let name = match f.ident {
                    Some(ident) => { ident.to_string() },
                    None => idx.to_string()
                };
                let ty = ty_to_str(&f.ty);
                if !STD_TYPES.contains(&ty.as_str()) {
                    if let Type::Path(p) = &f.ty {
                        non_std_types.push(p.path.clone());
                    }
                }
                ts.append_all(quote!(
                    fields.push(hills_base::StructField{
                        name: #name.to_string(),
                        ty: #ty.to_string(),
                    });
                ));
            }
            ts.append_all(quote!(
                let self_reflect = hills_base::TypeInfo::Struct(hills_base::StructInfo {
                    doc: #doc.to_string(),
                    fields
                });
            ));
            let struct_ident = input.ident;
            let struct_ident_str = struct_ident.to_string();

            let mut ts_non_std = quote!();
            for ty in non_std_types {
                ts_non_std.append_all(quote!(
                    #ty::reflect(to);
                ));
            }
            quote!(
                impl hills_base::Reflect for #struct_ident {
                    fn reflect(to: &mut hills_base::TypeCollection) {
                        let ty_name = format!("{}::{}", module_path!(), #struct_ident_str);
                        if to.root.is_empty() {
                            to.root = ty_name.clone();
                        }
                        #ts
                        to.refs.insert(ty_name, self_reflect);
                        #ts_non_std
                    }
                }
            )
        }
        Data::Enum(de) => {
            quote!()
        }
        Data::Union(_) => {
            abort!(input.span(), "Unions are not supported");
        }
    }.into();
    // eprintln!("{ts}");
    ts
}

fn collect_docs(attrs: &[Attribute]) -> String {
    let mut doc = String::new();
    for attr in attrs {
        if attr.meta.path().to_token_stream().to_string() == "doc" {
            doc.push_str(format!("{:?}", attr.meta).as_str());
        }
    }
    doc
}

const STD_TYPES: [&str; 12] = ["u8", "u16", "u32", "u64", "u128", "i8", "i16", "i32", "i64", "i128", "f32", "f64"];

fn ty_to_str(ty: &Type) -> String {
    match ty {
        Type::Path(path) => {
            let mut path_str = String::new();
            if path.path.leading_colon.is_some() {
                path_str.push_str("::");
            }
            let mut is_first = true;
            for s in &path.path.segments {
                if is_first {
                    is_first = false;
                } else {
                    path_str.push_str("::");
                }
                path_str.push_str(s.ident.to_string().as_str());
            }
            path_str
        }
        u => {
            abort!(u.span(), "Only Path types are supported now");
        }
    }
}