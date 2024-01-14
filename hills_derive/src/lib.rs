use proc_macro::TokenStream;
use proc_macro_error::__export::proc_macro2::Span;
use proc_macro_error::abort;
use quote::{quote, ToTokens, TokenStreamExt};
use syn::spanned::Spanned;
use syn::{parse_macro_input, Attribute, Data, DeriveInput, Fields, Type};

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
    let doc = collect_docs(&input.attrs);
    let mut non_std_types = Vec::new();

    let reflect_ts = match input.data {
        Data::Struct(ds) => {
            let mut ts = quote!(
                let mut fields = Vec::new();
            );
            for (idx, f) in ds.fields.into_iter().enumerate() {
                let ident = match f.ident {
                    Some(ident) => ident.to_string(),
                    None => idx.to_string(),
                };
                let ty = ty_to_str(&f.ty);
                if !STD_TYPES.contains(&ty.as_str()) {
                    if let Type::Path(p) = &f.ty {
                        non_std_types.push(p.path.clone());
                    }
                }
                ts.append_all(quote!(
                    fields.push(hills_base::StructField {
                        ident: #ident.to_string(),
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

            ts
        }
        Data::Enum(de) => {
            let mut ts = quote!(
                let mut variants = Vec::new();
            );

            for variant in de.variants.iter() {
                let fields = match &variant.fields {
                    Fields::Named(fields_named) => {
                        let mut idents = Vec::new();
                        let mut tys = Vec::new();
                        for (idx, f) in fields_named.named.iter().enumerate() {
                            let ty = ty_to_str(&f.ty);
                            let ident = match &f.ident {
                                Some(ident) => ident.to_string(),
                                None => idx.to_string(),
                            };
                            idents.push(ident);
                            if !STD_TYPES.contains(&ty.as_str()) {
                                if let Type::Path(p) = &f.ty {
                                    non_std_types.push(p.path.clone());
                                }
                            }
                            tys.push(ty);
                        }
                        quote! { hills_base::EnumFields::Named([
                            #( hills_base::StructField { ident: #idents.to_string(), ty: #tys.to_string() } ),*
                        ].into()) }
                    }
                    Fields::Unnamed(fields_unnamed) => {
                        let mut list = Vec::new();
                        for f in &fields_unnamed.unnamed {
                            let ty = ty_to_str(&f.ty);
                            if !STD_TYPES.contains(&ty.as_str()) {
                                if let Type::Path(p) = &f.ty {
                                    non_std_types.push(p.path.clone());
                                }
                            }
                            list.push(ty);
                        }

                        quote!(hills_base::EnumFields::Unnamed([
                            #(#list.to_string()),*
                        ].into()))
                    }
                    Fields::Unit => {
                        quote!(hills_base::EnumFields::Unit)
                    }
                };
                let ident = variant.ident.to_string();
                ts.append_all(quote!(
                    variants.push(hills_base::EnumVariant {
                        ident: #ident.to_string(),
                        fields: #fields
                    });
                ));
            }

            ts.append_all(quote!(
                let self_reflect = hills_base::TypeInfo::Enum(hills_base::EnumInfo {
                    doc: #doc.to_string(),
                    variants
                });
            ));

            ts
        }
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

    let struct_ident = input.ident;
    let struct_ident_str = struct_ident.to_string();
    quote!(
        impl hills_base::Reflect for #struct_ident {
            fn reflect(to: &mut hills_base::TypeCollection) {
                let ty_name = format!("{}::{}", module_path!(), #struct_ident_str);
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

fn collect_docs(attrs: &[Attribute]) -> String {
    let mut doc = String::new();
    for attr in attrs {
        if attr.meta.path().to_token_stream().to_string() == "doc" {
            doc.push_str(format!("{:?}", attr.meta).as_str());
        }
    }
    doc
}

const STD_TYPES: [&str; 17] = [
    "u8", "u16", "u32", "u64", "u128", "i8", "i16", "i32", "i64", "i128", "f32", "f64", "bool",
    "String", "Vec", "HashMap", "HashSet",
];

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
