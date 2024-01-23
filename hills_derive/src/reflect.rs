use proc_macro2::TokenStream;
use proc_macro_error::abort;
use quote::{quote, TokenStreamExt};
use syn::spanned::Spanned;
use syn::{DataEnum, DataStruct, Fields, Path, Type};

pub fn process_struct(non_std_types: &mut Vec<Path>, ds: DataStruct) -> TokenStream {
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
            fields
        });
    ));

    ts
}

pub fn process_enum(non_std_types: &mut Vec<Path>, de: DataEnum) -> TokenStream {
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
            variants
        });
    ));

    ts
}

const STD_TYPES: [&str; 18] = [
    "u8", "u16", "u32", "u64", "u128", "i8", "i16", "i32", "i64", "i128", "f32", "f64", "bool",
    "Option", "String", "Vec", "HashMap", "HashSet",
];

// TODO: Handle generics
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
