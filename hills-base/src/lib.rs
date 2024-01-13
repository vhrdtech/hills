use std::collections::HashMap;

#[derive(Debug)]
pub struct TypeCollection {
    pub root: String,
    pub refs: HashMap<String, TypeInfo>,
}

impl TypeCollection {
    pub fn new() -> TypeCollection {
        TypeCollection {
            root: String::new(),
            refs: HashMap::new()
        }
    }
}

#[derive(Debug)]
pub enum TypeInfo {
    Struct(StructInfo),
    Enum(EnumInfo),
}

#[derive(Debug)]
pub struct StructInfo {
    pub doc: String,
    pub fields: Vec<StructField>
}

#[derive(Debug)]
pub struct StructField {
    pub name: String,
    pub ty: String,
}

#[derive(Debug)]
pub struct EnumInfo {
    pub doc: String,
    // pub variants: Vec<EnumVariant>,
}

// pub struct EnumVariant {
//     pub name: String,
//     pub
// }

pub trait Reflect {
    fn reflect(to: &mut TypeCollection);
}