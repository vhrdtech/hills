use std::collections::HashMap;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Debug, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
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

#[derive(Archive, Debug, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub enum TypeInfo {
    Struct(StructInfo),
    Enum(EnumInfo),
}

#[derive(Archive, Debug, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct StructInfo {
    pub doc: String,
    pub fields: Vec<StructField>
}

#[derive(Archive, Debug, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct StructField {
    pub name: String,
    pub ty: String,
}

#[derive(Archive, Debug, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
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