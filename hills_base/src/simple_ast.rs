use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Archive, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
            refs: HashMap::new(),
        }
    }
}

#[derive(Archive, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub enum TypeInfo {
    Struct(StructInfo),
    Enum(EnumInfo),
}

#[derive(Archive, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct StructInfo {
    pub fields: Vec<StructField>,
}

#[derive(Archive, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct StructField {
    pub ident: String,
    pub ty: String,
}

#[derive(Archive, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct EnumInfo {
    pub variants: Vec<EnumVariant>,
}

#[derive(Archive, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct EnumVariant {
    pub ident: String,
    pub fields: EnumFields,
}

#[derive(Archive, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub enum EnumFields {
    Named(Vec<StructField>),
    Unnamed(Vec<String>),
    Unit,
}
