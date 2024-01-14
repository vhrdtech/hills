use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Archive, Debug, Eq, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct TypeCollection {
    pub root: String,
    pub refs: HashMap<String, TypeInfo>,
}

impl PartialEq for TypeCollection {
    fn eq(&self, other: &Self) -> bool {
        self.refs == other.refs
    }
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

#[derive(Archive, Debug, Eq, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct StructInfo {
    pub fields: Vec<StructField>,
}

impl PartialEq for StructInfo {
    fn eq(&self, other: &Self) -> bool {
        if other.fields.len() < self.fields.len() {
            return false;
        }
        for (f, f_new) in self.fields.iter().zip(other.fields.iter()) {
            if f != f_new {
                return false;
            }
        }
        true
    }
}

#[derive(Archive, Debug, Eq, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct StructField {
    pub ident: String,
    pub ty: String,
}

impl PartialEq for StructField {
    fn eq(&self, other: &Self) -> bool {
        self.ty == other.ty
    }
}

#[derive(Archive, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct EnumInfo {
    pub variants: Vec<EnumVariant>,
}

#[derive(Archive, Debug, Eq, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct EnumVariant {
    pub ident: String,
    pub fields: EnumFields,
}

impl PartialEq for EnumVariant {
    fn eq(&self, other: &Self) -> bool {
        self.fields == other.fields
    }
}

#[derive(Archive, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub enum EnumFields {
    Named(Vec<StructField>),
    Unnamed(Vec<String>),
    Unit,
}

pub trait Reflect {
    fn reflect(to: &mut TypeCollection);
}

#[derive(Archive, PartialEq, Eq, Debug, Hash, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(PartialEq, Eq, Debug, Hash))]
pub struct SimpleVersion {
    /// Backwards compatibility breaking
    pub major: u16,
    /// Backwards and Future compatible changes
    pub minor: u16,
}

impl SimpleVersion {
    pub const fn new(major: u16, minor: u16) -> SimpleVersion {
        SimpleVersion { major, minor }
    }
}
