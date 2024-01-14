use crate::{EnumFields, TypeCollection, TypeInfo};

/// Checks whether new type set is backwards compatible with the previous according to rules:
/// * Struct field and enum variant renaming is allowed.
/// * Adding new struct fields  is allowed.
/// * Changing types in structs or in enum fields is forbidden.
/// * Adding new enum fields is forbidden.
pub fn is_backwards_compatible(previous: &TypeCollection, next: &TypeCollection) -> bool {
    let Some(prev_root) = previous.refs.get(previous.root.as_str()) else {
        return false;
    };
    let Some(next_root) = next.refs.get(previous.root.as_str()) else {
        return false;
    };
    match prev_root {
        TypeInfo::Struct(prev_si) => match next_root {
            TypeInfo::Struct(next_si) => {
                if prev_si.fields.len() > next_si.fields.len() {
                    return false;
                }
                for (f, f_new) in prev_si.fields.iter().zip(next_si.fields.iter()) {
                    if f.ty != f_new.ty {
                        return false;
                    }
                }
                true
            }
            TypeInfo::Enum(_) => {
                return false;
            }
        },
        TypeInfo::Enum(prev_ei) => match next_root {
            TypeInfo::Enum(next_ei) => {
                if prev_ei.variants.len() != next_ei.variants.len() {
                    return false;
                }
                for (f, f_new) in prev_ei.variants.iter().zip(next_ei.variants.iter()) {
                    if !is_enum_fields_compatible(&f.fields, &f_new.fields) {
                        return false;
                    }
                }
                true
            }
            TypeInfo::Struct(_) => {
                return false;
            }
        },
    }
}

fn is_enum_fields_compatible(prev_field: &EnumFields, next_fields: &EnumFields) -> bool {
    match prev_field {
        EnumFields::Named(prev_named) => {
            let EnumFields::Named(next_named) = next_fields else {
                return false;
            };
            if prev_named.len() != next_named.len() {
                return false;
            }
            for (f, f_new) in prev_named.iter().zip(next_named.iter()) {
                if f.ty != f_new.ty {
                    return false;
                }
            }
            true
        }
        EnumFields::Unnamed(prev_unnamed) => {
            let EnumFields::Unnamed(next_unnamed) = next_fields else {
                return false;
            };
            prev_unnamed == next_unnamed
        }
        EnumFields::Unit => next_fields == &EnumFields::Unit,
    }
}
