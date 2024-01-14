use hills_derive::Reflect;
use hills_base::{Reflect, TypeCollection, TypeInfo};

#[derive(Reflect)]
struct Simple {
    _x: u32,
    _y: u32,
    _z: NonStandard
}

#[derive(Reflect)]
struct NonStandard {
    _z: u32
}

#[test]
fn basic_test() {
    let mut ty_info = TypeCollection::new();
    Simple::reflect(&mut ty_info);
    // println!("{ty_info:#?}");
    let simple = ty_info.refs.get("basic::Simple").unwrap();
    assert!(matches!(simple, TypeInfo::Struct(_)));
    if let TypeInfo::Struct(s) = simple {
        let mut fields = s.fields.iter();
        let field_x = fields.next().unwrap();
        assert_eq!(field_x.name, "_x");
        assert_eq!(field_x.ty, "u32");
        let field_y = fields.next().unwrap();
        assert_eq!(field_y.name, "_y");
        assert_eq!(field_y.ty, "u32");
        let field_z = fields.next().unwrap();
        assert_eq!(field_z.name, "_z");
        assert_eq!(field_z.ty, "NonStandard");
    }

    let non_standard = ty_info.refs.get("basic::NonStandard").unwrap();
    assert!(matches!(non_standard, TypeInfo::Struct(_)));
    if let TypeInfo::Struct(s) = non_standard {
        let mut fields = s.fields.iter();
        let field_z = fields.next().unwrap();
        assert_eq!(field_z.name, "_z");
        assert_eq!(field_z.ty, "u32");
    }
}