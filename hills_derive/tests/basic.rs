use hills_base::{EnumFields, Reflect, StructField, TypeCollection, TypeInfo};
use hills_derive::Reflect;

#[derive(Reflect)]
struct MyStruct {
    _x: u32,
    _y: u32,
    _z: NonStandard,
}

#[derive(Reflect)]
struct NonStandard {
    _z: u32,
}

#[derive(Reflect)]
enum MyEnum {
    _A,
    _B(i32, f32),
    _C { x: u8, y: u16 },
}

#[test]
fn struct_test() {
    let mut tc = TypeCollection::new();
    MyStruct::reflect(&mut tc);
    // println!("{tc:#?}");
    assert_eq!(tc.root, "basic::MyStruct");
    let my_struct = tc.refs.get("basic::MyStruct").unwrap();
    assert!(matches!(my_struct, TypeInfo::Struct(_)));
    if let TypeInfo::Struct(s) = my_struct {
        let mut fields = s.fields.iter();
        let field_x = fields.next().unwrap();
        assert_eq!(field_x.ident, "_x");
        assert_eq!(field_x.ty, "u32");
        let field_y = fields.next().unwrap();
        assert_eq!(field_y.ident, "_y");
        assert_eq!(field_y.ty, "u32");
        let field_z = fields.next().unwrap();
        assert_eq!(field_z.ident, "_z");
        assert_eq!(field_z.ty, "NonStandard");
    }

    let non_standard = tc.refs.get("basic::NonStandard").unwrap();
    assert!(matches!(non_standard, TypeInfo::Struct(_)));
    if let TypeInfo::Struct(s) = non_standard {
        let mut fields = s.fields.iter();
        let field_z = fields.next().unwrap();
        assert_eq!(field_z.ident, "_z");
        assert_eq!(field_z.ty, "u32");
    }
}

#[test]
fn enum_test() {
    let mut tc = TypeCollection::new();
    MyEnum::reflect(&mut tc);
    // println!("{tc:#?}");
    let my_enum = tc.refs.get("basic::MyEnum").unwrap();
    assert!(matches!(my_enum, TypeInfo::Enum(_)));
    if let TypeInfo::Enum(e) = my_enum {
        let mut variants = e.variants.iter();
        let variant0 = variants.next().unwrap();
        assert_eq!(variant0.ident, "_A");
        assert_eq!(variant0.fields, EnumFields::Unit);

        let variant1 = variants.next().unwrap();
        assert_eq!(variant1.ident, "_B");
        assert_eq!(
            variant1.fields,
            EnumFields::Unnamed(["i32".into(), "f32".into()].into())
        );

        let variant2 = variants.next().unwrap();
        assert_eq!(variant2.ident, "_C");
        assert_eq!(
            variant2.fields,
            EnumFields::Named(
                [
                    StructField {
                        ident: "x".into(),
                        ty: "u8".to_string()
                    },
                    StructField {
                        ident: "y".into(),
                        ty: "u16".to_string()
                    }
                ]
                .into()
            )
        )
    }
}
