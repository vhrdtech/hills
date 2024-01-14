use hills_base::{Reflect, TypeCollection};

mod evolving {
    use hills_derive::Reflect;

    pub mod ev0_0 {
        use super::*;

        #[derive(Reflect)]
        pub struct MyStruct {
            _x: u32,
        }

        #[derive(Reflect)]
        pub enum MyEnum {
            _A,
            _B(u32),
            _C { x: u32 },
            __Future1,
        }
    }

    pub mod ev0_1a {
        use super::*;

        #[derive(Reflect)]
        pub struct MyStruct {
            _z: u32,
            _y: u32,
        }

        #[derive(Reflect)]
        pub enum MyEnum {
            _A,
            _B(u32),
            _C { x: u32 },
            _D,
        }
    }

    pub mod ev0_1b {
        use super::*;

        #[derive(Reflect)]
        pub struct MyStruct {
            _x: i32,
        }

        #[derive(Reflect)]
        pub enum MyEnum {
            _A,
            _B(i32),
            _C { x: u32 },
            __Future1,
        }
    }
}

#[test]
fn struct_evolution() {
    let mut tc_ev0_0 = TypeCollection::new();
    evolving::ev0_0::MyStruct::reflect(&mut tc_ev0_0);

    let mut tc_ev0_1a = TypeCollection::new();
    evolving::ev0_1a::MyStruct::reflect(&mut tc_ev0_1a);
    // Can add more fields and rename old ones, without changing their types.
    assert_eq!(tc_ev0_0, tc_ev0_1a);

    let mut tc_ev0_1b = TypeCollection::new();
    evolving::ev0_1b::MyStruct::reflect(&mut tc_ev0_1b);
    // Cannot change field types.
    assert_ne!(tc_ev0_0, tc_ev0_1b);
}

#[test]
fn enum_evolution() {
    let mut tc_ev0_0 = TypeCollection::new();
    evolving::ev0_0::MyEnum::reflect(&mut tc_ev0_0);

    let mut tc_ev0_1a = TypeCollection::new();
    evolving::ev0_1a::MyEnum::reflect(&mut tc_ev0_1a);
    // Can rename existing variants.
    assert_eq!(tc_ev0_0, tc_ev0_1a);

    let mut tc_ev0_1b = TypeCollection::new();
    evolving::ev0_1b::MyEnum::reflect(&mut tc_ev0_1b);
    // Cannot change variant types
    assert_ne!(tc_ev0_0, tc_ev0_1b);
}
