use std::collections::HashMap;
use hills_derive::Reflect;
use hills_base::{Reflect, TypeCollection};

#[derive(Reflect)]
struct Simple {
    x: u32,
    y: u32,
    z: NonStandard
}

#[derive(Reflect)]
struct NonStandard {
    z: u32
}

#[test]
fn basic_test() {
    let mut ty_info = TypeCollection::new();
    Simple::reflect(&mut ty_info);
    println!("{ty_info:#?}");
}