pub mod evolution_check;
pub mod generic_key;
pub mod simple_ast;
pub mod simple_version;

pub use evolution_check::is_backwards_compatible;
pub use generic_key::{GenericKey, TreeKey};
pub use simple_ast::*;
pub use simple_version::*;
pub use zerocopy;

pub trait Reflect {
    fn reflect(to: &mut TypeCollection);
}

pub trait TreeRoot {
    fn tree_name() -> &'static str;
}
