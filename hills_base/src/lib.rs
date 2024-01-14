pub mod evolution_check;
pub mod simple_ast;
pub mod simple_version;

pub use evolution_check::is_backwards_compatible;
pub use simple_ast::*;
pub use simple_version::*;

pub trait Reflect {
    fn reflect(to: &mut TypeCollection);
}

pub trait TreeKey {
    fn tree_name() -> &'static str;
}

pub trait TreeRoot {
    fn tree_name() -> &'static str;
}
