pub mod evolution_check;
pub mod simple_ast;

pub use evolution_check::is_backwards_compatible;
pub use simple_ast::*;

use rkyv::{Archive, Deserialize, Serialize};

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
