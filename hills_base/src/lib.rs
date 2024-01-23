pub mod evolution_check;
pub mod generic_key;
pub mod simple_ast;
pub mod simple_version;

pub use evolution_check::is_backwards_compatible;
pub use generic_key::{GenericKey, TreeKey};
pub use simple_ast::*;
pub use simple_version::*;

pub trait Reflect {
    fn reflect(to: &mut TypeCollection);
}

pub trait TreeRoot {
    fn tree_name() -> &'static str;
    fn evolution() -> SimpleVersion;
}

use rkyv::with::AsBox;
use rkyv::{Archive, Deserialize, Serialize};

/// This wrapper type serializes the contained value out-of-line so that newer
/// versions can be viewed as the older version.
#[derive(Archive, Deserialize, Serialize)]
#[repr(transparent)]
#[archive(check_bytes)]
pub struct Evolving<T>(#[with(AsBox)] pub T);
