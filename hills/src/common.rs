use crate::consts::MANAGED_TREES;
use rkyv::ser::serializers::{
    AllocScratchError, CompositeSerializerError, SharedSerializeMapError,
};
use rkyv::validation::validators::DefaultValidatorError;
use rkyv::validation::CheckArchiveError;
use rkyv::{check_archived_root, to_bytes, Archive, Deserialize, Serialize};
use sled::Db;
use std::collections::HashSet;
use std::convert::Infallible;
use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Sled(#[from] sled::Error),

    #[error("{}", .0)]
    Internal(String),

    #[error("Ws")]
    Ws,

    #[error("rkyv serialize: {}", .0)]
    RkyvSerializeError(String),

    #[error("rkyv check_archived_root failed: {}", .0)]
    RkyvDeserializeError(String),

    #[error("broadcast channel error")]
    PostageBroadcast,
}

impl From<CompositeSerializerError<Infallible, AllocScratchError, SharedSerializeMapError>>
    for Error
{
    fn from(
        value: CompositeSerializerError<Infallible, AllocScratchError, SharedSerializeMapError>,
    ) -> Self {
        Error::RkyvSerializeError(format!("{value:?}"))
    }
}

impl<T: Debug> From<CheckArchiveError<T, DefaultValidatorError>> for Error {
    fn from(value: CheckArchiveError<T, DefaultValidatorError>) -> Self {
        Error::RkyvDeserializeError(format!("{value:?}"))
    }
}

impl From<Infallible> for Error {
    fn from(_value: Infallible) -> Self {
        Error::RkyvDeserializeError("Infallible".into())
    }
}

#[derive(Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct ManagedTrees {
    pub trees: HashSet<String>,
    pub _dummy22: [u8; 18],
}

impl ManagedTrees {
    pub fn add_to_managed(db: &Db, tree_name: impl AsRef<str>) -> Result<(), Error> {
        let tree_name = tree_name.as_ref();
        match db.get(MANAGED_TREES)? {
            Some(trees) => {
                let trees = check_archived_root::<ManagedTrees>(&trees)?;
                if !trees.trees.contains(tree_name) {
                    let mut trees: ManagedTrees = trees.deserialize(&mut rkyv::Infallible)?;
                    trees.trees.insert(tree_name.to_string());
                    let trees_bytes = to_bytes::<_, 128>(&trees)?;
                    db.insert(MANAGED_TREES, trees_bytes.as_slice())?;
                }
            }
            None => {
                let trees = ManagedTrees {
                    trees: [tree_name.to_string()].into(),
                    _dummy22: [0u8; 18],
                };
                let trees_bytes = to_bytes::<_, 128>(&trees)?;
                db.insert(MANAGED_TREES, trees_bytes.as_slice())?;
            }
        }
        Ok(())
    }

    pub fn managed(db: &Db) -> Result<Vec<String>, Error> {
        match db.get(MANAGED_TREES)? {
            Some(trees) => {
                let trees = check_archived_root::<ManagedTrees>(&trees)?;
                let trees: Vec<String> = trees.trees.iter().map(|name| name.to_string()).collect();
                Ok(trees)
            }
            None => Ok(vec![]),
        }
    }
}
