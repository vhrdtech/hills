use crate::record::RecordId;
use crate::sync::NodeKind;
use crate::tree::TreeDescriptor;
use hills_base::{Reflect, SimpleVersion, TypeCollection};
use log::trace;
use rkyv::check_archived_root;
use rkyv::ser::serializers::{
    AllocScratchError, CompositeSerializerError, SharedSerializeMapError,
};
use sled::{Db, Tree};
use std::collections::HashMap;
use std::convert::Infallible;
use std::path::Path;
use thiserror::Error;

pub struct VhrdDb {
    db: Db,
    node_kind: NodeKind,
    descriptors: Tree,
    open_trees: HashMap<String, TreeBundle>,
}

#[derive(Clone)]
struct TreeBundle {
    /// Key -> Record tree
    data: Tree,
    /// Monotonic serial -> JournalEntry
    journal: Tree,
    /// Monotonic index -> Key for all the latest revisions
    latest_revision_index: Tree,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Sled(#[from] sled::Error),

    #[error("Tree {} not found", .0)]
    TreeNotFound(String),

    #[error("check_archived_root failed")]
    RkyvCheckArchivedRoot,

    #[error("rkyv serialize: {}", .0)]
    RkyvSerializeError(String),
}

impl
    From<
        CompositeSerializerError<
            std::convert::Infallible,
            AllocScratchError,
            SharedSerializeMapError,
        >,
    > for Error
{
    fn from(
        value: CompositeSerializerError<Infallible, AllocScratchError, SharedSerializeMapError>,
    ) -> Self {
        Error::RkyvSerializeError(format!("{value:?}"))
    }
}

impl VhrdDb {
    pub fn open<P: AsRef<Path>>(path: P, node_kind: NodeKind) -> Result<Self, Error> {
        let db = sled::open(path)?;
        let descriptors = db.open_tree("descriptors")?;
        Ok(VhrdDb {
            db,
            node_kind,
            descriptors,
            open_trees: HashMap::default(),
        })
    }

    pub fn create_record(&mut self, tree_name: impl AsRef<str>) -> Result<RecordId, Error> {
        let descriptor = self.descriptors.get(tree_name.as_ref().as_bytes())?;
        let Some(descriptor) = descriptor else {
            return Err(Error::TreeNotFound(tree_name.as_ref().to_string()));
        };
        let descriptor = check_archived_root::<TreeDescriptor>(&descriptor)
            .map_err(|_| Error::RkyvCheckArchivedRoot)?;
        trace!("{descriptor:?}");

        // match self.node_kind {
        //     NodeKind::Server | NodeKind::StandAlone => {}
        //     NodeKind::Client => {}
        //     NodeKind::Backup => {}
        // }
        todo!()
    }

    pub fn open_tree<R: Reflect>(
        &mut self,
        tree_name: impl AsRef<str>,
        evolution: SimpleVersion,
    ) -> Result<TreeBundle, Error> {
        let tree_name = tree_name.as_ref();
        match self.open_trees.get(tree_name) {
            Some(tree) => Ok(tree.clone()),
            None => {
                match self.descriptors.get(tree_name.as_bytes())? {
                    Some(_descriptor) => {
                        // TODO: register new evolution
                        // TODO: check that root type provided matches evolution
                    }
                    None => {
                        trace!("Create new tree {tree_name}");
                        let mut tc = TypeCollection::new();
                        R::reflect(&mut tc);
                        let descriptor = TreeDescriptor {
                            next_temporary_id: 0,
                            next_global_id: None,
                            description: "".to_string(),
                            ts: [(evolution, tc)].into(),
                        };
                        let descriptor_bytes = rkyv::to_bytes::<_, 1024>(&descriptor)?;
                        self.descriptors
                            .insert(tree_name.as_bytes(), descriptor_bytes.as_slice())?;
                    }
                }
                let data = self.db.open_tree(tree_name.as_bytes())?;
                let journal = self
                    .db
                    .open_tree(format!("{tree_name}_journal").as_bytes())?;
                let latest_revision_index = self
                    .db
                    .open_tree(format!("{tree_name}_latest_revision_index").as_bytes())?;
                let bundle = TreeBundle {
                    data,
                    journal,
                    latest_revision_index,
                };
                self.open_trees
                    .insert(tree_name.to_string(), bundle.clone());
                Ok(bundle)
            }
        }
    }
}
