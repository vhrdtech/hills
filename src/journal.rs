use rkyv::{Archive, Deserialize, Serialize};
use crate::RecordId;

pub type Journal = Vec<JournalEntry>;

#[derive(Archive, Serialize, Deserialize)]
pub struct JournalEntry {
    id: RecordId,
    action: Action,
}

#[derive(Archive, Serialize, Deserialize)]
pub enum Action {
    Create,
    Modify,
    Release,
    ChangeState(u32),
    AssignGlobalId(u32),
}