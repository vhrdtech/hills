use hills_base::GenericKey;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Serialize, Deserialize)]
pub struct JournalEntry {
    serial: u64,
    key: GenericKey,
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
