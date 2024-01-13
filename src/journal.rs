use crate::RecordId;

pub type Journal = Vec<JournalEntry>;

pub struct JournalEntry {
    id: RecordId,
    action: Action,
}

pub enum Action {
    Create,
    Modify,
    Release,
    ChangeState(u32),
    AssignGlobalId(u32),
}