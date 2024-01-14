pub enum NodeKind {
    /// Node gives out mutable locks, accepts changes and serves data for other nodes.
    Server,
    /// Node talks to a server, produces new data and requests existing one when need be.
    Client,
    /// Node only receiving data from a server and storing it.
    Backup,
    /// Node that fully owns database file, no need for borrowing entries for editing.
    StandAlone,
}
