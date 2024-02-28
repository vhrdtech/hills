<p align="center">
    Embeddable, distributed, typed kv database.
</p>

# Features and requirements

* Strongly typed, support any Rust type that [rkyv](https://github.com/rkyv/rkyv) supports.
* Data is organised as a collection of named kv trees.
* Each record is uniquely identified by a typed key: (id, revision).
  * Key's from one tree cannot be used with another, to improve code readability and help avoid errors.
  * Each client gets a key range for record creation, without waiting for server answer or when offline.
* Design data structures once, use as is everywhere (no data marshalling), converting to SQL and back.
* Directly access all the data locally without waiting as Rust types. Easy to use from immediate mode GUI.
  * Zero-copy access through a closure or owned record retrieval after deserialization.
* Record borrowing to avoid race conditions from multiple users. Editing or removing records is only allowed after checking out from server.
* Server implementation providing real time synchronisation and check-in check-out system.
* Distributed, but not on a massive scale.
* Optional versioning support: when a record is "released", it cannot be modified anymore, so old data relations are preserved as is.
  * Released records keep a number that could still be changed and used to implement custom lifetime state machines.
* Data evolution support:
  * Versions are kept with records and checked for compatibility.
  * Older code can access newer data.
  * Migration controlled by user (not yet implemented).
  * Procedural macro to extract data type definitions automatically.
* Record creation, modification, borrowing events are propagated to a user application, so code or GUI could instantly react to them.
* Indexing support: on each action user provided indexer method is called. Search, filtering and other operations can be implemented.
  * User holds a Clone-able index handle, that provides methods to interact with an index.
* Convenient macro to derive all the required traits on user types.
  * Serde's Serialize and Deserialize are derived as well, so the same data could be further shared / exported / etc.
* Access through trait interface to any tree with an OpaqueKey (holds tree name as well), mainly to implement universal database viewer GUI. [ron](https://github.com/ron-rs/ron) format is used to view and edit data, without having to create any custom UI.
* Client remembers UUID of a server on first connection, syncs up to date with it and rejects other servers later to avoid data corruption.

## Missing features and limitations

* Data backup (database folder could be backed up though when server is stopped).
* Not very well tested and wasn't yet used for a long time.
* Authentication is not implemented, should only be used on a private network or through VPN.
* Apart from indexing support there is not much in terms of queries and other relational database features.
* Record borrows are not saved when server is stopped or a client application is closed.
  * If client is restarted and connects back - borrows are shared with the client.
  * If server is restarted while a client still holds records, it can continue editing without server knowing.
* Offline record borrows: not hard to implement, just need to save borrows on the client as well.
* Most likely not usable now with big database sizes (large portion of RAM and more), depends on sled supporting this.
* Need to check server "identity" to avoid connecting to a server of a completely different application, though if tree names do not overlap, one server could actually be used.
* Procedural macro type structure extraction is more of a proof of concept and could be improved.
* Examples.
* Documentation.

## Potential features

* File storage and synchronisation.
* Configurable synchronisation granularity, now all trees are synchronised with all connected clients, so everyone essentially holds a copy of the whole database.

# Rationale

The idea for this project is to be able to create distributed applications without having to:
* Setup and maintain a separate database.
* Deal with a less diverse type system of a database (compared to Rust).
* Convert data back and forth.
* Develop and maintain a backend and potentially a set of additional data types.
* Wait on the client side for data and/or buffer before doing useful work.
* Deferring data processing to a backend, having to develop and maintain even more data types and APIs to accomplish this.

Additionally gaining several properties, such as:
* Zero-copy access thanks to [rkyv](https://github.com/rkyv/rkyv) and [sled](https://github.com/spacejam/sled). (Not measured, but potentially orders of magnitude faster than doing SQL queries).
* Ability to view all the data offline, without loosing any functions of an application.
* Create new records offline, that will be automatically synchronised later.

In particular, it is used for an internal product lifetime and manufacturing management system at vhrd.tech.
Keeping track of products, equipment, bill of materials, parts stock and ordering, serials and barcodes in use and others.
System is accessed from user workstations to view and edit data, as well as from various machinery (e.g. pick and place machine, when assembling boards and fetching parts from a warehouse stock).
