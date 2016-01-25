# chicken-lmdb

Chicken Scheme bindings for the Lightning Memory-Mapped Database
Manager (LMDB) (http://symas.com/mdb/doc/index.html) database
management library.


## Library procedures

`(lmdb-open dbname [enckey])`
Opens or creates LMDB database with optional encryption key.

`(lmdb-delete dbname)`
Deletes LMDB database.

`(lmdb-ref db key)`
Looks up key in database.

`(lmdb-set! db key value)`
Sets a key-value pair in the database.

`(lmdb-close db)`
Closes database handle.

`(lmdb-count db)`
Returns number of key-value pairs in database.

`(lmdb-keys db)`
Returns a list of database keys.

`(lmdb-values db)`
Returns a list of database values.

`(lmdb-fold f init db)`
Fold over the keys and values in the database.

`(lmdb-for-each db)`
Iterate over the keys and values in the database.

`(hash-table->lmdb t dbfile [enckey])`
Saves SRFI-69 hash table to database.

`(lmdb->table dbfile [enckey])`
Load database into SRFI-69 hash table.

