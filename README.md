# chicken-lmdb

Chicken Scheme bindings for the Lightning Memory-Mapped Database
Manager (LMDB) (http://symas.com/mdb/doc/index.html) database
management library.


## Library procedures

`(db-open filename [key: enckey] [mapsize: size])`
Opens or creates LMDB database with optional encryption key and map size.

`(db-close db)`
Closes LMDB database handle.

`(db-begin db [dbname: dbname])`
Begins LMDB transaction with optional database name.

`(db-end db)`
Commits and ends LMDB transaction.

`(db-abort db)`
Aborts LMDB transaction.

`(db-delete-database dbname)`
Deletes LMDB database.

`(db-ref db key)`
Looks up key in database.

`(db-set! db key value)`
Sets a key-value pair in the database.

`(db-rm db key)`
Removes a key from the database.

`(db-count db)`
Returns number of key-value pairs in database.

`(db-keys db)`
Returns a list of database keys.

`(db-values db)`
Returns a list of database values.

`(db-key-len m)`
Returns the length of the current key.

`(db-value-len m)`
Returns the length of the current value.

`(db-key m buf)`
Copies the current key to the specified blob.

`(db-value m buf)`
Copies the current value to the specified blob.

`(db-fold f init db)`
Fold over the keys and values in the database.

`(db-for-each f db)`
Iterate over the keys and values in the database.

`(hash-table->db t dbfile [enckey])`
Saves SRFI-69 hash table to database.

`(db->hash-table dbfile [enckey])`
Load database into SRFI-69 hash table.

## Example

```scheme

;; lmdb encrypted key-value creation and lookup

(let* ((fname (make-pathname "." "mydb.mdb"))
       (keys (list "k1" 'k2 '(k3)))
       (values (list 'one 2 "three"))
       (cryptokey (string->blob "1234"))
       (mm (db-open fname key: cryptokey)))
  (db-begin mm)
  (let loop ((ks keys) (vs values))
    (if (> (length ks) 0) 
        (begin
          (db-set! mm (string->blob (->string (car ks))) (string->blob (->string (car vs))))
          (loop (cdr ks) (cdr vs)))))
  (db-end mm)
  (db-begin mm)
  (let ((res (let loop ((ks keys) (vs values))
               (if (= (length ks) 0) #t
                   (let ((v (db-ref mm (string->blob (->string (car ks))))))
                     (if (not (equal? (string->blob (->string (car vs))) v))  #f
                         (loop (cdr ks) (cdr vs)))))))
        )
    (db-end mm)
    (db-close mm)
    res)
  )
```

