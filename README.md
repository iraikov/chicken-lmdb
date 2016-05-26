# chicken-lmdb

Chicken Scheme bindings for the Lightning Memory-Mapped Database
Manager (LMDB) (http://symas.com/mdb/doc/index.html) database
management library.


## Library procedures

`(lmdb-open filename [key: enckey])`
Opens or creates LMDB database with optional encryption key.

`(lmdb-close db)`
Closes LMDB database handle.

`(lmdb-begin db [dbname: dbname])`
Begins LMDB transaction with optional database name.

`(lmdb-end db)`
Commits and ends LMDB transaction.

`(lmdb-delete dbname)`
Deletes LMDB database.

`(lmdb-ref db key)`
Looks up key in database.

`(lmdb-set! db key value)`
Sets a key-value pair in the database.

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

`(lmdb->hash-table dbfile [enckey])`
Load database into SRFI-69 hash table.

## Example

```scheme

;; lmdb encrypted key-value creation and lookup

(let* ((fname (make-pathname "." "mydb.mdb"))
       (keys (list "k1" 'k2 '(k3)))
       (values (list 'one 2 "three"))
       (cryptokey (string->blob "1234"))
       (mm (lmdb-open fname key: cryptokey)))
  (lmdb-begin mm)
  (let loop ((ks keys) (vs values))
    (if (> (length ks) 0) 
        (begin
          (lmdb-set! mm (string->blob (->string (car ks))) (string->blob (->string (car vs))))
          (loop (cdr ks) (cdr vs)))))
  (lmdb-end mm)
  (lmdb-begin mm)
  (let ((res (let loop ((ks keys) (vs values))
               (if (= (length ks) 0) #t
                   (let ((v (lmdb-ref mm (string->blob (->string (car ks))))))
                     (if (not (equal? (string->blob (->string (car vs))) v))  #f
                         (loop (cdr ks) (cdr vs)))))))
        )
    (lmdb-end mm)
    (lmdb-close mm)
    res)
  )
```

