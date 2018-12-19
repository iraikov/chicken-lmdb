(import scheme (chicken base) (chicken blob) (chicken string)
        (chicken random) (chicken pathname) (chicken file)
        lmdb-ht srfi-4 srfi-69 test)


(debuglevel 3)


;(test-group "lmdb encrypted key-value creation and lookup"
;            (test-assert
             (let* ((fname (make-pathname "." "unittest.mdb")))
               ;(db-delete-database fname)
               (let* ((keys (list "k1" 'k2 '(k3)))
                      (values (list 'one 2 "three"))
                      (cryptokey (random-bytes (make-blob 24)))
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
                   (db-delete-database fname)
                   res)
                 ))
             


(test-group "lmdb unencrypted key-value creation and lookup"
            (test-assert
             (let* ((fname (make-pathname "." "unittest.mdb")))
               (db-delete-database fname)
               (let* ((keys (list "k1" 'k2 '(k3)))
                      (values (list 'one 2 "three"))
                      (mm (db-open fname)))
                 (db-begin mm)
                 (let loop ((ks keys) (vs values))
                   (if (> (length ks) 0) 
                       (begin
                         (db-set! mm (string->blob (->string (car ks))) (string->blob (->string (car vs))))
                         (loop (cdr ks) (cdr vs)))))
                 (db-end mm)
                 (db-begin mm)
                 (let* ((res (let loop ((ks keys) (vs values))
                               (if (= (length ks) 0) #t
                                   (let ((v (db-ref mm (string->blob (->string (car ks))))))
                                     (if (not (equal? (string->blob (->string (car vs))) v))  #f
                                         (loop (cdr ks) (cdr vs)))))))
                        )
                   (db-end mm)
                   (db-close mm)
                   (db-delete-database fname)
                   res)
                 ))
             ))

(test-group "lmdb unencrypted key-value creation and fold / for-each"
             (let* ((fname (make-pathname "." "unittest.mdb")))
               (db-delete-database fname)
               (let* ((keys (list "k1" 'k2 '(k3)))
                      (values (list 'one 2 "three"))
                      (mm (db-open fname)))
                 (db-begin mm)
                 (let loop ((ks keys) (vs values))
                   (if (> (length ks) 0) 
                       (begin
                         (db-set! mm (string->blob (->string (car ks))) (string->blob (->string (car vs))))
                         (loop (cdr ks) (cdr vs)))))
                 (db-end mm)
                 (db-begin mm)
                 (let ((res (db-fold (lambda (k v ax) (cons (cons k v) ax)) '() mm)))
                   (db-end mm)
                   (test res (map (lambda (k v) (cons (string->blob (->string k)) (string->blob (->string v)))) 
                                  (list 'k2 "k1" '(k3))
                                  (list 2 'one "three"))))
                 (db-begin mm)
                 (let ((res (make-parameter '())))
                   (db-for-each (lambda (k v) (res (cons (cons k v) (res)))) mm)
                   (db-end mm)
                   (test (res)
                         (map (lambda (k v) (cons (string->blob (->string k)) (string->blob (->string v)))) 
                              (list 'k2 "k1" '(k3))
                              (list 2 'one "three"))))
                 (db-close mm)
                 (db-delete-database fname)
                 ))
             )


(test-group "lmdb unencrypted key-value creation and conversion to/from hash tables"
             (let* ((fname (make-pathname "." "unittest.mdb")))
               (db-delete-database fname)
               (let* ((keys (list "k1" 'k2 '(k3)))
                      (values (list 'one 2 "three"))
                      (mm (db-open fname)))
                 (db-begin mm)
                 (let loop ((ks keys) (vs values))
                   (if (> (length ks) 0) 
                       (begin
                         (db-set! mm (string->blob (->string (car ks))) (string->blob (->string (car vs))))
                         (loop (cdr ks) (cdr vs)))))
                 (db-end mm)
                 (db-close mm)
                 (let ((ht (db->hash-table fname)))
                   ht)
                   ;(test (hash-table->alist ht)
                   ;      (map (lambda (k v) (cons (string->blob (->string k)) (string->blob (->string v)))) 
                   ;           (list 'k2 "k1" '(k3))
                   ;           (list 2 'one "three"))))
                 (db-delete-database fname)
                 ))
             )


(test-group "lmdb named database creation and lookup"
            (test-assert
             (let* ((fname (make-pathname "." "unittest.mdb")))
               (db-delete-database fname)
               (let* ((keys (list "k1" 'k2 '(k3)))
                      (values (list 'one 2 "three"))
                      )
                 (let ((mm (db-open fname maxdbs: 2)))
                   (db-begin mm dbname: "test1" )
                   (let loop ((ks keys) (vs values))
                     (if (> (length ks) 0) 
                         (begin
                           (db-set! mm (string->blob (->string (car ks))) (string->blob (->string (car vs))))
                           (loop (cdr ks) (cdr vs)))))
                   (db-end mm)
                   (db-begin mm dbname: "test2" )
                   (let loop ((ks keys) (vs values))
                     (if (> (length ks) 0) 
                         (begin
                           (db-set! mm (string->blob (->string (car vs))) (string->blob (->string (car ks))))
                           (loop (cdr ks) (cdr vs)))))
                   (db-end mm)
                   (db-begin mm dbname: "test1" )
                   (let* ((res1 (let loop ((ks keys) (vs values))
                                  (if (= (length ks) 0) #t
                                      (let ((v1 (db-ref mm (string->blob (->string (car ks))))))
                                        (if (not (equal? (string->blob (->string (car vs))) v1))
                                            #f
                                            (loop (cdr ks) (cdr vs)))))))
                          )
                     (db-end mm)
                     (db-begin mm dbname: "test2" )
                     (let* ((res2 (let loop ((ks keys) (vs values))
                                    (if (= (length ks) 0) #t
                                        (let ((v2 (db-ref mm (string->blob (->string (car vs))))))
                                          (if (not (equal? (string->blob (->string (car ks))) v2))
                                              #f
                                              (loop (cdr ks) (cdr vs)))))))
                            )
                       (db-end mm)
                       (db-close mm)
                       (db-delete-database fname)
                       (and res1 res2))
                     ))
                 ))
             )
            )

(test-group "lmdb mdb-notfound condition"
            (let* ((fname (make-pathname "." "unittest.mdb")))
              (db-delete-database fname)
              (let ((mm (db-open fname maxdbs: 2)))
                (db-begin mm)
                (test "condition-case for get missing key"
                      'missing
                      (condition-case (db-ref mm (string->blob "asdfasdf"))
                                      ((exn lmdb mdb-notfound) 'missing)
                                      (var () (print "mdb-notfound error: " var))
                                      ))
                (db-end mm)
                (db-close mm))))

(test-group "abort transaction"
	    (let* ((fname (make-pathname "." "unittest.mdb")))
              (db-delete-database fname)
              (let ((mm (db-open fname maxdbs: 2)))
                (db-begin mm)
		;; set foo
		(db-set! mm (string->blob "foo") (string->blob "one"))
		(db-end mm)
		(db-begin mm)
		;; foo is still set
		(test (string->blob "one")
		      (db-ref mm (string->blob "foo")))
		;; set bar
		(db-set! mm (string->blob "bar") (string->blob "two"))
		;; abort
		(db-abort mm)
		(db-begin mm)
		;; foo is still set
		(test (string->blob "one")
		      (db-ref mm (string->blob "foo")))
		;; bar is not set
		(test 'missing
		      (condition-case (db-ref mm (string->blob "bar"))
			((exn lmdb mdb-notfound) 'missing)))
                (db-end mm)
                (db-close mm))))

(test-exit)
