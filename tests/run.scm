(use lmdb files posix srfi-4 test)

(randomize)

(lmdb-debuglevel 3)

(define (random-blob n)
  (let ((v (make-u8vector n)))
    (let loop ((n n))
      (if (> n 0)
          (begin 
            (u8vector-set! v (- n 1) (random 255))
            (loop (- n 1)))
          (u8vector->blob v)))
    ))

(test-group "lmdb encrypted key-value creation and lookup"
            (test-assert
             (let* ((fname (make-pathname "." "unittest.mdb")))
               (lmdb-delete fname)
               (let* ((keys (list "k1" 'k2 '(k3)))
                      (values (list 'one 2 "three"))
                      (cryptokey (random-blob 24))
                      (mm (lmdb-open fname key: cryptokey)))
                 (let loop ((ks keys) (vs values))
                   (if (> (length ks) 0) 
                       (begin
                         (lmdb-set! mm (string->blob (->string (car ks))) (string->blob (->string (car vs))))
                         (loop (cdr ks) (cdr vs)))))
                 (lmdb-close mm)
                 (let* ((mm (lmdb-open fname key: cryptokey))
                        (res (let loop ((ks keys) (vs values))
                               (if (= (length ks) 0) #t
                                   (let ((v (lmdb-ref mm (string->blob (->string (car ks))))))
                                     (if (not (equal? (string->blob (->string (car vs))) v))  #f
                                         (loop (cdr ks) (cdr vs)))))))
                        )
                   (lmdb-close mm)
                   (lmdb-delete fname)
                   res)
                 ))
             ))

(test-group "lmdb unencrypted key-value creation and lookup"
            (test-assert
             (let* ((fname (make-pathname "." "unittest.mdb")))
               (lmdb-delete fname)
               (let* ((keys (list "k1" 'k2 '(k3)))
                      (values (list 'one 2 "three"))
                      (mm (lmdb-open fname)))
                 (let loop ((ks keys) (vs values))
                   (if (> (length ks) 0) 
                       (begin
                         (lmdb-set! mm (string->blob (->string (car ks))) (string->blob (->string (car vs))))
                         (loop (cdr ks) (cdr vs)))))
                 (lmdb-close mm)
                 (let* ((mm (lmdb-open fname))
                        (res (let loop ((ks keys) (vs values))
                               (if (= (length ks) 0) #t
                                   (let ((v (lmdb-ref mm (string->blob (->string (car ks))))))
                                     (if (not (equal? (string->blob (->string (car vs))) v))  #f
                                         (loop (cdr ks) (cdr vs)))))))
                        )
                   (lmdb-close mm)
                   (lmdb-delete fname)
                   res)
                 ))
             ))

(test-group "lmdb unencrypted key-value creation and fold"
             (let* ((fname (make-pathname "." "unittest.mdb")))
               (lmdb-delete fname)
               (let* ((keys (list "k1" 'k2 '(k3)))
                      (values (list 'one 2 "three"))
                      (mm (lmdb-open fname)))
                 (let loop ((ks keys) (vs values))
                   (if (> (length ks) 0) 
                       (begin
                         (lmdb-set! mm (string->blob (->string (car ks))) (string->blob (->string (car vs))))
                         (loop (cdr ks) (cdr vs)))))
                 (lmdb-close mm)
                 (let* ((mm (lmdb-open fname))
                        (res (lmdb-fold (lambda (k v ax) (cons (cons k v) ax)) '() mm))
                        )
                   (lmdb-close mm)
                   (lmdb-delete fname)
                   (test res (map (lambda (k v) (cons (string->blob (->string k)) (string->blob (->string v)))) 
                                  (list 'k2 "k1" '(k3))
                                  (list 2 'one "three")))
                   ))
             ))


(test-group "lmdb named database creation and lookup"
            (test-assert
             (let* ((fname (make-pathname "." "unittest.mdb")))
               (lmdb-delete fname)
               (let* ((keys (list "k1" 'k2 '(k3)))
                      (values (list 'one 2 "three"))
                      )
                 (let ((mm1 (lmdb-open fname dbname: "test1" maxdbs: 2)))
                   (let loop ((ks keys) (vs values))
                     (if (> (length ks) 0) 
                         (begin
                           (lmdb-set! mm1 (string->blob (->string (car ks))) (string->blob (->string (car vs))))
                           (loop (cdr ks) (cdr vs)))))
                   (lmdb-close mm1))
                 (let ((mm2 (lmdb-open fname dbname: "test2" maxdbs: 2)))
                   (let loop ((ks keys) (vs values))
                     (if (> (length ks) 0) 
                         (begin
                           (lmdb-set! mm2 (string->blob (->string (car vs))) (string->blob (->string (car ks))))
                           (loop (cdr ks) (cdr vs)))))
                   (lmdb-close mm2))
                 (let* ((mm1 (lmdb-open fname dbname: "test1" maxdbs: 2))
                        (res1 (let loop ((ks keys) (vs values))
                                (if (= (length ks) 0) #t
                                    (let ((v1 (lmdb-ref mm1 (string->blob (->string (car ks))))))
                                      (if (not (equal? (string->blob (->string (car vs))) v1))
                                          #f
                                          (loop (cdr ks) (cdr vs)))))))
                        )
                   (lmdb-close mm1)
                   (let* ((mm2 (lmdb-open fname dbname: "test2" maxdbs: 2))
                          (res2 (let loop ((ks keys) (vs values))
                                  (if (= (length ks) 0) #t
                                      (let ((v2 (lmdb-ref mm2 (string->blob (->string (car vs))))))
                                        (if (not (equal? (string->blob (->string (car ks))) v2))
                                            #f
                                            (loop (cdr ks) (cdr vs)))))))
                          )
                     (lmdb-close mm2)
                     (lmdb-delete fname)
                     (and res1 res2))
                   ))
               ))
            )
