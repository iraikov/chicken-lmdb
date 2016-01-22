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
                      (mm (lmdb-open fname cryptokey)))
                 (let loop ((ks keys) (vs values))
                   (if (> (length ks) 0) 
                       (begin
                         (lmdb-set! mm (string->blob (->string (car ks))) (string->blob (->string (car vs))))
                         (loop (cdr ks) (cdr vs)))))
                 (lmdb-close mm)
                 (let* ((mm (lmdb-open fname cryptokey))
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

