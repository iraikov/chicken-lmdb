#|

Chicken Scheme bindings for lmdb, fast key value database.
Modified for Chicken Scheme by Ivan Raikov.

Based on lmdb wrapper for LambdaNative - a cross-platform Scheme
framework Copyright (c) 2009-2015, University of British Columbia All
rights reserved.

Redistribution and use in source and binary forms, with or
without modification, are permitted provided that the
following conditions are met:

* Redistributions of source code must retain the above
copyright notice, this list of conditions and the following
disclaimer.

* Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following
disclaimer in the documentation and/or other materials
provided with the distribution.

* Neither the name of the University of British Columbia nor
the names of its contributors may be used to endorse or
promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
|#

(module lmdb

        (
         lmdb-debuglevel
         lmdb-makectx
         lmdb-destroyctx
         lmdb-init
         lmdb-cleanup
         lmdb-write
         lmdb-read
         lmdb-key-len
         lmdb-value-len
         lmdb-key
         lmdb-value
         lmdb-count
         lmdb-index-first
         lmdb-index-next
         lmdb-delete
         lmdb-open
         make-lmdb
         lmdb-set!
         lmdb-ref
         lmdb-close
         lmdb-count
         lmdb-keys
         lmdb-values
         lmdb-fold
         lmdb-for-each
         hash-table->lmdb
         lmdb->hash-table
         )

        
	(import scheme chicken foreign)
        (import (only extras printf)
                 (only data-structures identity)
                (only posix create-directory delete-directory)
                (only files make-pathname))
        (require-extension srfi-69 rabbit)

        (define lmdb-debuglevel (make-parameter 0))
        (define (lmdb-log level . x)
          (if (>= (lmdb-debuglevel) level) (apply printf x)))

#>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <lmdb.h>

#define C_bytevector_length(x)      (C_header_size(x))

static void chicken_Panic (C_char *) C_noret;
static void chicken_Panic (C_char *msg)
{
  C_word *a = C_alloc (C_SIZEOF_STRING (strlen (msg)));
  C_word scmmsg = C_string2 (&a, msg);
  C_halt (scmmsg);
  exit (5); /* should never get here */
}

static void chicken_ThrowException(C_word value) C_noret;
static void chicken_ThrowException(C_word value)
{
  char *aborthook = C_text("\003sysabort");

  C_word *a = C_alloc(C_SIZEOF_STRING(strlen(aborthook)));
  C_word abort = C_intern2(&a, aborthook);

  abort = C_block_item(abort, 0);
  if (C_immediatep(abort))
    Chicken_Panic(C_text("`##sys#abort' is not defined"));

#if defined(C_BINARY_VERSION) && (C_BINARY_VERSION >= 8)
  C_word rval[3] = { abort, C_SCHEME_UNDEFINED, value };
  C_do_apply(3, rval);
#else
  C_save(value);
  C_do_apply(1, abort, C_SCHEME_UNDEFINED);
#endif
}

void chicken_lmdb_exception (int code, int msglen, const char *msg) 
{
  C_word *a;
  C_word scmmsg;
  C_word list;

  a = C_alloc (C_SIZEOF_STRING (msglen) + C_SIZEOF_LIST(2));
  scmmsg = C_string2 (&a, (char *) msg);
  list = C_list(&a, 2, C_fix(code), scmmsg);
  chicken_ThrowException(list);
}

struct _mdb {
  MDB_env *env;
  MDB_dbi dbi;
  MDB_val key, value;
  MDB_txn *txn;
  MDB_cursor *cursor;
};

struct _mdb *_mdb_init(char *fname, char *dbname, int maxdbs)
{
  int rc;
  struct _mdb *m = (struct _mdb *)malloc(sizeof(struct _mdb));
  if ((rc = mdb_env_create(&m->env)) != 0) 
  {
     chicken_lmdb_exception (rc, 34, "_mdb_init: error in mdb_env_create");
  }
  if (maxdbs > 0) {
     if ((rc = mdb_env_set_maxdbs(m->env, maxdbs)) != 0)
     {
        chicken_lmdb_exception (rc, 38, "_mdb_init: error in mdb_env_set_maxdbs");
     }
  }
  if ((rc = mdb_env_open(m->env, fname, 0, 0664)) != 0)
  {
     chicken_lmdb_exception (rc, 33, "_mdb_init: error in mdb_env_open");
  }
  if ((rc = mdb_txn_begin(m->env, NULL, 0, &(m->txn))) != 0)
  {
     chicken_lmdb_exception (rc, 33, "_mdb_init: error in mdb_txn_begin");
  }
  if ((rc = mdb_open(m->txn, dbname, MDB_CREATE, &m->dbi)) != 0)
  {
     chicken_lmdb_exception (rc, 28, "_mdb_init: error in mdb_open");
  }
  m->cursor=NULL;
  return m;
}


int _mdb_write(struct _mdb *m, unsigned char *k, int klen, unsigned char *v, int vlen)
{
  int rc;
  m->key.mv_size = klen;
  m->key.mv_data = k;
  m->value.mv_size = vlen;
  m->value.mv_data = v;
  if ((rc = mdb_put(m->txn, m->dbi, &(m->key), &(m->value), 0)) != 0)
  {
     rc = mdb_txn_commit(m->txn);
     chicken_lmdb_exception (rc, 28, "_mdb_write: error in mdb_put");
  }
  return rc;
}

int _mdb_read(struct _mdb *m, unsigned char *k, int klen)
{
  int rc;
  m->key.mv_size = klen;
  m->key.mv_data = k;
  if ((rc = mdb_get(m->txn,m->dbi,&m->key, &m->value)) != 0)
  {
     chicken_lmdb_exception (rc, 27, "_mdb_read: error in mdb_get");
  }
  return rc;
}

int _mdb_index_first(struct _mdb *m)
{
  int rc;
  if (m->cursor) { mdb_cursor_close(m->cursor); }
  if ((rc = mdb_cursor_open(m->txn, m->dbi, &(m->cursor))) != 0)
  {
     chicken_lmdb_exception (rc, 42, "_mdb_index_first: error in mdb_cursor_open");
  } else 
    if ((rc = mdb_cursor_get(m->cursor, &(m->key), &(m->value), MDB_FIRST)) != 0)
    {
     chicken_lmdb_exception (rc, 42, "_mdb_index_first: error in mdb_cursor_get");
    }
  return rc;
}

int _mdb_index_next(struct _mdb *m)
{
  int rc;
  rc = mdb_cursor_get(m->cursor, &(m->key), &(m->value), MDB_NEXT);
  return rc;
}

int _mdb_del(struct _mdb *m, unsigned char *k, int klen)
{
  int rc;
  m->key.mv_size = klen;
  m->key.mv_data = k;
  rc = mdb_del(m->txn, m->dbi, &m->key, &m->value);
  if (!rc) { rc = mdb_txn_commit(m->txn); }
  return rc;
}

int _mdb_key_len(struct _mdb *m) { return m->key.mv_size; }
void _mdb_key(struct _mdb *m, unsigned char *buf) { memcpy(buf,m->key.mv_data,m->key.mv_size); }
int _mdb_value_len(struct _mdb *m) { return m->value.mv_size; }
void _mdb_value(struct _mdb *m, unsigned char *buf) { memcpy(buf,m->value.mv_data,m->value.mv_size); }

void _mdb_cleanup(struct _mdb *m)
{
  mdb_txn_commit(m->txn);
  mdb_close(m->env, m->dbi);
  mdb_env_close(m->env);
  free(m);
}

int _mdb_count(struct _mdb *m)
{
  MDB_stat s;
  int rc;
  rc = mdb_stat(m->txn, m->dbi, &s);
  return s.ms_entries;
}
<#

;; encode/decode context

(define lmdb-makectx rabbit-make)

(define lmdb-destroyctx rabbit-destroy!)

(define (lmdb-encoder keyctx)
  (lambda (obj) (rabbit-encode! keyctx obj)))

(define (lmdb-decoder keyctx)
  (lambda (u8v) 
    (condition-case 
     (rabbit-decode! keyctx u8v)
     [var () (begin (warning "lmdb-decoder" "failed to deserialize: " var)
                    'LMDB_FAILURE)])
    ))


;; ffi


(define lmdb-init0 (foreign-safe-lambda* 
                    nonnull-c-pointer ((nonnull-c-string fname) (c-string dbname) (int maxdbs))
                    "C_return (_mdb_init (fname,dbname,maxdbs));"))
(define (lmdb-init fname #!key (dbname #f) (maxdbs 0))
  (lmdb-init0 fname dbname maxdbs))

(define lmdb-cleanup (foreign-safe-lambda* 
                   void ((nonnull-c-pointer m))
                   "_mdb_cleanup (m);"))

(define (lmdb-write m key val) 
  (lmdb-log 3 "lmdb-write: ~A ~A = ~A~%" m key val)
  ((foreign-safe-lambda* int ((nonnull-c-pointer m) (scheme-object key) (scheme-object val))
#<<END
     int klen, vlen, result; void* keydata, *valdata;
     C_i_check_bytevector (key);
     C_i_check_bytevector (val);
     klen     = C_bytevector_length(key);
     keydata  = C_c_bytevector (key);
     vlen     = C_bytevector_length(val);
     valdata  = C_c_bytevector (val);
     result   = (void *)_mdb_write(m, keydata, klen, valdata, vlen);
     C_return (result);
END
) m key val))


(define (lmdb-read m key) 
  (lmdb-log 3 "lmdb-read: ~A~%" key)
  ((foreign-safe-lambda* int ((nonnull-c-pointer m) (scheme-object key))
#<<END
     int klen, result; void* keydata;
     C_i_check_bytevector (key);
     klen     = C_bytevector_length(key);
     keydata  = C_c_bytevector (key);
     result   = (void *)_mdb_read(m, keydata, klen);
     C_return (result);
END
) m key))


(define lmdb-key-len (foreign-safe-lambda* 
                      unsigned-int ((nonnull-c-pointer m)) 
                      "C_return (_mdb_key_len (m));"))
(define lmdb-value-len (foreign-safe-lambda* 
                        unsigned-int ((nonnull-c-pointer m)) 
                        "C_return (_mdb_value_len (m));"))

(define lmdb-key (foreign-safe-lambda* 
                  unsigned-int ((nonnull-c-pointer m) (scheme-object buf)) 
                  "_mdb_key (m, C_c_bytevector(buf));"))
(define lmdb-value (foreign-safe-lambda* 
                    unsigned-int ((nonnull-c-pointer m) (scheme-object buf)) 
                    "_mdb_value (m, C_c_bytevector(buf));"))

(define lmdb-count (foreign-safe-lambda* 
                    unsigned-int ((nonnull-c-pointer m)) 
                    "C_return (_mdb_count (m));"))

(define lmdb-index-first (foreign-safe-lambda* 
                          unsigned-int ((nonnull-c-pointer m)) 
                          "C_return (_mdb_index_first (m));"))
(define lmdb-index-next (foreign-safe-lambda* 
                         unsigned-int ((nonnull-c-pointer m)) 
                         "C_return (_mdb_index_next (m));"))


;; main interface

(define-record-type lmdb-session
  (make-lmdb-session handler encoder decoder ctx)
  lmdb-session?
  (handler       lmdb-session-handler )
  (encoder       lmdb-session-encoder )
  (decoder       lmdb-session-decoder )
  (ctx           lmdb-session-ctx )
  )


(define (lmdb-delete fname)
  (lmdb-log 2 "lmdb-delete ~A~%" fname)
  (if (file-exists? fname) (begin
     (delete-file (make-pathname fname "data.mdb"))
     (delete-file (make-pathname fname "lock.mdb"))
     (delete-directory fname)))) 


(define (lmdb-open fname #!key (key #f) (dbname #f) (maxdbs 0))
  (lmdb-log 2 "lmdb-open ~A ~A~%" fname key)
  (let ((ctx (and key (lmdb-makectx key))))
    (if (not (file-exists? fname)) (create-directory fname))
    (make-lmdb-session
     (lmdb-init fname dbname: dbname maxdbs: maxdbs)
     (if (not ctx) identity (lmdb-encoder ctx))
     (if (not ctx) identity (lmdb-decoder ctx)) 
     ctx)))


(define make-lmdb lmdb-open)


(define (lmdb-set! s key val)
  (lmdb-log 2 "lmdb-set! ~A ~A ~A~%" s key val)
  (let* ((lmdb-ptr (lmdb-session-handler s))
         (lmdb-encode (lmdb-session-encoder s))
         (u8key (lmdb-encode key))
         (u8val (lmdb-encode val)))
    (lmdb-write lmdb-ptr u8key u8val)))


(define (lmdb-ref s key)
  (lmdb-log 2 "lmdb-ref ~A ~A~%" s key)
  (let* ((m (lmdb-session-handler s))
         (encode (lmdb-session-encoder s))
         (decode (lmdb-session-decoder s))
         (u8key (encode key))
         (res (lmdb-read m u8key))
         (vlen (if (= res 0) (lmdb-value-len m) #f))
         (u8val (if vlen (make-blob vlen) #f)))
    (if u8val (begin (lmdb-value m u8val) (decode u8val)) #f)))


(define (lmdb-close s) 
  (lmdb-log 2 "lmdb-close ~A~%" s)
  (let ((ctx (lmdb-session-ctx s)))
    (lmdb-cleanup (lmdb-session-handler s))
    (if ctx (lmdb-destroyctx ctx))))


(define (lmdb-count s)
  (lmdb-log 2 "lmdb-count ~A~%" s)
  (lmdb-count (lmdb-session-handler s)))


(define (lmdb-get-key s)
  (lmdb-log 2 "lmdb-get-key ~A~%" s)
  (let* ((m (lmdb-session-handler s))
         (decode (lmdb-session-decoder s)))
    (let* ((klen (lmdb-key-len m))
           (k (make-blob klen)))
      (lmdb-key m k) 
      (decode k)
      ))
  )

(define (lmdb-keys s)
  (lmdb-log 2 "lmdb-keys ~A~%" s)
  (let* ((m (lmdb-session-handler s))
         (decode (lmdb-session-decoder s)))
    (lmdb-index-first m)
    (let loop ((idx (list (lmdb-get-key s))))
      (let ((res (lmdb-index-next m)))
        (if (not (= res 0)) idx
          (let* ((klen (lmdb-key-len m))
                 (k (make-blob klen)))
            (lmdb-key m k) 
            (loop (cons (decode k) idx))
            ))
        ))
    ))

(define (lmdb-get-value s)
  (lmdb-log 2 "lmdb-value ~A~%" s)
  (let* ((m (lmdb-session-handler s))
         (decode (lmdb-session-decoder s)))
    (let* ((vlen (lmdb-value-len m))
           (v (make-blob vlen)))
      (lmdb-value m v) 
      (decode v)
      ))
  )


(define (lmdb-values s)
  (lmdb-log 2 "lmdb-values ~A~%" s)
  (let* ((m (lmdb-session-handler s))
         (decode (lmdb-session-decoder s)))
    (lmdb-index-first m)
    (let loop ((idx (list (lmdb-get-value s))))
      (let ((res (lmdb-index-next m)))
        (if (not (= res 0)) idx
          (let* ((vlen (lmdb-value-len m))
                 (v (make-blob vlen)))
            (lmdb-value m v) 
            (loop (cons (decode v) idx))
            ))
        ))
    ))


(define (lmdb-fold f init s)
  (lmdb-log 2 "lmdb-fold ~A~%" s)
  (let* ((m (lmdb-session-handler s))
         (decode (lmdb-session-decoder s)))
    (lmdb-index-first m)
    (let loop ((ax (let ((k0 (lmdb-get-key s))
                         (v0 (lmdb-get-value s)))
                     (f k0 v0 init))))
      (let ((res (lmdb-index-next m)))
        (if (not (= res 0)) ax
          (let* ((klen (lmdb-key-len m))
                 (k (make-blob klen))
                 (vlen (lmdb-value-len m))
                 (v (make-blob vlen)))
            (lmdb-key m k) 
            (lmdb-value m v) 
            (loop (f (decode k) (decode v) ax))
            ))
        ))
    ))


(define (lmdb-for-each f s)
  (lmdb-log 2 "lmdb-for-each ~A~%" s)
  (let* ((m (lmdb-session-handler s))
         (decode (lmdb-session-decoder s)))
    (lmdb-index-first m)
    (let ((k0 (lmdb-get-key s))
          (v0 (lmdb-get-value s)))
      (f k0 v0))
    (let loop ()
      (let ((res (lmdb-index-next m)))
        (if (not (= res 0)) (begin)
          (let* ((klen (lmdb-key-len m))
                 (k (make-blob klen))
                 (vlen (lmdb-value-len m))
                 (v (make-blob vlen)))
            (lmdb-key m k) 
            (lmdb-value m v) 
            (f (decode k) (decode v))
            (loop)
            ))
        ))
    ))


(define (hash-table->lmdb t mfile . key)
  (lmdb-log 2 "table->lmdb ~A ~A ~A~%" t mfile key)
  (lmdb-delete mfile)
  (let ((s (apply lmdb-open (append (list mfile) key))))
    (hash-table-for-each t (lambda (k v) (lmdb-set! s (string->blob (symbol->string k)) v)))
    (lmdb-close s) #t))


(define (lmdb->hash-table mfile . key)
  (lmdb-log 2 "lmdb->table ~A ~A~%" mfile key)
  (if (not (file-exists? mfile)) #f
    (let* ((s (apply lmdb-open (append (list mfile) key)))
           (m (lmdb-session-handler s))
           (decode (lmdb-session-decoder s))
           (t (make-hash-table)))
      (if m 
          (begin
            (lmdb-index-first m) 
            (let ((k0 (lmdb-key s))
                  (v0 (lmdb-value s)))
              (hash-table-set! t k0 v0))
            (let ((fnl (let loop ()
                         (let* ((res (lmdb-index-next m))
                                (klen (if (= res 0) (lmdb-key-len m) #f))
                                (vlen (if (= res 0) (lmdb-value-len m) #f))
                                (u8val (if vlen (make-blob vlen) #f))
                                (u8key (if klen (make-blob klen) #f))
                                (k (if u8key (begin (lmdb-key m u8key) (decode u8key)) #f))
                                (v (if u8val (begin (lmdb-value m u8val) (decode u8val)) #f)))
                           (if (or (not (= res 0)) (equal? k 'LMDB_FAILURE) (equal? v 'LMDB_FAILURE))
                               t (begin
                                   (hash-table-set! t k v)
                                   (loop)))))))
              (lmdb-close s)
              fnl
              ))
          #f))))

)
