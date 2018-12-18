
(import (chicken base) (chicken format) (chicken process)
        (chicken process-context) srfi-13 compile-file)

(define args (command-line-arguments))

(define (lmdb-try-compile ldflags cppflags)
  (and (try-compile 
	(string-append "#include <stdlib.h>\n"
		       "#include <stdio.h>\n"
		       "#include <string.h>\n"
	 	       "#include <lmdb.h>\n"
		       "int main(int argc, char **argv) { mdb_env_create(NULL); return 0; }\n")
	ldflags: ldflags
	cflags: cppflags
        verbose: #t)
       (cons cppflags ldflags)))

(define-syntax lmdb-test 
  (syntax-rules ()
    ((_ (flags ...))
     (condition-case (lmdb-try-compile flags ...)
		     (t ()    #f)))))

(define cpp+ld-options
  (let ((cflags (get-environment-variable "LMDB_CFLAGS"))
	(lflags (get-environment-variable "LMDB_LFLAGS"))
	(libs   (get-environment-variable "LMDB_LIBS")))
    (if (and cflags lflags libs)
	(lmdb-test ("<lmdb.h>" cflags (string-append lflags " " libs)))
	(or (lmdb-test ("-llmdb" ""))
	    (error "unable to figure out location of LMDB")))
    ))

(define c-options  (car cpp+ld-options))
(define ld-options (cdr cpp+ld-options))

(define cmd (intersperse
             (append args (list (sprintf "-L \"~A\"" ld-options)
                                (sprintf "-C \"~A\"" c-options)))
             " "))
(system (string-concatenate cmd))
