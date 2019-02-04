;;; ob-bigquery.el --- org-babel functions for bigquery evaluation

;; Copyright (C) Nick Hansen

;; Author: Nick Hansen
;; Keywords: literate programming, reproducible research, google bigquery
;; Homepage: TODO
;; Version: 0.01

;;; License:

;; This program is free software; you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation; either version 3, or (at your option)
;; any later version.
;;
;; This program is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.
;;
;; You should have received a copy of the GNU General Public License
;; along with GNU Emacs; see the file COPYING.  If not, write to the
;; Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
;; Boston, MA 02110-1301, USA.

;;; Commentary:

;; TODO

;;; Requirements:

;; It is expected that you have the google cloud command line tools,
;; and in particular the bq module, installed and configured.

;;; Code:
(require 'ob)
(require 'cl)
(require 'subr-x)
;(require 'ob-ref)
(require 'ob-eval)
(require 'json)
;; possibly require modes required for your language

;; optionally define a file extension for this language
(add-to-list 'org-babel-tangle-lang-exts '("bigquery" . "sql"))

;; optionally declare default header arguments for this language
(defvar org-babel-default-header-args:bigquery '())

;; This function expands the body of a source code block by doing
;; things like prepending argument definitions to the body, it should
;; be called by the `org-babel-execute:bigquery' function below.
(defun org-babel-expand-body:bigquery (body params &optional processed-params)
  "Expand BODY according to PARAMS, return the expanded body."
  (let ((vars (nth 1 (or processed-params (org-babel-process-params params)))))
    (concat
     (mapconcat ;; define any variables
      (lambda (pair)
        ;; TODO set up parameter expansion
        (format "## %s=%S"
                (car pair) (org-babel-bigquery-var-to-bigquery (cdr pair))))
      vars "\n") "\n" body "\n")))

;; This is the main function which is called to evaluate a code
;; block.
;;
;; This function will evaluate the body of the source code and
;; return the results as emacs-lisp depending on the value of the
;; :results header argument
;; - output means that the output to STDOUT will be captured and
;;   returned
;; - value means that the value of the last statement in the
;;   source code block will be returned
;;
;; The most common first step in this function is the expansion of the
;; PARAMS argument using `org-babel-process-params'.
;;
;; Please feel free to not implement options which aren't appropriate
;; for your language (e.g. not all languages support interactive
;; "session" evaluation).  Also you are free to define any new header
;; arguments which you feel may be useful -- all header arguments
;; specified by the user will be available in the PARAMS variable.
(defun org-babel-execute:bigquery (body params)
  "Execute a block of Bigquery code with org-babel.
This function is called by `org-babel-execute-src-block'"
  (message "Executing Bigquery source code block")
  (let* (;;; From template
         ;(processed-params (org-babel-process-params params))
         ;; set the session if the session variable is non-nil
         ;(session (org-babel-bigquery-initiate-session (first processed-params)))
         ;; variables assigned for use in the block
         ;(vars (second processed-params))
         ;(result-params (third processed-params))
         ;; either OUTPUT or VALUE which should behave as described above
         ;(result-type (fourth processed-params))
         ;; expand the body with `org-babel-expand-body:bigquery'
         ;(full-body (org-babel-expand-body:bigquery
         ;            body params processed-params))

         (result-params (cdr (assq :result-params params)))
         (project-id (cdr (assq :project-id params)))
         (destination-table (cdr (assq :destination-table params)))
         (max-rows (or (cdr (assq :max-rows params)) 10))
         (global-flags (let ((flags " --format json --headless "))
                         (when project-id
                           (setq flags
                                 (concat flags
                                         (format "--project_id %s " project-id))))
                         flags))
         (command-flags (let ((flags (format " --nouse_legacy_sql --max_rows=%s "
                                             max-rows)))
                          (when destination-table
                            (setq flags
                                  (concat flags
                                          (format "--destination_table=%s --replace "
                                                  destination-table))))
                          flags))
         (command (concat "bq"
                          global-flags
                          "query"
                          command-flags)))
    (message "Bigquery command: %s" command)
    ;; This is org-babel-eval but with different error handling, because
    ;; bq doesn't send sql parse errors to STDERR
    (let ((err-buff (get-buffer-create " *Org-Babel Error*")) exit-code)
      (with-current-buffer err-buff (erase-buffer))
      (with-temp-buffer
        (insert body)
        (setq exit-code
              (org-babel--shell-command-on-region
               (point-min) (point-max) command err-buff))
        (if (or (not (numberp exit-code)) (> exit-code 0))
            (progn
              (with-current-buffer err-buff
                (message "Bigquery evaluation failure: %s" (buffer-string)))
              (org-babel-eval-error-notify exit-code (buffer-string))
              (save-excursion
                (when (get-buffer org-babel-error-buffer-name)
                  (with-current-buffer org-babel-error-buffer-name
                    (unless (derived-mode-p 'compilation-mode)
                      (compilation-mode))
                    ;; Compilation-mode enforces read-only, but Babel expects the buffer modifiable.
                    (setq buffer-read-only nil))))
              nil)
          (org-babel-bigquery-json-table-to-emacs-table
           (buffer-string)))))))

(defun org-babel-bigquery-process-json-table-entry (entry)
    (let ((entry-name (concat path (car entry)))
          (entry-value (cdr entry)))
      (cond
       ((stringp entry-value)
        (progn (puthash entry-name
                        (cons entry-value
                              (gethash entry-name row))
                        row)
               (setq row-height (max row-height (length (gethash entry-name row))))
               (add-to-list 'columns entry-name)))
       ((vectorp entry-value)
        (->> entry-value
             (-map (lambda (v) (cons (car entry) v)))
             (-map #'org-babel-bigquery-process-json-table-entry)))
       ((listp entry-value)
        (-map (lambda (v)
                (let ((path (concat entry-name ".")))
                  (org-babel-bigquery-process-json-table-entry v)))
              entry-value)))))

(defun org-babel-bigquery-json-table-to-emacs-table (json-str)
  (let* (columns
         (json-key-type 'string)
         (path "")
         (all-rows (->> (json-read-from-string json-str)
                        (--map (let ((row (make-hash-table :test 'equal))
                                     (row-height 0))
                                 (-map #'org-babel-bigquery-process-json-table-entry it)
                                 (cons row row-height)))))
         (columns (cl-sort columns 'string-lessp))
         (all-rows (--mapcat (let ((row-vals (car it))
                                   (row-height (cdr it))
                                   rows)
                               (while (< 0 row-height)
                                 (setq rows (cons (-map (lambda (c)
                                                          (let ((col-vals (gethash c row-vals)))
                                                            (if col-vals
                                                                (progn (puthash c (cdr col-vals) row-vals)
                                                                       (car col-vals))
                                                              "")))
                                                        columns)
                                                  rows))
                                 (setq row-height (1- row-height)))
                               (cons 'hline (reverse rows)))
                             all-rows)))
    (cons columns all-rows)))

(defun bigquery-display-table-schema (table-identifier)
  (string-match "^\\(\\([a-zA-Z0-9_-]+\\)[.:]\\)?\\([a-zA-Z0-9_]+\\)\\.\\([a-zA-Z0-9_]+\\)$"
                table-identifier)
  (let* ((project-id (match-string 2 table-identifier))
         (dataset-id (match-string 3 table-identifier))
         (table-name (match-string 4 table-identifier))
         (schema-buffer (get-buffer-create (format "*BigQuery Schema: %s*" table-identifier)))
         (schema-json (shell-command-to-string (concat "bq show --schema "
                                                       (when project-id
                                                         (concat project-id ":"))
                                                       dataset-id "." table-name)))
         (json-key-type 'keyword)
         (header-prefix "*")
         (name-prefix ""))
    (cl-labels ((pprint-field-value (value)
                                    (insert (format "%-75s%24s\n"
                                                    (concat header-prefix " " name-prefix (alist-get :name value) " ")
                                                    (concat ":" (capitalize (alist-get :type value))
                                                            ":" (capitalize (alist-get :mode value)) ":"))))
                (pprint-schema-entity (value)
                                      (cond
                                       ((vectorp value)
                                        (-map #'pprint-schema-entity value))
                                       ((member '(:type . "RECORD") value)
                                        (progn
                                          (pprint-field-value value)
                                          (let ((header-prefix (concat header-prefix "*"))
                                                (name-prefix (concat name-prefix (alist-get :name value) ".")))
                                            (pprint-schema-entity (alist-get :fields value)))))
                                       (t (pprint-field-value value)))))
      (with-current-buffer schema-buffer
        (erase-buffer)
        (org-mode)
        (condition-case nil
            (let ((schema-data (json-read-from-string schema-json)))
              (pprint-schema-entity schema-data))
          (error
           (insert "Schema Error:\n" schema-json)))
        (goto-char 0)))
    (display-buffer schema-buffer)))

(defun bigquery-display-table-schema-at-point ()
  (interactive)
  (bigquery-display-table-schema (thing-at-point 'filename)))


;;;;;; SOME FUNCTIONS THAT WERE IN OB-TEMPLATE.EL THAT I PROBABLY DON'T CARE ABOUT

;; This function should be used to assign any variables in params in
;; the context of the session environment.
(defun org-babel-prep-session:bigquery (session params)
  "Prepare SESSION according to the header arguments specified in PARAMS."
  )

(defun org-babel-bigquery-var-to-bigquery (var)
  "Convert an elisp var into a string of bigquery source code
specifying a var of the same value."
  (format "%S" var))

(defun org-babel-bigquery-initiate-session (&optional session)
  "If there is not a current inferior-process-buffer in SESSION then create.
Return the initialized session."
  (unless (string= session "none")
    ))

(when org-src-lang-modes
  (add-to-list 'org-src-lang-modes '("bigquery" . sql)))

(provide 'ob-bigquery)
;;; ob-bigquery.el ends here


;; (org-babel-do-load-languages
;;  'org-babel-load-languages
;;  '((bigquery . t)))
