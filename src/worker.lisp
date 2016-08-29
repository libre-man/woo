(in-package :cl-user)
(defpackage woo.worker
  (:use :cl
        :woo.specials)
  (:import-from :woo.ev
                :*evloop*
                :with-sockaddr)
  (:import-from :woo.queue
                :make-multiqueue
                :multiqueue-enqueue
                :multiqueue-dequeue)
  (:export :make-cluster
           :stop-cluster
           :kill-cluster
           :add-job-to-cluster))
(in-package :woo.worker)

(defparameter *worker* nil)

(defvar *worker-counter* 0)

(defstruct (worker (:constructor %make-worker))
  (id (incf *worker-counter*))
  (random-state (make-random-state t))
  evloop
  dequeue-async
  stop-async
  process-fn
  thread
  main-queue
  (status :running))

(defun notify-new-job (worker)
  (lev:ev-async-send (worker-evloop worker) (worker-dequeue-async worker)))

(defun stop-worker (worker)
  (vom:debug "[~D] Stopping a worker..." (worker-id worker))
  (with-slots (evloop stop-async status) worker
    (setf status :stopping)
    (lev:ev-async-send evloop stop-async)))

(defun kill-worker (worker)
  (vom:debug "[~D] Killing a worker..." (worker-id worker))
  (with-slots (status thread) worker
    (setf status :stopping)
    (bt:destroy-thread thread)))

(cffi:defcallback worker-dequeue :void ((evloop :pointer) (listener :pointer) (events :int))
  (declare (ignore evloop listener events))
  (loop with queue = (worker-main-queue *worker*)
        for (socket found) = (multiple-value-list (multiqueue-dequeue queue (worker-random-state *worker*)))
        while found
        do (funcall (worker-process-fn *worker*) socket)))

(cffi:defcallback worker-stop :void ((evloop :pointer) (listener :pointer) (events :int))
  (declare (ignore listener events))
  ;; Close existing all sockets.
  (maphash (lambda (fd socket)
             (wev:close-socket socket))
           wev:*data-registry*)

  ;; Stop all events.
  (lev:ev-break evloop lev:+EVBREAK-ALL+))

(defun finalize-worker (worker)
  (with-slots (evloop dequeue-async stop-async thread status) worker
    (cffi:foreign-free dequeue-async)
    (cffi:foreign-free stop-async)
    (setf evloop nil
          dequeue-async nil
          stop-async nil
          thread nil
          status :stopped)))

(defun make-worker (process-fn when-died main-queue)
  (let* ((dequeue-async (cffi:foreign-alloc '(:struct lev:ev-async)))
         (stop-async (cffi:foreign-alloc '(:struct lev:ev-async)))
         (worker (%make-worker :dequeue-async dequeue-async
                               :stop-async stop-async
                               :process-fn process-fn
                               :main-queue main-queue))
         (worker-lock (bt:make-lock)))
    (lev:ev-async-init dequeue-async 'worker-dequeue)
    (lev:ev-async-init stop-async 'worker-stop)
    (setf (worker-thread worker)
          (bt:make-thread
           (lambda ()
             (bt:acquire-lock worker-lock)
             (let ((*worker* worker))
               (wev:with-sockaddr
                 (unwind-protect
                      (wev:with-event-loop ()
                        (setf (worker-evloop worker) *evloop*)
                        (bt:release-lock worker-lock)
                        (lev:ev-async-start *evloop* dequeue-async)
                        (lev:ev-async-start *evloop* stop-async))
                   (unless (eq (worker-status worker) :stopping)
                     (vom:debug "[~D] Worker has died" (worker-id worker))
                     (funcall when-died worker))
                   (finalize-worker worker)
                   (vom:debug "[~D] Bye." (worker-id worker))))))
           :initial-bindings (default-thread-bindings)
           :name "woo-worker"))
    (sleep 0.1)
    (bt:acquire-lock worker-lock)
    worker))

(defstruct (cluster (:constructor %make-cluster
                        (queue &optional workers)))
  (workers '())
  queue
  (random-state (make-random-state t)))

(defun add-job-to-cluster (cluster job)
  (multiqueue-enqueue job
                      (cluster-queue cluster)
                      (cluster-random-state cluster))
  (mapc #'notify-new-job
        (cluster-workers cluster)))

(defun make-cluster (worker-num process-fn)
  (let ((cluster (%make-cluster (make-multiqueue worker-num))))
    (labels ((make-new-worker ()
               (vom:debug "Starting a new worker...")
               (make-worker process-fn
                            (lambda (worker)
                              (setf (cluster-workers cluster)
                                    (cons (make-new-worker)
                                          (remove worker (cluster-workers cluster) :test #'eq))))
                            (cluster-queue cluster))))
      (setf (cluster-workers cluster)
            (loop repeat worker-num
                  collect (make-new-worker))))
    cluster))

(defun cluster-running-workers (cluster)
  (remove-if-not #'worker-thread (cluster-workers cluster)))

(defun stop-cluster (cluster)
  (let ((workers (cluster-running-workers cluster)))
    (mapc #'stop-worker workers)
    (loop repeat 100
          while (find-if #'worker-thread workers)
          do (sleep 0.1)
          finally
             (mapc #'kill-worker (cluster-running-workers cluster)))))

(defun kill-cluster (cluster)
  (mapc #'kill-worker (cluster-running-workers cluster)))
