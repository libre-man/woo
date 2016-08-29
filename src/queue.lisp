(in-package :cl-user)
(defpackage woo.queue
  (:use :cl)
  (:export :make-multiqueue
           :multiqueue-enqueue
           :multiqueue-dequeue))
(in-package :woo.queue)

(defmacro define-speedy-function (name args &body body)
  `(progn (declaim (inline ,name))
          (defun ,name ,args
            (declare (optimize (speed 3) (safety 0) (debug 0)))
            ,@body)))

(defstruct (multiqueue ( :constructor %make-multiqueue))
  (subqueues-amount 0 :type fixnum)
  (subqueues #() :type simple-array)
  (locks #() :type simple-array)
  (try-amount 50 :type fixnum))

(defun make-multiqueue (process-amount &optional (c 2))
  "Make a multiqueue as described in https://arxiv.org/pdf/1411.1209.pdf.
PROCESS-AMOUNT is the amount of processes that are going to use the queue and c
is the C constant as described in the above paper. PROCESS-AMOUNT is only used
for performance tuning and it is safe to use the queue with more or less
processes."
  (let ((max-queue-index (* process-amount c)))
    (check-type max-queue-index fixnum))
  (let* ((subqueues-amount (the fixnum (* process-amount c)))
         (subqueues (make-array subqueues-amount
                                :initial-contents
                                (loop repeat subqueues-amount
                                      collect (cl-speedy-queue:make-queue 128))))
         (locks (make-array subqueues-amount
                            :initial-contents (loop repeat subqueues-amount
                                                    collect (bt:make-lock))
                            :adjustable nil
                            :fill-pointer nil)))
    (%make-multiqueue :subqueues-amount subqueues-amount
                      :subqueues subqueues
                      :locks locks)))

(defun multiqueue-empty-p (queue &optional (relaxed-p nil))
  "Check if a given multiqueue is empty. If RELAXED is non nil we will not wait
till we have locked each queue so the result might be t even if the queues are
empty. However this will NOT lead to data corruption."
  (declare (type multiqueue queue) (type boolean relaxed-p))
  (%multiqueue-empty-p queue relaxed-p))

(define-speedy-function %multiqueue-empty-p (queue relaxed)
  (with-slots (subqueues-amount subqueues locks)
      queue
    (declare (type (simple-array simple-array) subqueues)
             (type simple-array locks))
    (loop
      for i = (the fixnum 0) then (the fixnum (+ i 1))
      repeat (the fixnum subqueues-amount)
      when (the boolean (or relaxed (bt:acquire-lock (aref locks i) t)))
        if (the boolean (cl-speedy-queue:queue-empty-p (aref subqueues i)))
          do (unless relaxed
               (bt:release-lock (aref locks i)))
        else
          return (prog1 nil
                   (unless relaxed
                     (bt:release-lock (aref locks i))))
      end
      finally (return t))))

(defun multiqueue-enqueue (object queue random-state)
  (declare (type multiqueue queue))
  (%multiqueue-enqueue object queue random-state))

(define-speedy-function %multiqueue-enqueue (object queue random-state)
  (with-slots (subqueues-amount subqueues locks)
      queue
    (declare (type simple-array locks)
             (type (simple-array simple-array) subqueues)
             (type fixnum subqueues-amount))
    (flet ((rand () (the fixnum (random subqueues-amount random-state))))
      (loop for i = (rand)
            when (the boolean (bt:acquire-lock (aref locks i) nil))
              if (the boolean (cl-speedy-queue:queue-full-p (aref subqueues i)))
                do (bt:release-lock (aref locks i))
            else
              do (progn (cl-speedy-queue:enqueue object (aref subqueues i))
                        (bt:release-lock (aref locks i))
                        (return t))))))

(defun multiqueue-dequeue (queue random-state)
  (%multiqueue-dequeue queue random-state))

(define-speedy-function %multiqueue-dequeue (queue random-state)
  (with-slots (subqueues-amount subqueues locks try-amount)
      queue
    (declare (type simple-array locks)
             (type (simple-array simple-array) subqueues)
             (type fixnum subqueues-amount try-amount))
    (flet ((rand () (the fixnum (random (the fixnum subqueues-amount) random-state))))
      (loop for i = (rand)
              and cur = (the fixnum 0) then (the fixnum (if (= cur try-amount) 0 (1+ cur)))
            for subqueue = (aref subqueues i)
              and lock = (aref locks i)

            when (bt:acquire-lock lock nil)
              do (if (the boolean (cl-speedy-queue:queue-empty-p subqueue))
                     (bt:release-lock lock)
                     (return (let ((res (cl-speedy-queue:dequeue subqueue)))
                               (bt:release-lock lock)
                               (values res t))))
            end
            when (and (= cur 0) (the boolean (multiqueue-empty-p queue t)))
              do (return (values nil nil))
            end))))
