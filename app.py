from celery import Celery
from logging import getLogger
import functools
import gevent

logger = getLogger(__name__)
app = Celery('test-gevent',
             broker="amqp://guest:guest@localhost:5672",
             task_ignore_result=True,
             broker_pool_limit=3)



def timeout(timeout):
    def wrapper(f):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            with gevent.Timeout(timeout):
                try:
                    return f(*args, **kwargs)
                except gevent.Timeout:
                    logger.error("[%s] Timed Out", gevent.getcurrent().minimal_ident)
                    return -1
            return None
        return wrapped
    return wrapper


@app.task
def log(i):
    logger.info("Got number %s", i)

@app.task
@timeout(1)
def delay_in_loop():
    while True:
        log.apply_async(args=(gevent.getcurrent().minimal_ident,) , queue='other')
        logger.info("[%s] Queue Type %s",
                    gevent.getcurrent().minimal_ident,
                    type(app.producer_pool._resource))


@app.task
def loops():
    delay_in_loop.delay()
    delay_in_loop.delay()
    delay_in_loop.delay()
    delay_in_loop.delay()
    delay_in_loop.delay()

# if broker_pool_limit less than the number of greenthreads then
# the following error occurs during gevent timeout
"""

[2025-12-31 11:12:49,765: ERROR/MainProcess] Task app.delay_in_loop[c5eb2860-52af-4cfb-9701-79010966a273] raised unexpected: RuntimeError('Semaphore released too many times')
Traceback (most recent call last):
  File "/Users/nate/.local/share/uv/python/cpython-3.14.0rc2-macos-aarch64-none/lib/python3.14/queue.py", line 202, in get
    self.not_empty.wait()
    ~~~~~~~~~~~~~~~~~~~^^
  File "/Users/nate/.local/share/uv/python/cpython-3.14.0rc2-macos-aarch64-none/lib/python3.14/threading.py", line 378, in wait
    self._acquire_restore(saved_state)
    ~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^
  File "/Users/nate/.local/share/uv/python/cpython-3.14.0rc2-macos-aarch64-none/lib/python3.14/threading.py", line 326, in _acquire_restore
    self._lock.acquire()           # Ignore saved state
    ~~~~~~~~~~~~~~~~~~^^
  File "/Users/nate/wave/test-celery/.venv/lib/python3.14/site-packages/gevent/thread.py", line 302, in acquire
    acquired = BoundedSemaphore.acquire(self, blocking, timeout)
  File "src/gevent/_semaphore.py", line 184, in gevent._gevent_c_semaphore.Semaphore.acquire
  File "src/gevent/_semaphore.py", line 253, in gevent._gevent_c_semaphore.Semaphore.acquire
  File "src/gevent/_abstract_linkable.py", line 529, in gevent._gevent_c_abstract_linkable.AbstractLinkable._wait
  File "src/gevent/_abstract_linkable.py", line 495, in gevent._gevent_c_abstract_linkable.AbstractLinkable._wait_core
  File "src/gevent/_abstract_linkable.py", line 504, in gevent._gevent_c_abstract_linkable.AbstractLinkable._wait_core
  File "src/gevent/_abstract_linkable.py", line 498, in gevent._gevent_c_abstract_linkable.AbstractLinkable._wait_core
  File "src/gevent/_abstract_linkable.py", line 450, in gevent._gevent_c_abstract_linkable.AbstractLinkable._AbstractLinkable__wait_to_be_notified
  File "src/gevent/_abstract_linkable.py", line 459, in gevent._gevent_c_abstract_linkable.AbstractLinkable._switch_to_hub
  File "src/gevent/_greenlet_primitives.py", line 61, in gevent._gevent_c_greenlet_primitives.SwitchOutGreenletWithLoop.switch
  File "src/gevent/_greenlet_primitives.py", line 65, in gevent._gevent_c_greenlet_primitives.SwitchOutGreenletWithLoop.switch
  File "src/gevent/_gevent_c_greenlet_primitives.pxd", line 35, in gevent._gevent_c_greenlet_primitives._greenlet_switch
gevent.timeout.Timeout: 1 second

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/Users/nate/wave/test-celery/.venv/lib/python3.14/site-packages/celery/app/trace.py", line 479, in trace_task
    R = retval = fun(*args, **kwargs)
                 ~~~^^^^^^^^^^^^^^^^^
  File "/Users/nate/wave/test-celery/.venv/lib/python3.14/site-packages/celery/app/trace.py", line 779, in __protected_call__
    return self.run(*args, **kwargs)
           ~~~~~~~~^^^^^^^^^^^^^^^^^
  File "/Users/nate/wave/test-celery/app.py", line 22, in wrapped
    return f(*args, **kwargs)
  File "/Users/nate/wave/test-celery/app.py", line 39, in delay_in_loop
    log.apply_async(args=(gevent.getcurrent().minimal_ident,) , queue='other')
    ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/nate/wave/test-celery/.venv/lib/python3.14/site-packages/celery/app/task.py", line 608, in apply_async
    return app.send_task(
           ~~~~~~~~~~~~~^
        self.name, args, kwargs, task_id=task_id, producer=producer,
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ...<2 lines>...
        **options
        ^^^^^^^^^
    )
    ^
  File "/Users/nate/wave/test-celery/.venv/lib/python3.14/site-packages/celery/app/base.py", line 943, in send_task
    with self.producer_or_acquire(producer) as P:
         ~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^
  File "/Users/nate/wave/test-celery/.venv/lib/python3.14/site-packages/celery/utils/objects.py", line 84, in __enter__
    context = self._context = self.fallback(
                              ~~~~~~~~~~~~~^
        *self.fb_args, **self.fb_kwargs
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ).__enter__()
    ^
  File "/Users/nate/wave/test-celery/.venv/lib/python3.14/site-packages/kombu/resource.py", line 73, in acquire
    R = self._resource.get(block=block, timeout=timeout)
  File "/Users/nate/.local/share/uv/python/cpython-3.14.0rc2-macos-aarch64-none/lib/python3.14/queue.py", line 194, in get
    with self.not_empty:
         ^^^^^^^^^^^^^^
  File "/Users/nate/.local/share/uv/python/cpython-3.14.0rc2-macos-aarch64-none/lib/python3.14/threading.py", line 317, in __exit__
    return self._lock.__exit__(*args)
           ~~~~~~~~~~~~~~~~~~~^^^^^^^
  File "src/gevent/_semaphore.py", line 285, in gevent._gevent_c_semaphore.Semaphore.__exit__
  File "src/gevent/_semaphore.py", line 286, in gevent._gevent_c_semaphore.Semaphore.__exit__
  File "src/gevent/_semaphore.py", line 500, in gevent._gevent_c_semaphore.BoundedSemaphore.release
RuntimeError: Semaphore released too many times
[2025-12-31 11:12:49,769: ERROR/MainProcess] Task app.delay_in_loop[0637afaf-3c8a-4953-ab4c-fd6bf7123ba5] raised unexpected: RuntimeError('Semaphore released too many times')
Traceback (most recent call last):
  File "/Users/nate/.local/share/uv/python/cpython-3.14.0rc2-macos-aarch64-none/lib/python3.14/queue.py", line 202, in get
    self.not_empty.wait()
    ~~~~~~~~~~~~~~~~~~~^^
  File "/Users/nate/.local/share/uv/python/cpython-3.14.0rc2-macos-aarch64-none/lib/python3.14/threading.py", line 378, in wait
    self._acquire_restore(saved_state)
    ~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^
  File "/Users/nate/.local/share/uv/python/cpython-3.14.0rc2-macos-aarch64-none/lib/python3.14/threading.py", line 326, in _acquire_restore
    self._lock.acquire()           # Ignore saved state
    ~~~~~~~~~~~~~~~~~~^^
  File "/Users/nate/wave/test-celery/.venv/lib/python3.14/site-packages/gevent/thread.py", line 302, in acquire
    acquired = BoundedSemaphore.acquire(self, blocking, timeout)
  File "src/gevent/_semaphore.py", line 184, in gevent._gevent_c_semaphore.Semaphore.acquire
  File "src/gevent/_semaphore.py", line 253, in gevent._gevent_c_semaphore.Semaphore.acquire
  File "src/gevent/_abstract_linkable.py", line 529, in gevent._gevent_c_abstract_linkable.AbstractLinkable._wait
  File "src/gevent/_abstract_linkable.py", line 495, in gevent._gevent_c_abstract_linkable.AbstractLinkable._wait_core
  File "src/gevent/_abstract_linkable.py", line 504, in gevent._gevent_c_abstract_linkable.AbstractLinkable._wait_core
  File "src/gevent/_abstract_linkable.py", line 498, in gevent._gevent_c_abstract_linkable.AbstractLinkable._wait_core
  File "src/gevent/_abstract_linkable.py", line 450, in gevent._gevent_c_abstract_linkable.AbstractLinkable._AbstractLinkable__wait_to_be_notified
  File "src/gevent/_abstract_linkable.py", line 459, in gevent._gevent_c_abstract_linkable.AbstractLinkable._switch_to_hub
  File "src/gevent/_greenlet_primitives.py", line 61, in gevent._gevent_c_greenlet_primitives.SwitchOutGreenletWithLoop.switch
  File "src/gevent/_greenlet_primitives.py", line 65, in gevent._gevent_c_greenlet_primitives.SwitchOutGreenletWithLoop.switch
  File "src/gevent/_gevent_c_greenlet_primitives.pxd", line 35, in gevent._gevent_c_greenlet_primitives._greenlet_switch
gevent.timeout.Timeout: 1 second

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/Users/nate/wave/test-celery/.venv/lib/python3.14/site-packages/celery/app/trace.py", line 479, in trace_task
    R = retval = fun(*args, **kwargs)
                 ~~~^^^^^^^^^^^^^^^^^
  File "/Users/nate/wave/test-celery/.venv/lib/python3.14/site-packages/celery/app/trace.py", line 779, in __protected_call__
    return self.run(*args, **kwargs)
           ~~~~~~~~^^^^^^^^^^^^^^^^^
  File "/Users/nate/wave/test-celery/app.py", line 22, in wrapped
    return f(*args, **kwargs)
  File "/Users/nate/wave/test-celery/app.py", line 39, in delay_in_loop
    log.apply_async(args=(gevent.getcurrent().minimal_ident,) , queue='other')
    ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/nate/wave/test-celery/.venv/lib/python3.14/site-packages/celery/app/task.py", line 608, in apply_async
    return app.send_task(
           ~~~~~~~~~~~~~^
        self.name, args, kwargs, task_id=task_id, producer=producer,
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ...<2 lines>...
        **options
        ^^^^^^^^^
    )
    ^
  File "/Users/nate/wave/test-celery/.venv/lib/python3.14/site-packages/celery/app/base.py", line 943, in send_task
    with self.producer_or_acquire(producer) as P:
         ~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^
  File "/Users/nate/wave/test-celery/.venv/lib/python3.14/site-packages/celery/utils/objects.py", line 84, in __enter__
    context = self._context = self.fallback(
                              ~~~~~~~~~~~~~^
        *self.fb_args, **self.fb_kwargs
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ).__enter__()
    ^
  File "/Users/nate/wave/test-celery/.venv/lib/python3.14/site-packages/kombu/resource.py", line 73, in acquire
    R = self._resource.get(block=block, timeout=timeout)
  File "/Users/nate/.local/share/uv/python/cpython-3.14.0rc2-macos-aarch64-none/lib/python3.14/queue.py", line 194, in get
    with self.not_empty:
         ^^^^^^^^^^^^^^
  File "/Users/nate/.local/share/uv/python/cpython-3.14.0rc2-macos-aarch64-none/lib/python3.14/threading.py", line 317, in __exit__
    return self._lock.__exit__(*args)
           ~~~~~~~~~~~~~~~~~~~^^^^^^^
  File "src/gevent/_semaphore.py", line 285, in gevent._gevent_c_semaphore.Semaphore.__exit__
  File "src/gevent/_semaphore.py", line 286, in gevent._gevent_c_semaphore.Semaphore.__exit__
  File "src/gevent/_semaphore.py", line 500, in gevent._gevent_c_semaphore.BoundedSemaphore.release
RuntimeError: Semaphore released too many times
[2025-12-31 11:12:49,772: ERROR/MainProcess] Task app.delay_in_loop[e3819d2b-54c8-4818-9760-44117743ebd5] raised unexpected: RuntimeError('Semaphore released too many times')
Traceback (most recent call last):
  File "/Users/nate/.local/share/uv/python/cpython-3.14.0rc2-macos-aarch64-none/lib/python3.14/queue.py", line 202, in get
    self.not_empty.wait()
    ~~~~~~~~~~~~~~~~~~~^^
  File "/Users/nate/.local/share/uv/python/cpython-3.14.0rc2-macos-aarch64-none/lib/python3.14/threading.py", line 378, in wait
    self._acquire_restore(saved_state)
    ~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^
  File "/Users/nate/.local/share/uv/python/cpython-3.14.0rc2-macos-aarch64-none/lib/python3.14/threading.py", line 326, in _acquire_restore
    self._lock.acquire()           # Ignore saved state
    ~~~~~~~~~~~~~~~~~~^^
  File "/Users/nate/wave/test-celery/.venv/lib/python3.14/site-packages/gevent/thread.py", line 302, in acquire
    acquired = BoundedSemaphore.acquire(self, blocking, timeout)
  File "src/gevent/_semaphore.py", line 184, in gevent._gevent_c_semaphore.Semaphore.acquire
  File "src/gevent/_semaphore.py", line 253, in gevent._gevent_c_semaphore.Semaphore.acquire
  File "src/gevent/_abstract_linkable.py", line 529, in gevent._gevent_c_abstract_linkable.AbstractLinkable._wait
  File "src/gevent/_abstract_linkable.py", line 495, in gevent._gevent_c_abstract_linkable.AbstractLinkable._wait_core
  File "src/gevent/_abstract_linkable.py", line 504, in gevent._gevent_c_abstract_linkable.AbstractLinkable._wait_core
  File "src/gevent/_abstract_linkable.py", line 498, in gevent._gevent_c_abstract_linkable.AbstractLinkable._wait_core
  File "src/gevent/_abstract_linkable.py", line 450, in gevent._gevent_c_abstract_linkable.AbstractLinkable._AbstractLinkable__wait_to_be_notified
  File "src/gevent/_abstract_linkable.py", line 459, in gevent._gevent_c_abstract_linkable.AbstractLinkable._switch_to_hub
  File "src/gevent/_greenlet_primitives.py", line 61, in gevent._gevent_c_greenlet_primitives.SwitchOutGreenletWithLoop.switch
  File "src/gevent/_greenlet_primitives.py", line 65, in gevent._gevent_c_greenlet_primitives.SwitchOutGreenletWithLoop.switch
  File "src/gevent/_gevent_c_greenlet_primitives.pxd", line 35, in gevent._gevent_c_greenlet_primitives._greenlet_switch
gevent.timeout.Timeout: 1 second

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/Users/nate/wave/test-celery/.venv/lib/python3.14/site-packages/celery/app/trace.py", line 479, in trace_task
    R = retval = fun(*args, **kwargs)
                 ~~~^^^^^^^^^^^^^^^^^
  File "/Users/nate/wave/test-celery/.venv/lib/python3.14/site-packages/celery/app/trace.py", line 779, in __protected_call__
    return self.run(*args, **kwargs)
           ~~~~~~~~^^^^^^^^^^^^^^^^^
  File "/Users/nate/wave/test-celery/app.py", line 22, in wrapped
    return f(*args, **kwargs)
  File "/Users/nate/wave/test-celery/app.py", line 39, in delay_in_loop
    log.apply_async(args=(gevent.getcurrent().minimal_ident,) , queue='other')
    ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/nate/wave/test-celery/.venv/lib/python3.14/site-packages/celery/app/task.py", line 608, in apply_async
    return app.send_task(
           ~~~~~~~~~~~~~^
        self.name, args, kwargs, task_id=task_id, producer=producer,
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ...<2 lines>...
        **options
        ^^^^^^^^^
    )
    ^
  File "/Users/nate/wave/test-celery/.venv/lib/python3.14/site-packages/celery/app/base.py", line 943, in send_task
    with self.producer_or_acquire(producer) as P:
         ~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^
  File "/Users/nate/wave/test-celery/.venv/lib/python3.14/site-packages/celery/utils/objects.py", line 84, in __enter__
    context = self._context = self.fallback(
                              ~~~~~~~~~~~~~^
        *self.fb_args, **self.fb_kwargs
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ).__enter__()
    ^
  File "/Users/nate/wave/test-celery/.venv/lib/python3.14/site-packages/kombu/resource.py", line 73, in acquire
    R = self._resource.get(block=block, timeout=timeout)
  File "/Users/nate/.local/share/uv/python/cpython-3.14.0rc2-macos-aarch64-none/lib/python3.14/queue.py", line 194, in get
    with self.not_empty:
         ^^^^^^^^^^^^^^
  File "/Users/nate/.local/share/uv/python/cpython-3.14.0rc2-macos-aarch64-none/lib/python3.14/threading.py", line 317, in __exit__
    return self._lock.__exit__(*args)
           ~~~~~~~~~~~~~~~~~~~^^^^^^^
  File "src/gevent/_semaphore.py", line 285, in gevent._gevent_c_semaphore.Semaphore.__exit__
  File "src/gevent/_semaphore.py", line 286, in gevent._gevent_c_semaphore.Semaphore.__exit__
  File "src/gevent/_semaphore.py", line 500, in gevent._gevent_c_semaphore.BoundedSemaphore.release
RuntimeError: Semaphore released too many times
[2025-12-31 11:12:49,773: ERROR/MainProcess] [14] Timed Out
"""
