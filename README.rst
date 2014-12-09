Celery integration with Tornado
===============================

.. image:: https://badge.fury.io/py/tornado-celery.png
        :target: http://badge.fury.io/py/tornado-celery
.. image:: https://pypip.in/d/tornado-celery/badge.png
        :target: https://crate.io/packages/tornado-celery/

tornado-celery is a non-blocking Celery client for Tornado web framework

Usage
-----

Calling Celery tasks(has return value) from Tornado RequestHandler: ::

    from tornado import gen, web
    import tcelery, tasks

    tcelery.setup_nonblocking_producer()

    class AsyncHandler(web.RequestHandler):
        @web.asynchronous
        def get(self):
            tasks.echo.apply_async(args=['Hello world!'], callback=self.on_async_result)

        def on_async_result(self, async_result):
            async_result.get(callback=self.on_actual_result)
        
        def on_actual_result(self, result):
            self.write(str(result))
            self.finish()

with generator-based interface: ::

    class GenAsyncHandler(web.RequestHandler):
        @web.asynchronous
        @gen.coroutine
        def get(self):
            async_result = yield gen.Task(tasks.sleep.apply_async, args=[3])
            result = yield gen.Task(async_result.get)
            self.write(str(result))
            self.finish()

Calling Celery tasks(no return value) from Tornado RequestHandler: ::

    @web.asynchronous
    def get(self):
        tasks.echo.apply_async(args=['Hello world!'], callback=self.on_async_result)

    def on_async_result(self, async_result):
        self.write("task sent") # ack-ed if BROKER_TRANSPORT_OPTIONS: {'confirm_publish': True}
        self.finish()

with generator-based interface: ::

    @web.asynchronous
    @gen.coroutine
    def get(self):
        yield gen.Task(tasks.sleep.apply_async, args=[3])
        self.write("task sent") # ack-ed if BROKER_TRANSPORT_OPTIONS: {'confirm_publish': True}
        self.finish()

**NOTE:** Currently callbacks only work with AMQP and Redis backends.
To use the Redis backend, you must install `tornado-redis
<https://github.com/leporo/tornado-redis>`_.

tornado-celery can be launched as a web server: ::

    $ cd tornado-celery
    $ python -m tcelery --port=8888 --app=examples.tasks --address=0.0.0.0

Execute a task asynchronously: ::

    $ curl -X POST -d '{"args":["hello"]}' http://localhost:8888/apply-async/examples.tasks.echo/
    {"task-id": "a24c9e38-4976-426a-83d6-6b10b4de7ab1", "state": "PENDING"}

Get the result: ::

    $ curl http://localhost:8888/tasks/result/a24c9e38-4976-426a-83d6-6b10b4de7ab1/
    {"task-id": "a24c9e38-4976-426a-83d6-6b10b4de7ab1", "state": "SUCCESS", "result": "hello"}

Execute a task and get the result: ::

    $ curl -X POST -d '{"args":[1,2]}' http://localhost:8888/apply/examples.tasks.add/
    {"task-id": "fe3cc5a5-d11b-4b17-a6e2-e7fd2fba7ec6", "state": "SUCCESS", "result": 3}

Execute a task with timeout: ::

    $ curl -X POST -d '{"args":[5],"timeout":1}' http://localhost:8888/apply/examples.tasks.sleep/
    {"task-id": "9ca78e26-bbb2-404c-b3bb-bc1c63cbdf41", "state": "REVOKED"}

Installation
------------

To install, simply: ::

    $ pip install tornado-celery

Documentation
-------------

Documentation is available at `Read the Docs`_

.. _Read the Docs: http://tornado-celery.readthedocs.org


Running the Tests
-----------------

To run the tests for the AMQP backend: ::

    $ python examples/tasks.py worker
    $ cd examples && python -m tcelery -A tasks
    $ python tests/functests.py

To run the tests for the Redis backend, first make sure redis is running, then: ::

    $ CELERY_RESULT_BACKEND=redis:// python examples/tasks.py worker
    $ cd examples && CELERY_RESULT_BACKEND=redis:// python -m tcelery -A tasks
    $ python tests/functests.py
