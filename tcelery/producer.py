from __future__ import absolute_import

import sys
import logging

from datetime import timedelta
from kombu import serialization
from kombu.utils import cached_property
from kombu import common

from celery.app.amqp import TaskProducer
from celery.backends.amqp import AMQPBackend
from celery.backends.redis import RedisBackend
from celery.backends.base import DisabledBackend
from celery.utils import timeutils

from .result import AsyncResult


try:
    from .redis import RedisConsumer
except ImportError:
    RedisConsumer = None

is_py3k = sys.version_info >= (3, 0)

logger = logging.getLogger(__name__)

class AMQPConsumer(object):
    def __init__(self, producer):
        self.producer = producer

    def wait_for(self, task_id, callback, expires=None, persistent=None):
        if persistent is None:
            persistent = True
        conn = self.producer.conn_pool.connection()

        def consume_callback(channel, deliver, properties, reply):
            callback(reply)
            try:
                channel.basic_cancel(consumer_tag=deliver.consumer_tag, nowait=True)
            except Exception:
                pass

        conn.consume(task_id.replace('-', ''),
                     consume_callback,
                     x_expires=expires, persistent=persistent)


class NonBlockingTaskProducer(TaskProducer):

    conn_pool = None
    app = None
    result_cls = AsyncResult

    def __init__(self, channel=None, *args, **kwargs):
        super(NonBlockingTaskProducer, self).__init__(
            channel, *args, **kwargs)

    def publish(self, body, routing_key=None, delivery_mode=None,
                mandatory=False, immediate=False, priority=0,
                content_type=None, content_encoding=None, serializer=None,
                headers=None, compression=None, exchange=None, retry=False,
                retry_policy=None, declare=[], **properties):
        headers = {} if headers is None else headers
        retry_policy = {} if retry_policy is None else retry_policy
        routing_key = self.routing_key if routing_key is None else routing_key
        compression = self.compression if compression is None else compression
        exchange = exchange or self.exchange

        logger.info('Producer publish')

        callback = properties.pop('callback', None)
        task_id = body['id']

        if callback and not callable(callback):
            raise ValueError('callback should be callable')

        body, content_type, content_encoding = self._prepare(
            body, serializer, content_type, content_encoding,
            compression, headers)

        self.serializer = self.app.backend.serializer

        serialization.registry.enable(serializer)

        (self.content_type,
         self.content_encoding,
         self.encoder) = serialization.registry._encoders[self.serializer]

        conn = self.conn_pool.connection()

        # auto create/bind exchange/queue for the first call and caches
        for entity in declare:
            common.maybe_declare(entity, self.channel, retry=True)

        publish = conn.publish
        result = publish(body, priority=priority, content_type=content_type,
                         content_encoding=content_encoding, headers=headers,
                         properties=properties, routing_key=routing_key,
                         mandatory=mandatory, immediate=immediate,
                         exchange=exchange, declare=declare)

        if callback:
            async_result = self.result_cls(task_id=task_id,
                                           result=result,
                                           producer=self)
            if conn.confirm_delivery:
                conn.confirm_delivery_handler.add_callback(lambda result:
                                                           callback(async_result))

            else:
                logger.info("Run callback passed from send_task")
                callback(async_result)

        return result

    @cached_property
    def consumer(self):
        Consumer = {
            AMQPBackend: AMQPConsumer,
            RedisBackend: RedisConsumer
        }[type(self.app.backend)]
        if not Consumer:
            raise RuntimeError(
                "tornado-redis must be installed to use the redis backend")
        return Consumer(self)

    def decode(self, payload):
        payload = is_py3k and payload or str(payload)
        return serialization.decode(payload,
                                    content_type=self.content_type,
                                    content_encoding=self.content_encoding)

    def prepare_expires(self, value=None, type=None):
        if value is None:
            value = self.app.conf.CELERY_TASK_RESULT_EXPIRES
        if isinstance(value, timedelta):
            value = timeutils.timedelta_seconds(value)
        if value is not None and type:
            return type(value * 1000)
        return value

    def fail_if_backend_not_supported(self):
        if not isinstance(self.app.backend,
                          (AMQPBackend, RedisBackend, DisabledBackend)):
            raise NotImplementedError(
                'result retrieval can be used only with AMQP or Redis backends')

    def __repr__(self):
        return '<NonBlockingTaskProducer: {0.channel}>'.format(self)
