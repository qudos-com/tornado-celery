import logging
from datetime import timedelta
from functools import partial

from tornado import gen
from tornadoredis import Client
from tornadoredis.exceptions import ResponseError
from tornadoredis.pubsub import BaseSubscriber

logger = logging.getLogger(__name__)


class CelerySubscriber(BaseSubscriber):
    def unsubscribe_channel(self, channel_name, callback=None):
        """Unsubscribes the redis client from the channel"""
        del self.subscribers[channel_name]
        del self.subscriber_count[channel_name]
        self.redis.unsubscribe(channel_name, callback)

    def on_message(self, msg):
        if not msg:
            return
        try:
            if msg.kind == 'message' and msg.body:
                # Get the list of subscribers for this channel
                for subscriber in self.subscribers[msg.channel].keys():
                    subscriber(msg.body)
        finally:
            super(CelerySubscriber, self).on_message(msg)


class RedisClient(Client):
    @gen.engine
    def _consume_bulk(self, tail, callback=None):
        response = yield gen.Task(self.connection.read, int(tail) + 2)
        if isinstance(response, Exception):
            raise response
        if not response:
            raise ResponseError('EmptyResponse')
        else:
            # We don't cast try to convert to unicode here as the response
            # may not be utf-8 encoded, for example if using msgpack as a
            # serializer
            # response = to_unicode(response)
            response = response[:-2]
        callback(response)


class RedisConsumer(object):
    def __init__(self, producer):
        self.producer = producer
        backend = producer.app.backend
        self.client = RedisClient(host=backend.connparams['host'],
                                  port=backend.connparams['port'],
                                  password=backend.connparams['password'],
                                  selected_db=backend.connparams['db'],
                                  io_loop=producer.conn_pool.io_loop)
        self.client.connect()
        self.subscriber = CelerySubscriber(self.client)

    def wait_for(self, task_id, callback, expires=None, persistent=None):
        key = self.producer.app.backend.get_key_for_task(task_id)
        if expires:
            timeout = self.producer.conn_pool.io_loop.add_timeout(
                timedelta(milliseconds=expires), self.on_timeout, key)
        else:
            timeout = None
        logger.info("Subscribing to key %s (%s) with timeout %s",
                     task_id, key, timeout)
        self.subscriber.subscribe(
            key, partial(self.on_result, key, callback, timeout))

    def on_result(self, key, callback, timeout, result):
        if timeout:
            self.producer.conn_pool.io_loop.remove_timeout(timeout)
        logger.info("Got result for key %s, unsubscribing...", key)

        self.subscriber.unsubscribe_channel(
            key, partial(self.on_unsubscribed, key, callback, result))

    def on_unsubscribed(self, key, callback, result):
        logger.info("Unsubscribed from %s", key)
        callback(result)

    def on_timeout(self, key):
        logger.info("Task timed out on key %s")
        self.subscriber.unsubscribe_channel(key)
