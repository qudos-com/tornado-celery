from __future__ import absolute_import

import celery

from tornado import ioloop

from .connection import ConnectionPool
from .producer import NonBlockingTaskProducer
from .result import AsyncResult

__version__ = '99.3.12'
VERSION = tuple(int(s) for s in __version__.split('.'))


def setup_nonblocking_producer(celery_app=None, io_loop=None,
                               on_ready=None, result_cls=AsyncResult,
                               limit=1, producer_cls=NonBlockingTaskProducer,
                               callback=None):
    """Setup celery to use non blocking producer

    :param celery_app:
    :param io_loop:
    :param on_ready: alias for callback
    :param result_cls:
    :param limit:
    :param producer_cls:
    :param callback: Callback after connection is established
    :return:

    :type limit: int
    :type io_loop: tornado.ioloop.IOLoop
    :type on_ready: Function
    :type callback: Function
    """

    if on_ready is not None:
        assert callback is None, 'you may only supply either: on_ready or callback'
        callback = on_ready

    celery_app = celery_app or celery.current_app
    io_loop = io_loop or ioloop.IOLoop.instance()

    producer_cls.app = celery_app
    producer_cls.conn_pool = ConnectionPool(limit, io_loop)
    producer_cls.result_cls = result_cls
    if celery_app.conf['BROKER_URL'] and celery_app.conf['BROKER_URL'].startswith('amqp'):
        celery.app.amqp.AMQP.producer_cls = producer_cls

    def connect():
        broker_url = celery_app.connection().as_uri(include_password=True)
        options = celery_app.conf.get('CELERYT_PIKA_OPTIONS', {})
        producer_cls.conn_pool.connect(broker_url,
                                       options=options,
                                       callback=callback,
                                       confirm_delivery=_get_confirm_publish_conf(celery_app.conf))

    io_loop.add_callback(connect)

def _get_confirm_publish_conf(conf):
    broker_transport_options = conf.get('BROKER_TRANSPORT_OPTIONS', {})
    if (broker_transport_options and
        broker_transport_options.get('confirm_publish') is True):
        return True
    return False
