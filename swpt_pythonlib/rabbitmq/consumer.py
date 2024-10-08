import logging
import threading
import queue
import time
import pika
import pika.exceptions
from typing import Optional, Any
from functools import partial
from flask import Flask
from .common import MessageProperties

_LOGGER = logging.getLogger(__name__)


class _WorkerThread(threading.Thread):
    def __init__(self, consumer):
        super().__init__()
        self.consumer = consumer
        self.add_callback = consumer._connection.add_callback_threadsafe
        self.process_message = consumer.process_message
        self.is_running = False

    def stop(self):
        _LOGGER.info("Stopping worker thread %s", self.name)
        self.is_running = False

    def run(self):
        _LOGGER.info("Starting worker thread %s", self.name)
        self.is_running = True
        app = self.consumer.app
        work_queue = self.consumer._work_queue
        with app.app_context():
            while self.is_running:
                try:
                    args = work_queue.get(timeout=1.0)
                except queue.Empty:
                    continue
                self._process_message(*args)
                work_queue.task_done()

    def _process_message(self, channel, method, properties, body):
        delivery_tag = method.delivery_tag
        try:
            _LOGGER.debug("Processing message %i", delivery_tag)
            ok = self.process_message(body, properties)
        except Exception as e:
            self.consumer._on_error(self.name, e)
            self.stop()
            return

        if ok:
            _LOGGER.debug("Acknowledging message %i", delivery_tag)
            self.add_callback(
                partial(channel.basic_ack, delivery_tag=delivery_tag)
            )
        else:
            _LOGGER.debug("Rejecting message %i", delivery_tag)
            self.add_callback(
                partial(
                    channel.basic_reject,
                    delivery_tag=delivery_tag,
                    requeue=False,
                )
            )


class TerminatedConsumtion(Exception):
    """The consumption has been terminated."""


class Consumer:
    """A RabbitMQ message consumer

    :param app: Optional Flask app object. If not provided `init_app`
      must be called later, providing the Flask app object.

    :param config_prefix: A prefix for the Flask configuration
      settings for this consumer instance.

    :param url: RabbitMQ’s connection URL. If not passed, the value of
      the ``{config_prefix}_URL`` Flask configuration setting will be
      used.

    :param queue: The name of the RabbitMQ queue to consume from. If
      not passed, the value of the ``{config_prefix}_QUEUE`` Flask
      configuration setting will be used.

    :param threads: The number of worker threads in the pool. If not
      passed, the value of the ``{config_prefix}_THREADS`` Flask
      configuration setting will be used (the default is 1).

    :param prefetch_size: Specifies the prefetch window size. RabbitMQ
      will send a message in advance if it is equal to or smaller in
      size than the available prefetch size (and also falls into other
      prefetch limits). If not passed, the value of the
      ``{config_prefix}_PREFETCH_SIZE`` Flask configuration setting
      will be used (the default is 0, meaning "no specific limit").

    :param prefetch_count: Specifies a prefetch window in terms of
      whole messages. This field may be used in combination with the
      prefetch_size field. A message will only be sent in advance if
      both prefetch windows allow it. Setting a bigger value may give
      a performance improvement. If not passed, the value of the
      ``{config_prefix}_PREFETCH_COUNT`` Flask configuration setting
      will be used (the default is 1).

    :param draining_mode: If set to `True` (the default is `False`),
      when the queue has become empty, the consumption will stop for a
      while. This allows the emptied queue to be deleted safely. (In
      order to guarantee that no massages will be lost when deleting
      the queue, at the moment of the deletion there must be no
      remaining consumers.)

    The received messages will be processed by a pool of worker
    threads, created by the consumer instance, after the `start`
    method is called. Each consumer instance maintains a separate
    RabbitMQ connection. If the connection has been closed for some
    reason, the `start` method will throw an exception. To continue
    consuming, the `start` method can be called again.

    This class is meant to be subclassed. For example::

        from swpt_pythonlib import rabbitmq

        class ExampleConsumer(rabbitmq.Consumer):
            def process_message(self, body, properties):
                if len(body) == 0:
                    return False  # Malformed (empty) message

                # Process the  message here.

                return True  # Successfully processed
    """

    def __init__(
        self,
        app: Optional[Flask] = None,
        *,
        config_prefix: str = "SIGNALBUS_RABBITMQ",
        url: Optional[str] = None,
        queue: Optional[str] = None,
        threads: Optional[int] = None,
        prefetch_size: Optional[int] = None,
        prefetch_count: Optional[int] = None,
        draining_mode: bool = False,
    ):
        self.config_prefix = config_prefix
        self.url = url
        self.queue = queue
        self.threads = threads
        self.prefetch_size = prefetch_size
        self.prefetch_count = prefetch_count
        self.draining_mode = draining_mode
        self._draining_pause_seconds = 15.0
        self._stopped = True
        self._work_queue: Optional[queue.Queue] = None
        self._connection: Optional[pika.BlockingConnection] = None
        if app is not None:
            self.init_app(app)

    def init_app(self, app: Flask):
        """Bind the instance to a Flask app object.

        :param app: A Flask app object
        """
        config = app.config
        prefix = self.config_prefix
        self.app = app

        if self.url is None:
            self.url = config[f"{prefix}_URL"]
        if self.queue is None:
            self.queue = config[f"{prefix}_QUEUE"]
        if self.threads is None:
            self.threads = config.get(f"{prefix}_THREADS", 1)
        if self.prefetch_size is None:
            self.prefetch_size = config.get(f"{prefix}_PREFETCH_SIZE", 0)
        if self.prefetch_count is None:
            self.prefetch_count = config.get(f"{prefix}_PREFETCH_COUNT", 1)

        self._purge()

    def start(self) -> None:
        """Opens a RabbitMQ connection and starts processing messages until
        one of the following things happen:

        * The connection has been lost.

        * An error has occurred during message processing.

        * The `stop` method has been called on the consumer instance.

        This method blocks and never returns normally. If one of the
        previous things happen an `TerminatedConsumtion` exception
        will be raised. Also, this method may raise
        `pika.exceptions.AMQPError` when, for some reason, a proper
        RabbitMQ connection can not be established.
        """
        assert isinstance(self.url, str)
        assert isinstance(self.queue, str)
        assert isinstance(self.threads, int)
        assert isinstance(self.prefetch_size, int)
        assert isinstance(self.prefetch_count, int)
        assert self.threads >= 1
        assert self.prefetch_size >= 0
        assert self.prefetch_count >= 0

        _LOGGER.info("Consumer started")
        self._stopped = False
        self._work_queue = queue.Queue(
            max(self.prefetch_count // 3, self.threads)
        )
        self._connection = pika.BlockingConnection(
            pika.URLParameters(self.url)
        )

        channel = self._connection.channel()
        channel.basic_qos(self.prefetch_size, self.prefetch_count)
        workers = [_WorkerThread(self) for _ in range(self.threads)]
        for worker in workers:
            worker.start()

        try:
            while not self._stopped:
                inactivity_counter = 0

                for method, properties, body in channel.consume(
                    self.queue, inactivity_timeout=1.0
                ):  # type: ignore
                    if method is not None:
                        inactivity_counter = 0
                        t = (channel, method, properties, body)
                        self._work_queue.put(t)
                    else:
                        inactivity_counter += 1
                        if self.draining_mode and (
                                inactivity_counter > self.inactivity_threshold
                        ):
                            channel.cancel()
                            self._make_draining_pause()

                    if self._stopped:
                        break
        except pika.exceptions.ChannelClosed:
            pass

        for worker in workers:
            worker.stop()

        for worker in workers:
            worker.join()

        channel.cancel()
        self._purge()
        raise TerminatedConsumtion()

    def process_message(
        self, body: bytes, properties: MessageProperties
    ) -> bool:
        """This method must be implemented by the sub-classes.

        :param body: message body
        :param properties: message properties

        The method should return `True` if the message has been
        successfully processed, and can be removed from the queue. If
        `False` is returned, this means that the message is malformed,
        and can not be processed. (Usually, malformed messages will be
        send to a "dead letter queue".)
        """

        raise NotImplementedError

    def stop(self, signum: Any = None, frame: Any = None) -> None:
        """Orders the consumer to stop.

        This is useful for properly handling process termination. For
        example::

            import signal

            consumer = Consumer(...)  # creates the instance

            signal.signal(signal.SIGINT, consumer.stop)
            signal.signal(signal.SIGTERM, consumer.stop)
            consumer.start()
        """
        _LOGGER.info("Consumer stopped")
        self._stopped = True

    def _on_error(self, thread_name, error):
        _LOGGER.error("Exception in thread %s:", thread_name, exc_info=error)
        self.stop()

    def _purge(self):
        self.stop()
        self._work_queue = None
        if self._connection is not None and self._connection.is_open:
            self._connection.close()

        self._connection = None

    @property
    def inactivity_threshold(self):
        return 1 + int(self._draining_pause_seconds / 20.0)

    def _make_draining_pause(self):
        # The duration of the pause is calculated so that, if
        # possible, all draining consumer instances will start at the
        # same time.
        epoch = time.time()
        dps = self._draining_pause_seconds
        pause_until = epoch + ((epoch + dps) % dps)

        while not self._stopped:
            remaining_seconds = pause_until - time.time()
            if remaining_seconds < 0.01:
                break
            self._connection.sleep(min(1.0, remaining_seconds))

        self._draining_pause_seconds = min(2.0 * dps, 3600.0)
