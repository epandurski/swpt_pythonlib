"""
Adds to Flask-SQLAlchemy the capability to atomically send
messages (signals) over a message bus.
"""

import time
import logging
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import inspect
from .utils import retry_on_deadlock

__all__ = ['SignalBus', 'SignalBusMixin']


def _get_class_registry(base):
    return base.registry._class_registry if hasattr(base, 'registry') else base._decl_class_registry


def _raise_error_if_not_signal_model(model):
    if not hasattr(model, 'send_signalbus_message'):
        raise RuntimeError(
            '{} can not be flushed because it does not have a'
            ' "send_signalbus_message" method.'
        )


class SignalBus:
    """Instances of this class send signal messages that have been recorded
    in the SQL database, over a message bus. The sending of the recorded
    messages should be triggered explicitly by a function call.

    """

    def __init__(self, db: SQLAlchemy):
        self.db = db
        self.signal_session = self.db.session
        self.logger = logging.getLogger(__name__)
        retry = retry_on_deadlock(self.signal_session, retries=11, max_wait=1.0)
        self._flushmany_signals_with_retry = retry(self._flushmany_signals)

    def get_signal_models(self):
        """Return all signal types in a list.

        :rtype: list(`signal-model`)

        """

        base = self.db.Model
        return [
            cls for cls in _get_class_registry(base).values() if (
                isinstance(cls, type)
                and issubclass(cls, base)
                and hasattr(cls, 'send_signalbus_message')
            )
        ]

    def flushmany(self, models=None):
        """Send pending signals over the message bus.

        This method assumes that the number of pending signals might
        be huge. Using `SignalBus.flushmany` when auto-flushing is
        enabled for the given signal types is not recommended, because
        it may result in multiple delivery of messages.

        `SignalBus.flushmany` can be very useful when recovering from
        long periods of disconnectedness from the message bus, or when
        auto-flushing is disabled. If your database (and its
        SQLAlchemy dialect) supports ``FOR UPDATE SKIP LOCKED``,
        multiple processes will be able to run this method in
        parallel, without stepping on each others' toes.

        :param models: If passed, flushes only signals of the specified types.
        :type models: list(`signal-model`) or `None`
        :return: The total number of signals that have been sent

        """

        return self._flush_models(flush_fn=self._flushmany_signals_with_retry, models=models)

    def _init_app(self, app):
        from . import signalbus_cli

        if app.extensions.get('signalbus') not in [None, self]:
            raise RuntimeError(
                "A 'SignalBus' instance has already been registered on this Flask app."
                " Import and use that instance instead."
            )
        app.extensions['signalbus'] = self
        app.cli.add_command(signalbus_cli.signalbus)

        @app.teardown_appcontext
        def shutdown_signal_session(response_or_exc):
            self.signal_session.remove()
            return response_or_exc

    def _compose_signal_query(self, model, max_count=None):
        m = inspect(model)
        pk_attrs = [m.get_property_by_column(c).class_attribute for c in m.primary_key]
        query = self.signal_session.query(model)
        if max_count is not None:
            query = query.limit(max_count)
        return query, pk_attrs

    def _get_signal_burst_count(self, model):
        burst_count = int(getattr(model, 'signalbus_burst_count', 1))
        assert burst_count > 0, '"signalbus_burst_count" must be positive'
        return burst_count

    def _send_and_delete_signal_instances(self, model, instances):
        n = len(instances)
        if n > 1 and hasattr(model, 'send_signalbus_messages'):
            model.send_signalbus_messages(instances)
        else:
            for instance in instances:
                instance.send_signalbus_message()
        for instance in instances:
            self.signal_session.delete(instance)
        return n

    def _flush_models(self, flush_fn, models):
        sent_count = 0
        try:
            models_to_flush = self.get_signal_models() if models is None else models
            for model in models_to_flush:
                _raise_error_if_not_signal_model(model)
                sent_count += flush_fn(model)
        finally:
            self.signal_session.remove()
        return sent_count

    def _flushmany_signals(self, model):
        self.logger.info('Flushing %s in "flushmany" mode.', model.__name__)
        sent_count = 0
        burst_count = self._get_signal_burst_count(model)
        query, _ = self._compose_signal_query(model, max_count=burst_count)
        query = query.with_for_update(skip_locked=True)
        while True:
            signals = query.all()
            sent_count += self._send_and_delete_signal_instances(model, signals)
            self.signal_session.commit()
            self.signal_session.expire_all()
            if len(signals) < burst_count:
                break
        return sent_count


class SignalBusMixin:
    """A **mixin class** that can be used to extend
    :class:`~flask_sqlalchemy.SQLAlchemy` to send signals.

    For example::

        from flask import Flask
        from flask_sqlalchemy import SQLAlchemy
        from swpt_pythonlib.flask_signalbus import SignalBusMixin

        class CustomSQLAlchemy(SignalBusMixin, SQLAlchemy):
            pass

        app = Flask(__name__)
        db = CustomSQLAlchemy(app)
        db.signalbus.flush()

    """
    signalbus: SignalBus

    def init_app(self, app: Flask) -> None:
        super().init_app(app)
        self.signalbus = SignalBus(self)
        self.signalbus._init_app(app)
