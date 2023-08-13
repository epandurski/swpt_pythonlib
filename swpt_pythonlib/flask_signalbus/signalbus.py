import logging
import flask_sqlalchemy as fsa
import sqlalchemy.orm as sa_orm
from typing import Iterable, Callable, Optional
from flask_sqlalchemy.model import Model
from flask import Flask
from .utils import retry_on_deadlock

__all__ = ['SignalBus', 'SignalBusMixin']


def _get_class_registry(base: type[Model]) -> dict[str, type]:
    return (
        base.registry._class_registry
        if hasattr(base, 'registry')
        else base._decl_class_registry  # type: ignore
    )


def _raise_error_if_not_signal_model(model_cls: type[Model]) -> None:
    if not hasattr(model_cls, 'send_signalbus_message'):
        raise RuntimeError(
            '{} can not be flushed because it does not have a'
            ' "send_signalbus_message" method.'
        )


class SignalBus:
    """Instances of this class send signal messages that have been recorded
    in the SQL database, over a message bus. The sending of the recorded
    messages should be triggered explicitly by a function call.
    """

    def __init__(self, db: fsa.SQLAlchemy):
        self.db = db
        retry = retry_on_deadlock(db.session, retries=11, max_wait=1.0)
        self._flushmany_signals_with_retry = retry(self._flushmany_signals)

    def get_signal_models(self) -> list[type[Model]]:
        """Return all signal types in a list."""

        base = self.db.Model
        return [
            cls for cls in _get_class_registry(base).values() if (
                isinstance(cls, type)
                and issubclass(cls, base)
                and hasattr(cls, 'send_signalbus_message')
            )
        ]

    def flushmany(self, models: Optional[Iterable[type[Model]]] = None) -> int:
        """Send pending signals over the message bus.

        If your database (and its SQLAlchemy dialect) supports ``FOR UPDATE
        SKIP LOCKED``, multiple processes will be able to run this method in
        parallel, without stepping on each others' toes.

        :param models: If passed, flushes only signals of the specified types.
        :return: The total number of signals that have been sent
        """

        return self._flush_models(
            flush_fn=self._flushmany_signals_with_retry, models=models)

    def _init_app(self, app: Flask) -> None:
        from . import signalbus_cli

        if 'signalbus' in app.extensions:  # pragma: no cover
            raise RuntimeError(
                "A 'SignalBus' instance has already been registered on this"
                " Flask app. Import and use that instance instead."
            )
        app.extensions['signalbus'] = self
        app.cli.add_command(signalbus_cli.signalbus)

    def _compose_signal_query(
            self,
            model_cls: type[Model],
            max_count: int,
    ) -> sa_orm.Query:
        query = self.db.session.query(model_cls)
        query = query.limit(max_count)
        return query

    def _get_signal_burst_count(self, model_cls: type[Model]) -> int:
        burst_count = int(getattr(model_cls, 'signalbus_burst_count', 1))
        assert burst_count > 0, '"signalbus_burst_count" must be positive'
        return burst_count

    def _send_and_delete_instances(
            self, model_cls: type[Model], instances: list[Model]):
        n = len(instances)
        if n > 1 and hasattr(model_cls, 'send_signalbus_messages'):
            model_cls.send_signalbus_messages(instances)
        else:
            for instance in instances:
                assert hasattr(instance, 'send_signalbus_message')
                instance.send_signalbus_message()

        session = self.db.session
        for instance in instances:
            session.delete(instance)

        return n

    def _flush_models(
            self,
            flush_fn: Callable[[type[Model]], int],
            models: Optional[Iterable[type[Model]]],
    ):
        sent_count = 0
        try:
            to_flush = self.get_signal_models() if models is None else models
            for model in to_flush:
                _raise_error_if_not_signal_model(model)
                sent_count += flush_fn(model)
        finally:
            self.db.session.remove()
        return sent_count

    def _flushmany_signals(self, model_cls: type[Model]) -> int:
        logger = logging.getLogger(__name__)
        logger.info('Flushing %s.', model_cls.__name__)
        sent_count = 0
        burst_count = self._get_signal_burst_count(model_cls)
        query = self._compose_signal_query(model_cls, max_count=burst_count)
        query = query.with_for_update(skip_locked=True)
        while True:
            signals = query.all()
            sent_count += self._send_and_delete_instances(model_cls, signals)
            self.db.session.commit()
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
        assert isinstance(self, fsa.SQLAlchemy), \
            'SignalBusMixin must be used as mixin for a SQLAlchemy superclass.'

        super().init_app(app)  # type: ignore
        self.signalbus = SignalBus(self)
        self.signalbus._init_app(app)
