import logging
import flask_sqlalchemy as fsa
import sqlalchemy.orm as sa_orm
from sqlalchemy import select
from sqlalchemy.inspection import inspect
from sqlalchemy.sql.expression import tuple_, text
from typing import Iterable, Optional
from flask_sqlalchemy.model import Model
from flask import Flask
from .utils import retry_on_deadlock

__all__ = ["SignalBus", "SignalBusMixin", "get_models_to_flush"]


def _get_class_registry(base: type[Model]) -> dict[str, type]:
    return (
        base.registry._class_registry
        if hasattr(base, "registry")
        else base._decl_class_registry  # type: ignore
    )


def _raise_error_if_not_signal_model(model_cls: type[Model]) -> None:
    if not hasattr(model_cls, "send_signalbus_message"):
        raise RuntimeError(
            "{} can not be flushed because it does not have a"
            ' "send_signalbus_message" method.'
        )


class SignalBus:
    """Instances of this class send signal messages that have been recorded
    in the SQL database, over a message bus. The sending of the recorded
    messages should be triggered explicitly by a function call.
    """

    SET_SEQSCAN_ON = text("SET LOCAL enable_seqscan = on")
    SET_SEQSCAN_OFF = text("SET LOCAL enable_seqscan = off")
    SET_INDEXSCAN_ON = text("SET LOCAL enable_indexscan = on")
    SET_INDEXSCAN_OFF = text("SET LOCAL enable_indexscan = off")

    def __init__(self, db: fsa.SQLAlchemy):
        self.db = db
        retry = retry_on_deadlock(db.session, retries=11, max_wait=1.0)
        self._flushmany_signals_with_retry = retry(self._flushmany_signals)

    def get_signal_models(self) -> list[type[Model]]:
        """Return all signal types in a list."""

        base = self.db.Model
        return [
            cls
            for cls in _get_class_registry(base).values()
            if (
                isinstance(cls, type)
                and issubclass(cls, base)
                and hasattr(cls, "send_signalbus_message")
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

        sent_count = 0
        try:
            to_flush = self.get_signal_models() if models is None else models
            for model in to_flush:
                _raise_error_if_not_signal_model(model)
                sent_count += self._flushmany_signals_with_retry(model)
        finally:
            self.db.session.close()

        return sent_count

    def _init_app(self, app: Flask) -> None:
        from . import signalbus_cli

        if "signalbus" in app.extensions:  # pragma: no cover
            raise RuntimeError(
                "A 'SignalBus' instance has already been registered on this"
                " Flask app. Import and use that instance instead."
            )
        app.extensions["signalbus"] = self
        app.cli.add_command(signalbus_cli.signalbus)

    def _get_signal_burst_count(self, model_cls: type[Model]) -> int:
        burst_count = int(getattr(model_cls, "signalbus_burst_count", 1))
        assert burst_count > 0, '"signalbus_burst_count" must be positive'
        return burst_count

    def _send_and_delete(self, model_cls: type[Model], instances: list[Model]):
        n = len(instances)
        if n > 1 and hasattr(model_cls, "send_signalbus_messages"):
            model_cls.send_signalbus_messages(instances)
        else:
            for instance in instances:
                assert hasattr(instance, "send_signalbus_message")
                instance.send_signalbus_message()

        session = self.db.session
        for instance in instances:
            session.delete(instance)

        return n

    def _flushmany_signals(self, model_cls: type[Model]) -> int:
        logger = logging.getLogger(__name__)
        logger.info("Flushing %s.", model_cls.__name__)

        sent_count = 0
        burst_count = self._get_signal_burst_count(model_cls)
        mapper = inspect(model_cls)
        pk_attrs = [
            mapper.get_property_by_column(c).class_attribute
            for c in mapper.primary_key
        ]
        with self.db.engine.connect() as conn:
            conn.execute(self.SET_SEQSCAN_ON)
            with conn.execution_options(yield_per=burst_count).execute(
                select(*pk_attrs)
            ) as result:
                pk = tuple_(*pk_attrs)
                session = self.db.session
                q = session.query(model_cls).with_for_update(skip_locked=True)
                if hasattr(model_cls, "choose_rows"):
                    def _query_signals(pks):
                        chosen = model_cls.choose_rows([tuple(x) for x in pks])

                        # NOTE: By disabling sequential and index
                        # scans here, the planner is forced to use a
                        # bitmap scan. This can be a bit slower, but
                        # is the safe bet for generic plans. It
                        # prevents the use of a full sequential or
                        # index scan when, at the time of planning,
                        # the table is small.
                        session.execute(self.SET_SEQSCAN_OFF)
                        session.execute(self.SET_INDEXSCAN_OFF)
                        result = q.join(chosen, pk == tuple_(*chosen.c)).all()
                        session.execute(self.SET_INDEXSCAN_ON)
                        return result
                else:
                    def _query_signals(pks):
                        return q.filter(pk.in_(pks)).all()

                for primary_keys in result.partitions():
                    signals = _query_signals(primary_keys)
                    sent_count += self._send_and_delete(model_cls, signals)
                    session.commit()
                    session.expunge_all()

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
        db.signalbus.flushmany()
    """

    signalbus: SignalBus

    def init_app(self, app: Flask) -> None:
        assert isinstance(
            self, fsa.SQLAlchemy
        ), "SignalBusMixin must be used as mixin for a SQLAlchemy superclass."

        super().init_app(app)  # type: ignore
        self.signalbus = SignalBus(self)
        self.signalbus._init_app(app)


def get_models_to_flush(
    signalbus: SignalBus,
    model_names: list[str],
) -> list[type[Model]]:
    """Given model names, return model classes.

    Invalid model names are ignored with a warning. This function is useful
    for implementing CLI tools.

    """

    signal_names = set(model_names)
    wrong_names = set()
    models_to_flush = signalbus.get_signal_models()
    if signal_names:
        wrong_names = signal_names - {m.__name__ for m in models_to_flush}
        models_to_flush = [
            m for m in models_to_flush if m.__name__ in signal_names
        ]

    for name in wrong_names:
        logger = logging.getLogger(__name__)
        logger.warning('A signal with name "%s" does not exist.', name)

    return models_to_flush
