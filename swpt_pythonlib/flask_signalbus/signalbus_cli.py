import sys
import time
import logging
import click
from flask.cli import with_appcontext
from flask import current_app
from .signalbus import SignalBus, get_models_to_flush


@click.group()
def signalbus() -> None:
    """Perform SignalBus operations."""


@signalbus.command()
@with_appcontext
@click.option("-r", "--repeat", type=float, help="Flush every FLOAT seconds.")
@click.argument("signal_names", nargs=-1)
def flushmany(signal_names: list[str], repeat: float) -> None:
    """Send pending signals over the message bus.

    If a list of SIGNAL_NAMES is specified, flushes only those
    signals. If no SIGNAL_NAMES are specified, flushes all signals.

    If your database (and its SQLAlchemy dialect) supports "FOR UPDATE SKIP
    LOCKED", multiple processes will be able to run this command in
    parallel, without stepping on each others' toes.
    """

    signalbus: SignalBus = current_app.extensions["signalbus"]
    models_to_flush = get_models_to_flush(signalbus, signal_names)
    logger = logging.getLogger(__name__)
    logger.info(
        "Started flushing %s.", ", ".join(m.__name__ for m in models_to_flush)
    )

    while True:
        started_at = time.time()
        try:
            count = signalbus.flushmany(models_to_flush)
        except Exception:
            logger.exception("Caught error while sending pending signals.")
            sys.exit(1)

        if count > 0:
            logger.info("%i signals have been successfully processed.", count)
        else:  # pragma: no cover
            logger.debug("0 signals have been processed.")

        if repeat is None:
            break
        else:  # pragma: no cover
            time.sleep(max(0.0, repeat + started_at - time.time()))


@signalbus.command()
@with_appcontext
def signals() -> None:
    """Show all signal types."""

    signalbus: SignalBus = current_app.extensions["signalbus"]
    for signal_model in signalbus.get_signal_models():
        click.echo(signal_model.__name__)


@signalbus.command()
@with_appcontext
def pending() -> None:
    """Show the number of pending signals by signal type."""

    signalbus: SignalBus = current_app.extensions["signalbus"]
    pending = []
    total_pending = 0
    for signal_model in signalbus.get_signal_models():
        count = signal_model.query.count()
        if count > 0:
            pending.append((count, signal_model.__name__))
        total_pending += count

    if pending:
        pending.sort()
        max_chars = len(str(pending[-1][0]))
        for n, signal_name in pending:
            click.echo(f'{str(n).rjust(max_chars)} of type "{signal_name}"')

    click.echo(25 * "-")
    click.echo("Total pending: {} ".format(total_pending))
