import sys
import time
import logging
import click
from flask.cli import with_appcontext
from flask import current_app
from .utils import report_signal_count


def _get_models_to_flush(signalbus, signal_names, exclude):
    signal_names = set(signal_names)
    exclude = set(exclude)
    models_to_flush = signalbus.get_signal_models()
    if signal_names and exclude:
        logger = logging.getLogger(__name__)
        logger.warning('Specified both SIGNAL_NAMES and exclude option.')
    if signal_names:
        wrong_signal_names = signal_names - {m.__name__ for m in models_to_flush}
        models_to_flush = [m for m in models_to_flush if m.__name__ in signal_names]
    else:
        wrong_signal_names = exclude - {m.__name__ for m in models_to_flush}
    for name in wrong_signal_names:
        logger = logging.getLogger(__name__)
        logger.warning('A signal with name "%s" does not exist.', name)
    return [m for m in models_to_flush if m.__name__ not in exclude]


@click.group()
def signalbus():
    """Perform SignalBus operations."""


@signalbus.command()
@with_appcontext
@click.option('-e', '--exclude', multiple=True, help='Do not flush signals with the specified name.')
@click.option('-r', '--repeat', type=float, help='Flush every FLOAT seconds.')
@click.argument('signal_names', nargs=-1)
def flushmany(signal_names, exclude, repeat):
    """Send pending signals over the message bus.

    If a list of SIGNAL_NAMES is specified, flushes only those
    signals. If no SIGNAL_NAMES are specified, flushes all signals.

    If your database (and its SQLAlchemy dialect) supports "FOR UPDATE SKIP
    LOCKED", multiple processes will be able to run this command in
    parallel, without stepping on each others' toes.

    """

    signalbus = current_app.extensions['signalbus']
    models_to_flush = _get_models_to_flush(signalbus, signal_names, exclude)
    logger = logging.getLogger(__name__)
    logger.info('Started flushing %s.', ', '.join(m.__name__ for m in models_to_flush))

    while True:
        started_at = time.time()
        try:
            signal_count = signalbus.flushmany(models_to_flush)
        except Exception:
            logger.exception('Caught error while sending pending signals.')
            sys.exit(1)

        report_signal_count(signal_count)

        if repeat is None:
            break
        else:
            time.sleep(max(0.0, repeat + started_at - time.time()))


@signalbus.command()
@with_appcontext
def signals():
    """Show all signal types."""

    signalbus = current_app.extensions['signalbus']
    for signal_model in signalbus.get_signal_models():
        click.echo(signal_model.__name__)


@signalbus.command()
@with_appcontext
def pending():
    """Show the number of pending signals by signal type."""

    signalbus = current_app.extensions['signalbus']
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
            click.echo('{} of type "{}"'.format(str(n).rjust(max_chars), signal_name))
    click.echo(25 * '-')
    click.echo('Total pending: {} '.format(total_pending))
