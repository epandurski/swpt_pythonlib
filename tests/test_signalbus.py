import pytest
import flask_sqlalchemy as fsa
from mock import call
from .conftest import SignalBusAlchemy


def test_create_signalbus_alchemy(app):
    assert 'signalbus' not in app.extensions
    db = SignalBusAlchemy(app)
    assert 'signalbus' in app.extensions
    assert len(db.signalbus.get_signal_models()) == 0


def test_create_signalbus_alchemy_init_app(app):
    db = SignalBusAlchemy()
    assert 'signalbus' not in app.extensions
    db.init_app(app)
    assert 'signalbus' in app.extensions
    assert len(db.signalbus.get_signal_models()) == 0


def test_flush_signal_model(app, signalbus, Signal):
    assert len(signalbus.get_signal_models()) == 1
    signalbus.flushmany([Signal])


def test_flush_nonsignal_model(app, signalbus, NonSignal):
    assert len(signalbus.get_signal_models()) == 0
    with pytest.raises(RuntimeError):
        signalbus.flushmany([NonSignal])


def test_flush_signal_send_many_success(
        db, signalbus, send_mock, SignalSendMany):
    assert len(signalbus.get_signal_models()) == 1
    sig1 = SignalSendMany(value='b')
    sig2 = SignalSendMany(value='a')
    db.session.add(sig1)
    db.session.add(sig2)
    db.session.flush()
    sig1_id = sig1.id
    sig2_id = sig2.id
    db.session.commit()
    sent_count = signalbus.flushmany([SignalSendMany])
    assert sent_count == 2
    assert send_mock.call_count == 2
    assert call(sig1_id) in send_mock.call_args_list
    assert call(sig2_id) in send_mock.call_args_list


def test_flush_all_signal_models(app, signalbus, Signal, NonSignal):
    assert len(signalbus.get_signal_models()) == 1
    signalbus.flushmany()


def test_flushmany_signal_model(app, signalbus_with_pending_signal):
    assert signalbus_with_pending_signal.flushmany() == 1


def test_send_signal_success(db, signalbus, send_mock, Signal):
    sig = Signal(name='signal', value='1')
    db.session.add(sig)
    db.session.flush()
    sig_id = sig.id
    send_mock.assert_not_called()
    db.session.commit()
    signalbus.flushmany()
    send_mock.assert_called_once_with(
        sig_id,
        'signal',
        '1',
        {},
        str(sig),
    )
    assert Signal.query.count() == 0


def test_send_signal_error(db, signalbus, send_mock, Signal):
    sig = Signal(name='error', value='1')
    db.session.add(sig)
    db.session.commit()
    assert send_mock.call_count == 0
    with pytest.raises(ValueError):
        signalbus.flushmany()
    assert send_mock.call_count == 1
    assert Signal.query.count() == 1


def test_model_no_autoflush(db, signalbus, send_mock, Signal):
    db.session.add(Signal(name='signal', value='1'))
    db.session.commit()
    assert send_mock.call_count == 0
    assert Signal.query.count() == 1


def test_send_nonsignal_model(db, signalbus, send_mock, NonSignal):
    assert len(signalbus.get_signal_models()) == 0
    db.session.add(NonSignal())
    db.session.flush()
    db.session.commit()
    assert NonSignal.query.count() == 1


def test_signal_with_props_success(
        db, signalbus, send_mock, Signal, SignalProperty):
    sig = Signal(name='signal', value='1')
    sig.properties = [
        SignalProperty(name='first_name', value='John'),
        SignalProperty(name='last_name', value='Doe'),
    ]
    db.session.add(sig)
    db.session.flush()
    assert Signal.query.count() == 1
    assert SignalProperty.query.count() == 2
    sig_id = sig.id
    send_mock.assert_not_called()
    db.session.commit()
    signalbus.flushmany()
    send_mock.assert_called_once()
    args, kwargs = send_mock.call_args
    assert kwargs == {}
    assert len(args) == 5
    assert args[:3] == (sig_id, 'signal', '1')
    props = args[3]
    assert type(props) is dict
    assert len(props) == 2
    assert props['first_name'] == 'John'
    assert props['last_name'] == 'Doe'
    assert Signal.query.count() == 0
    assert SignalProperty.query.count() == 0
    assert args[4] == str(sig)


def test_signal_with_props_is_efficient(app, db, Signal, SignalProperty):
    with app.test_request_context():
        sig = Signal(name='signal', value='1')
        sig.properties = [
            SignalProperty(name='first_name', value='John'),
            SignalProperty(name='last_name', value='Doe'),
        ]
        db.session.add(sig)
        db.session.commit()
        all_statements = [
            q.statement for q in fsa.record_queries.get_recorded_queries()]
        assert not any('SELECT' in s for s in all_statements)


def test_flush_signal_with_props(
        db, signalbus, send_mock, Signal, SignalProperty):
    sig = Signal(name='signal', value='1')
    sig.properties = [
        SignalProperty(name='first_name', value='John'),
        SignalProperty(name='last_name', value='Doe'),
    ]
    db.session.add(sig)
    db.session.commit()
    send_mock.assert_not_called()
    signalbus.flushmany()
    send_mock.assert_called_once()
    props = send_mock.call_args[0][3]
    assert type(props) is dict
    assert len(props) == 2
    assert props['first_name'] == 'John'
    assert props['last_name'] == 'Doe'
    assert Signal.query.count() == 0
    assert SignalProperty.query.count() == 0
