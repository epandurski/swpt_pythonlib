import pytest
from swpt_pythonlib.flask_signalbus.utils import (
    retry_on_deadlock, DBSerializationError)
from sqlalchemy.exc import DBAPIError
from sqlalchemy import text


def test_retry_on_deadlock(db):
    retry = retry_on_deadlock(db.session, retries=5, max_wait=0.0)
    executions = []

    @retry
    def f():
        executions.append(1)
        raise DBSerializationError

    @retry
    def g():
        executions.append(1)
        db.session.execute(text('xxx'))

    with pytest.raises(DBSerializationError):
        f()
    assert len(executions) == 6

    with pytest.raises(DBAPIError):
        g()
    assert len(executions) == 7
