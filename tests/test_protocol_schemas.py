import pytest
from marshmallow import ValidationError
from datetime import datetime
from swpt_pythonlib import protocol_schemas as ps


def test_configure_account():
    s = ps.ConfigureAccountMessageSchema()

    data = s.loads("""{
    "type": "ConfigureAccount",
    "creditor_id": 1,
    "debtor_id": 2,
    "negligible_amount": 3.14,
    "config_data": "test config data",
    "config_flags": 128,
    "seqnum": 0,
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }""")

    assert data['type'] == 'ConfigureAccount'
    assert data['creditor_id'] == 1
    assert type(data['creditor_id']) is int
    assert data['debtor_id'] == 2
    assert type(data['debtor_id']) is int
    assert data['negligible_amount'] == 3.14
    assert data['config_data'] == 'test config data'
    assert data['config_flags'] == 128
    assert type(data['config_flags']) is int
    assert data['seqnum'] == 0
    assert type(data['seqnum']) is int
    assert data['ts'] == datetime.fromisoformat('2022-01-01T00:00:00+00:00')
    assert "unknown" not in data

    wrong_type = data.copy()
    wrong_type['type'] = 'WrongType'
    wrong_type = s.dumps(wrong_type)
    with pytest.raises(ValidationError, match='Invalid type.'):
        s.loads(wrong_type)

    missing_field = data.copy()
    del missing_field['config_data']
    missing_field = s.dumps(wrong_type)
    with pytest.raises(ValidationError, match='Missing data for required field.'):
        s.loads(missing_field)

    wrong_config_data = data.copy()
    wrong_config_data['config_data'] = 1500 * 'Щ'
    wrong_config_data = s.dumps(wrong_config_data)
    with pytest.raises(ValidationError, match='The length of config_data exceeds 2000 bytes'):
        s.loads(wrong_config_data)

    try:
        s.loads('{}')
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(m == ['Missing data for required field.'] for m in e.messages.values())


def test_rejected_config():
    s = ps.RejectedConfigMessageSchema()

    data = s.loads("""{
    "type": "RejectedConfig",
    "creditor_id": -1,
    "debtor_id": -2,
    "negligible_amount": 0,
    "config_data": "test config data",
    "config_flags": -128,
    "config_seqnum": 2147483647,
    "config_ts": "2022-01-01T00:00:00+00:00",
    "rejection_code": "ERROR2",
    "ts": "2022-01-02T00:00:00Z",
    "unknown": "ignored"
    }""")

    assert data['type'] == 'RejectedConfig'
    assert data['creditor_id'] == -1
    assert data['debtor_id'] == -2
    assert data['negligible_amount'] == 0
    assert type(data['negligible_amount']) is float
    assert data['config_data'] == 'test config data'
    assert data['config_flags'] == -128
    assert type(data['config_flags']) is int
    assert data['config_seqnum'] == 2147483647
    assert type(data['config_seqnum']) is int
    assert data['config_ts'] == datetime.fromisoformat('2022-01-01T00:00:00+00:00')
    assert data['rejection_code'] == 'ERROR2'
    assert data['ts'] == datetime.fromisoformat('2022-01-02T00:00:00+00:00')
    assert "unknown" not in data

    wrong_rejection_code = data.copy()
    wrong_rejection_code['rejection_code'] = 'Кирилица'
    wrong_rejection_code = s.dumps(wrong_rejection_code)
    with pytest.raises(ValidationError, match='The rejection_code field contains non-ASCII characters'):
        s.loads(wrong_rejection_code)

    wrong_config_data = data.copy()
    wrong_config_data['config_data'] = 1500 * 'Щ'
    wrong_config_data = s.dumps(wrong_config_data)
    with pytest.raises(ValidationError, match='The length of config_data exceeds 2000 bytes'):
        s.loads(wrong_config_data)

    try:
        s.loads('{}')
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(m == ['Missing data for required field.'] for m in e.messages.values())


def test_prepare_transfer():
    s = ps.PrepareTransferMessageSchema()

    data = s.loads("""{
    "type": "PrepareTransfer",
    "creditor_id": -1000000000000000,
    "debtor_id": -2000000000000000,
    "coordinator_type": "direct",
    "coordinator_id": 1111111111111111,
    "coordinator_request_id": 123456789012345,
    "min_locked_amount": 1000000000000,
    "max_locked_amount": 2000000000000,
    "recipient": "test recipient",
    "min_interest_rate": -100,
    "max_commit_delay": 2147483647,
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }""")

    assert data['type'] == 'PrepareTransfer'
    assert data['creditor_id'] == -1000000000000000
    assert data['debtor_id'] == -2000000000000000
    assert data['coordinator_type'] == 'direct'
    assert data['coordinator_id'] == 1111111111111111
    assert type(data['coordinator_id']) is int
    assert data['coordinator_request_id'] == 123456789012345
    assert type(data['coordinator_request_id']) is int
    assert data['min_locked_amount'] == 1000000000000
    assert type(data['min_locked_amount']) is int
    assert data['max_locked_amount'] == 2000000000000
    assert type(data['max_locked_amount']) is int
    assert data['recipient'] == 'test recipient'
    assert data['min_interest_rate'] == -100.0
    assert type(data['min_interest_rate']) is float
    assert data['max_commit_delay'] == 2147483647
    assert type(data['max_commit_delay']) is int
    assert data['ts'] == datetime.fromisoformat('2022-01-01T00:00:00+00:00')
    assert "unknown" not in data

    wrong_max_locked_amount = data.copy()
    wrong_max_locked_amount['max_locked_amount'] = 0
    wrong_max_locked_amount = s.dumps(wrong_max_locked_amount)
    with pytest.raises(ValidationError, match='max_locked_amount must be equal or greater than min_locked_amount'):
        s.loads(wrong_max_locked_amount)

    wrong_recipient = data.copy()
    wrong_recipient['recipient'] = 'Кирилица'
    wrong_recipient = s.dumps(wrong_recipient)
    with pytest.raises(ValidationError, match='The recipient field contains non-ASCII characters'):
        s.loads(wrong_recipient)

    try:
        s.loads('{}')
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(m == ['Missing data for required field.'] for m in e.messages.values())


def test_prepared_transfer():
    s = ps.PreparedTransferMessageSchema()

    data = s.loads("""{
    "type": "PreparedTransfer",
    "creditor_id": -1000000000000000,
    "debtor_id": -2000000000000000,
    "transfer_id": -3000000000000000,
    "coordinator_type": "direct",
    "coordinator_id": 1111111111111111,
    "coordinator_request_id": 123456789012345,
    "locked_amount": 1230000000000,
    "recipient": "test recipient",
    "prepared_at": "2022-01-01T00:00:00Z",
    "demurrage_rate": -5.5e0,
    "deadline": "2022-02-01T00:00:00Z",
    "min_interest_rate": -10,
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }""")

    assert data['type'] == 'PreparedTransfer'
    assert data['creditor_id'] == -1000000000000000
    assert data['debtor_id'] == -2000000000000000
    assert data['transfer_id'] == -3000000000000000
    assert type(data['transfer_id']) is int
    assert data['coordinator_type'] == 'direct'
    assert data['coordinator_id'] == 1111111111111111
    assert data['coordinator_request_id'] == 123456789012345
    assert data['locked_amount'] == 1230000000000
    assert type(data['locked_amount']) is int
    assert data['recipient'] == 'test recipient'
    assert data['prepared_at'] == datetime.fromisoformat('2022-01-01T00:00:00+00:00')
    assert data['demurrage_rate'] == -5.5
    assert data['deadline'] == datetime.fromisoformat('2022-02-01T00:00:00+00:00')
    assert data['min_interest_rate'] == -10
    assert type(data['min_interest_rate']) is float
    assert data['ts'] == datetime.fromisoformat('2022-01-01T00:00:00+00:00')
    assert "unknown" not in data

    wrong_recipient = data.copy()
    wrong_recipient['recipient'] = 'Кирилица'
    wrong_recipient = s.dumps(wrong_recipient)
    with pytest.raises(ValidationError, match='The recipient field contains non-ASCII characters'):
        s.loads(wrong_recipient)

    try:
        s.loads('{}')
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(m == ['Missing data for required field.'] for m in e.messages.values())


def test_finalize_transfer():
    s = ps.FinalizeTransferMessageSchema()

    data = s.loads("""{
    "type": "FinalizeTransfer",
    "creditor_id": -1000000000000000,
    "debtor_id": -2000000000000000,
    "transfer_id": -3000000000000000,
    "coordinator_type": "direct",
    "coordinator_id": 1111111111111111,
    "coordinator_request_id": 123456789012345,
    "committed_amount": 1230000000000,
    "transfer_note": "test note",
    "transfer_note_format": "",
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }""")

    assert data['type'] == 'FinalizeTransfer'
    assert data['creditor_id'] == -1000000000000000
    assert data['debtor_id'] == -2000000000000000
    assert data['transfer_id'] == -3000000000000000
    assert type(data['transfer_id']) is int
    assert data['coordinator_type'] == 'direct'
    assert data['coordinator_id'] == 1111111111111111
    assert data['coordinator_request_id'] == 123456789012345
    assert data['committed_amount'] == 1230000000000
    assert type(data['committed_amount']) is int
    assert data['transfer_note'] == 'test note'
    assert data['transfer_note_format'] == ''
    assert data['ts'] == datetime.fromisoformat('2022-01-01T00:00:00+00:00')
    assert "unknown" not in data

    try:
        s.loads('{}')
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(m == ['Missing data for required field.'] for m in e.messages.values())


def test_rejected_transfer():
    s = ps.RejectedTransferMessageSchema()

    data = s.loads("""{
    "type": "RejectedTransfer",
    "creditor_id": 1000000000000000,
    "debtor_id": 2000000000000000,
    "coordinator_type": "direct",
    "coordinator_id": 11,
    "coordinator_request_id": 1234,
    "status_code": "ERROR1",
    "total_locked_amount": 0,
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }""")

    assert data['type'] == 'RejectedTransfer'
    assert data['creditor_id'] == 1000000000000000
    assert data['debtor_id'] == 2000000000000000
    assert data['coordinator_type'] == 'direct'
    assert data['coordinator_id'] == 11
    assert data['coordinator_request_id'] == 1234
    assert data['status_code'] == 'ERROR1'
    assert data['total_locked_amount'] == 0
    assert type(data['total_locked_amount']) is int
    assert data['ts'] == datetime.fromisoformat('2022-01-01T00:00:00+00:00')
    assert "unknown" not in data

    wrong_status_code1 = data.copy()
    wrong_status_code1['status_code'] = 'OK'
    wrong_status_code1 = s.dumps(wrong_status_code1)
    with pytest.raises(ValidationError, match='The status_code field contains an invalid value'):
        s.loads(wrong_status_code1)

    wrong_status_code2 = data.copy()
    wrong_status_code2['status_code'] = 'Кирилица'
    wrong_status_code2 = s.dumps(wrong_status_code2)
    with pytest.raises(ValidationError, match='The status_code field contains non-ASCII characters'):
        s.loads(wrong_status_code2)

    wrong_coordinator_type = data.copy()
    wrong_coordinator_type['coordinator_type'] = 'Кирилица'
    wrong_coordinator_type = s.dumps(wrong_coordinator_type)
    with pytest.raises(ValidationError, match='The coordinator_type field contains non-ASCII characters'):
        s.loads(wrong_coordinator_type)

    empty_coordinator_type = data.copy()
    empty_coordinator_type['coordinator_type'] = ''
    empty_coordinator_type = s.dumps(empty_coordinator_type)
    with pytest.raises(ValidationError, match='Length must be between 1 and'):
        s.loads(empty_coordinator_type)

    try:
        s.loads('{}')
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(m == ['Missing data for required field.'] for m in e.messages.values())
