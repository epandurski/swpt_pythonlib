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
    assert data['debtor_id'] == 2
    assert data['negligible_amount'] == 3.14
    assert data['config_data'] == 'test config data'
    assert data['config_flags'] == 128
    assert data['seqnum'] == 0
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


def test_rejected_config():
    s = ps.RejectedConfigMessageSchema()

    data = s.loads("""{
    "type": "RejectedConfig",
    "creditor_id": 1,
    "debtor_id": 2,
    "negligible_amount": 3.14,
    "config_data": "test config data",
    "config_flags": 128,
    "config_seqnum": 0,
    "config_ts": "2022-01-01T00:00:00Z",
    "rejection_code": "ERROR2",
    "ts": "2022-01-02T00:00:00Z",
    "unknown": "ignored"
    }""")

    assert data['type'] == 'RejectedConfig'
    assert data['creditor_id'] == 1
    assert data['debtor_id'] == 2
    assert data['negligible_amount'] == 3.14
    assert data['config_data'] == 'test config data'
    assert data['config_flags'] == 128
    assert data['config_seqnum'] == 0
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


def test_rejected_transfer():
    s = ps.RejectedTransferMessageSchema()

    data = s.loads("""{
    "type": "RejectedTransfer",
    "creditor_id": 1,
    "debtor_id": 2,
    "coordinator_type": "direct",
    "coordinator_id": 11,
    "coordinator_request_id": 1234,
    "status_code": "ERROR1",
    "total_locked_amount": 0,
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }""")

    assert data['type'] == 'RejectedTransfer'
    assert data['creditor_id'] == 1
    assert data['debtor_id'] == 2
    assert data['coordinator_type'] == 'direct'
    assert data['coordinator_id'] == 11
    assert data['coordinator_request_id'] == 1234
    assert data['status_code'] == 'ERROR1'
    assert data['total_locked_amount'] == 0
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
