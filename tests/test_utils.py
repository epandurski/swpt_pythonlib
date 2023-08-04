import os
import pytest
from datetime import date, datetime
from flask import Flask
from swpt_pythonlib import utils as c

MIN_INT32 = -1 << 31
MAX_INT32 = (1 << 31) - 1
MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1
MAX_UINT64 = (1 << 64) - 1


def test_get_config_value():
    os.environ['K1'] = 'one'
    os.environ['K3'] = 'three'
    app = Flask(__name__)
    app.config.from_mapping({'K1': '1', 'K2': 2})
    with app.app_context():
        assert c.get_config_value('K1') == '1'
        with pytest.raises(ValueError):
            c.get_config_value('K2')
        assert c.get_config_value('K3') == 'three'
    assert c.get_config_value('K1') == 'one'
    assert c.get_config_value('K2') is None
    assert c.get_config_value('K3') == 'three'


def test_i64_to_u64():
    assert c.i64_to_u64(0) == 0
    assert c.i64_to_u64(1) == 1
    assert c.i64_to_u64(MAX_INT64) == MAX_INT64
    assert c.i64_to_u64(-1) == MAX_UINT64
    assert c.i64_to_u64(MIN_INT64) == MAX_INT64 + 1
    with pytest.raises(ValueError):
        c.i64_to_u64(MAX_INT64 + 1)
    with pytest.raises(ValueError):
        c.i64_to_u64(MIN_INT64 - 1)


def test_u64_to_i64():
    assert c.u64_to_i64(0) == 0
    assert c.u64_to_i64(1) == 1
    assert c.u64_to_i64(MAX_INT64) == MAX_INT64
    assert c.u64_to_i64(MAX_UINT64) == -1
    assert c.u64_to_i64(MAX_INT64 + 1) == MIN_INT64
    with pytest.raises(ValueError):
        c.u64_to_i64(-1)
    with pytest.raises(ValueError):
        c.u64_to_i64(MAX_UINT64 + 1)


def test_werkzeug_converter():
    from werkzeug.routing import Map, Rule
    from werkzeug.exceptions import NotFound

    m = Map([
        Rule('/debtors/<i64:debtorId>', endpoint='debtors'),
    ], converters={'i64': c.Int64Converter})
    urls = m.bind('example.com', '/')

    # Test URL match:
    assert urls.match('/debtors/0') == ('debtors', {'debtorId': 0})
    assert urls.match('/debtors/1') == ('debtors', {'debtorId': 1})
    assert urls.match('/debtors/9223372036854775807') == ('debtors', {'debtorId': 9223372036854775807})
    assert urls.match('/debtors/9223372036854775808') == ('debtors', {'debtorId': -9223372036854775808})
    assert urls.match('/debtors/18446744073709551615') == ('debtors', {'debtorId': -1})
    with pytest.raises(NotFound):
        assert urls.match('/debtors/01')
    with pytest.raises(NotFound):
        assert urls.match('/debtors/1x')
    with pytest.raises(NotFound):
        assert urls.match('/debtors/18446744073709551616')
    with pytest.raises(NotFound):
        assert urls.match('/debtors/-1')

    # Test URL build:
    assert urls.build('debtors', {'debtorId': 0}) == '/debtors/0'
    assert urls.build('debtors', {'debtorId': 1}) == '/debtors/1'
    assert urls.build('debtors', {'debtorId': 9223372036854775807}) == '/debtors/9223372036854775807'
    assert urls.build('debtors', {'debtorId': -9223372036854775808}) == '/debtors/9223372036854775808'
    with pytest.raises(ValueError):
        assert urls.build('debtors', {'debtorId': 9223372036854775808})
    with pytest.raises(ValueError):
        assert urls.build('debtors', {'debtorId': -9223372036854775809})
    with pytest.raises(ValueError):
        assert urls.build('debtors', {'debtorId': '1x'})


def test_date_to_int24():
    assert c.date_to_int24(date(1970, 1, 1)) == 0
    assert c.date_to_int24(date(1970, 1, 2)) == 1
    assert 365 * 7000 < c.date_to_int24(date(8970, 12, 31)) < 366 * 7000


def test_is_later_event():
    t1 = datetime(2000, 1, 1, 0, 0, 0)
    t2 = datetime(2000, 1, 1, 0, 0, 1)
    assert c.is_later_event((t2, 0), (t1, 0))
    assert not c.is_later_event((t1, 0), (t2, 0))
    assert not c.is_later_event((t1, 0), (t1, 0))
    assert not c.is_later_event((t1, 0), (t1, 1))
    assert c.is_later_event((t1, -2147483648), (t1, 2147483647))
    assert c.is_later_event((t1, 1), (t1, 0))
    assert c.is_later_event((t1, 1000), (t1, 0))
    assert c.is_later_event((t1, 0), (t1, None))
    assert c.is_later_event((t1, 0), (None, None))
    assert c.is_later_event((t1, 0), (None, 1))


def test_increment_seqnum():
    assert MAX_INT32 == 2147483647
    assert MIN_INT32 == -2147483648
    assert c.increment_seqnum(0) == 1
    assert c.increment_seqnum(MAX_INT32) == MIN_INT32
    assert c.increment_seqnum(MIN_INT32) == MIN_INT32 + 1


def test_seqnum_class():
    assert c.Seqnum(0) == c.Seqnum(0)
    assert c.Seqnum(1) > c.Seqnum(0)
    assert c.Seqnum(0) < c.Seqnum(1)
    assert c.Seqnum(MIN_INT32) > c.Seqnum(MAX_INT32)
    assert c.Seqnum(MAX_INT32) < c.Seqnum(MIN_INT32)
    assert c.Seqnum(-10) > c.Seqnum(MAX_INT32)
    assert c.Seqnum(0).increment().value == 1
    assert c.Seqnum(MAX_INT32).increment().value == MIN_INT32
    assert c.Seqnum(MIN_INT32).increment().value == MIN_INT32 + 1


def test_calc_bin_routing_key():
    assert c.calc_bin_routing_key(123) == '1.1.1.1.1.1.0.0.0.0.0.1.0.0.0.0.0.1.1.0.0.0.1.1'
    assert c.calc_bin_routing_key(-123) == '1.1.0.0.0.0.1.1.1.1.1.1.1.1.1.0.1.0.1.0.1.1.1.1'
    assert c.calc_bin_routing_key(123, 456) == '0.0.0.0.1.0.0.0.0.1.0.0.0.1.0.0.0.0.1.1.0.1.0.0'

    with pytest.raises(OverflowError):
        c.calc_bin_routing_key(99999999999999999999999999999999999)
    with pytest.raises(Exception):
        c.calc_bin_routing_key('')


def test_i64_to_hex_routing_key():
    assert c.i64_to_hex_routing_key(2) == '00.00.00.00.00.00.00.02'
    assert c.i64_to_hex_routing_key(-2) == 'ff.ff.ff.ff.ff.ff.ff.fe'

    with pytest.raises(OverflowError):
        c.i64_to_hex_routing_key(99999999999999999999999999999999999)
    with pytest.raises(Exception):
        c.i64_to_hex_routing_key('')


def test_sharding_realm():
    with pytest.raises(ValueError):
        c.ShardingRealm('INVALID')

    r = c.ShardingRealm('#')
    for n in range(100):
        assert r.match(n)
        assert r.match(n, match_parent=False)
        assert r.match(n, match_parent=True)

    r = c.ShardingRealm('1.1.1.1.1.1.0.#')
    assert r.match(123)
    assert not r.match(124)

    r = c.ShardingRealm('0.0.0.0.1.0.#')
    assert r.match(123, 456)
    assert not r.match(123, 457)
    assert not r.match(124, 456)

    r = c.ShardingRealm('0.1.#')
    rp = c.ShardingRealm('0.#')
    for n in range(100):
        assert r.match(n, match_parent=True) == rp.match(n)

    r = c.ShardingRealm('1.0.#')
    rp = c.ShardingRealm('1.#')
    for n in range(100):
        assert r.match(n, match_parent=True) == rp.match(n)
