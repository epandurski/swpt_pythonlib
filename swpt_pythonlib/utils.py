from __future__ import annotations
import os
import re
from hashlib import md5
from functools import total_ordering
from datetime import date, datetime, timedelta
from typing import Optional, Tuple
from werkzeug.routing import BaseConverter, ValidationError
from flask import current_app

_MIN_INT32 = -1 << 31
_MAX_INT32 = (1 << 31) - 1
_MIN_INT64 = -1 << 63
_MAX_INT64 = (1 << 63) - 1
_MAX_UINT64 = (1 << 64) - 1
_I64_SPAN = _MAX_UINT64 + 1
_DATE_1970_01_01 = date(1970, 1, 1)
_TD_PLUS_2SECONDS = timedelta(seconds=2)
_TD_MINUS_2SECONDS = timedelta(seconds=-2)
_RE_ROUTING_KEY = re.compile(r"^((?:[01]\.){0,20})\#$")


class _MISSING:
    pass


class ShardingRealm:
    """Holds a sharding key and a sharding mask.

    This class allows to easily check if a given shard is responsible for a
    given creditor/debtor ID (or a debtor ID, creditor ID pair).

    :ivar realm_mask: Bitmask for the realm (a 32-bit unsigned integer).
    :ivar realm: Bitprefix for the realm (a 32-bit unsigned integer)

    :ivar parent_realm_mask: Bitmask for the parent realm.
    :ivar parent_realm: Bitprefix for the parent realm.
    """

    def __init__(self, routing_key: str):
        m = _RE_ROUTING_KEY.match(routing_key)
        if m is None:
            raise ValueError("invalid routing key")

        bits = m[1].replace(".", "")
        n = len(bits)
        assert n <= 32
        p = 32 - n
        self.realm_mask = ((1 << n) - 1) << p
        self.realm = int("0" + bits, 2) << p
        self.parent_realm_mask = self.realm_mask & (self.realm_mask << 1)
        self.parent_realm = self.realm & self.parent_realm_mask

    def _match_md5_hash(self, md5_hash: bytes, match_parent=False) -> bool:
        sharding_key = int.from_bytes(md5_hash[:4], byteorder="big")
        if match_parent:
            return sharding_key & self.parent_realm_mask == self.parent_realm
        else:
            return sharding_key & self.realm_mask == self.realm

    def match_str(self, s: str, match_parent=False) -> bool:
        """Return whether the shard is responsible for the passed string-key.

        Also, it is possible to check whether the parent shard would
        be responsible for the passed string-key.

        Example::

          >>> r = ShardingRealm('1.#')
          >>> r.match("65")
          True
          >>> r.match("59")
          False
          >>> r.match("59", match_parent=True)  # The parent shard is "#"
          True

        """

        m = md5()
        m.update(s.encode("utf8"))
        return self._match_md5_hash(m.digest(), match_parent=match_parent)

    def match(self, first: int, *rest: int, match_parent=False) -> bool:
        """Return whether the shard is responsible for the passed sharding key.

        Also, it is possible to check whether the parent shard would be
        responsible for the passed sharding key.

        Example::

          >>> r = ShardingRealm('1.#')
          >>> r.match(1)
          True
          >>> r.match(3)
          False
          >>> r.match(3, match_parent=True)  # The parent shard is "#"
          True
          >>> r.match(1, 2)  # (Debtor ID, Creditor ID) pair
          True

        """

        return self._match_md5_hash(
            _calc_md5_hash(first, *rest), match_parent=match_parent
        )


@total_ordering
class Seqnum:
    """A signed 32-bit integer seqnum value.

    Comparisions beteen `Seqnum` instances correctly deal with the
    possible 32-bit integer wrapping.

    """

    def __init__(self, value: int):
        assert _MIN_INT32 <= value <= _MAX_INT32
        self.value = value

    def __eq__(self, other: object):
        return isinstance(other, Seqnum) and self.value == other.value

    def __gt__(self, other: Seqnum):
        return 0 < (self.value - other.value) % 0x100000000 < 0x80000000

    def increment(self) -> Seqnum:
        """Return an incremented instance."""

        value = self.value
        assert _MIN_INT32 <= value <= _MAX_INT32
        return Seqnum(_MIN_INT32 if value == _MAX_INT32 else value + 1)


def get_config_value(key: str) -> Optional[str]:
    """Get the value for the configuration variable with a name `key`.

    The returned value is either a string or `None`. If there is a
    `Flask` application context, the app's config will be checked
    first. If that fails, the environment will be checked next. If
    that fails too, `None` will be returned.

    """

    app_config_value = (
        current_app.config.get(key, _MISSING) if current_app else _MISSING
    )

    if app_config_value is _MISSING:
        return os.environ.get(key)

    if not isinstance(app_config_value, str):
        raise ValueError(f'a non-string value for "{key}"')

    return app_config_value


def i64_to_u64(value: int) -> int:
    """Convert a signed 64-bit integer to unsigned 64-bit integer.

    Raises `ValueError` if the value is not in the range of signed
    64-bit integers.

    """

    if value > _MAX_INT64 or value < _MIN_INT64:
        raise ValueError()
    if value >= 0:
        return value
    return value + _I64_SPAN


def u64_to_i64(value: int) -> int:
    """Convert an unsigned 64-bit integer to a signed 64-bit integer.

    Raises `ValueError` if the value is not in the range of unsigned
    64-bit integers.

    """

    if value > _MAX_UINT64 or value < 0:
        raise ValueError()
    if value <= _MAX_INT64:
        return value
    return value - _I64_SPAN


class Int64Converter(BaseConverter):
    """Flask URL converter for signed 64-bit integers.

    The converter can be registered with the Flask app like this::

      from flask import Flask
      from swpt_pythonlib.utils import Int64Converter

      app = Flask(__name__)
      app.url_map.converters['i64'] = Int64Converter

    """

    regex = r"0|[1-9][0-9]{0,19}"

    def to_python(self, value):
        try:
            return u64_to_i64(int(value))
        except ValueError:
            raise ValidationError()

    def to_url(self, value):
        value = int(value)
        return str(i64_to_u64(value))


def date_to_int24(d: date) -> int:
    """Return a non-negative 24-bit integer derived from a date.

    The passed date must not be before January 1st, 1970. The returned
    integer equals the number of days passed since January 1st, 1970.

    """

    days = (d - _DATE_1970_01_01).days
    assert days >= 0
    assert days >> 24 == 0
    return days


def is_later_event(
    event: Tuple[datetime, int],
    other_event: Tuple[Optional[datetime], Optional[int]],
) -> bool:
    """Return whether `event` is later than `other_event`.

    Each of the passed events must be a (`datetime`, `int`) tuple. The
    `datetime` must be the event timestamp, and the `int` must be the
    event sequential number (32-bit signed integer, with eventual
    wrapping).

    An event with a noticeably later timestamp (>= 1s) is always
    considered later than an event with an earlier timestamp. Only
    when the two timestamps are very close (< 1s), the sequential
    numbers of the events are compared. When the timestamp of
    `other_event` is `None`, `event` is considered as a later event.

    Note that sequential numbers are compared with possible 32-bit
    signed integer wrapping in mind. For example, compared to
    2147483647, -21474836478 is considered a later sequential number.

    """

    ts, seqnum = event
    other_ts, other_seqnum = other_event
    if other_ts is None:
        return True
    advance = ts - other_ts
    if advance >= _TD_PLUS_2SECONDS:
        return True
    if advance <= _TD_MINUS_2SECONDS:
        return False
    return (
        other_seqnum is None
        or 0 < (seqnum - other_seqnum) % 0x100000000 < 0x80000000
    )


def increment_seqnum(n: int) -> int:
    """Increment a 32-bit signed integer with wrapping."""

    assert _MIN_INT32 <= n <= _MAX_INT32
    return _MIN_INT32 if n == _MAX_INT32 else n + 1


def i64_to_hex_routing_key(n: int):
    """Calculate a hexadecimal RabbitMQ routing key from a i64 number.

    The hexadecimal routing key is calculated by placing the 8 bytes
    of the number together, separated with dots. For example::

      >>> i64_to_hex_routing_key(2)
      '00.00.00.00.00.00.00.02'
      >>> i64_to_hex_routing_key(-2)
      'ff.ff.ff.ff.ff.ff.ff.fe'

    """

    bytes_n = n.to_bytes(8, byteorder="big", signed=True)
    assert len(bytes_n) == 8
    return ".".join([format(byte, "02x") for byte in bytes_n])


def calc_bin_routing_key(first: int, *rest: int) -> str:
    """Calculate a binary RabbitMQ routing key from one or more i64 numbers.

    The binary routing key is calculated by taking the highest 24
    bits, separated with dots, of the MD5 digest of the passed
    numbers. For example::

      >>> calc_bin_routing_key(123)
      '1.1.1.1.1.1.0.0.0.0.0.1.0.0.0.0.0.1.1.0.0.0.1.1'
      >>> calc_bin_routing_key(-123)
      '1.1.0.0.0.0.1.1.1.1.1.1.1.1.1.0.1.0.1.0.1.1.1.1'
      >>> calc_bin_routing_key(123, 456)
      '0.0.0.0.1.0.0.0.0.1.0.0.0.1.0.0.0.0.1.1.0.1.0.0'

    """

    md5_hash = _calc_md5_hash(first, *rest)
    s = "".join([format(byte, "08b") for byte in md5_hash[:3]])
    assert len(s) == 24
    return ".".join(s)


def calc_iri_routing_key(iri: str) -> str:
    """Calculate a binary RabbitMQ routing key from an IRI string.

    The binary routing key is calculated by taking the highest 24
    bits, separated with dots, of the MD5 digest of the UTF-8
    serialization of the passed IRI string. For example::

      >>> calc_iri_routing_key("https://example.com/iri")
      '0.1.0.0.0.0.1.1.1.1.0.1.0.1.0.0.1.1.0.0.0.1.0.1'

    """

    m = md5()
    m.update(iri.encode("utf8"))
    md5_hash = m.digest()
    s = "".join([format(byte, "08b") for byte in md5_hash[:3]])
    assert len(s) == 24
    return ".".join(s)


def _calc_md5_hash(first: int, *rest: int) -> bytes:
    m = md5()
    m.update(first.to_bytes(8, byteorder="big", signed=True))
    for n in rest:
        m.update(n.to_bytes(8, byteorder="big", signed=True))
    return m.digest()
