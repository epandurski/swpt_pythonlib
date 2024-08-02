Changelog
=========


Version 0.10.3
--------------

- Improved scanning performance


Version 0.10.2
--------------

- Fix scanning error when table has never yet been vacuumed or
  analyzed.


Version 0.10.1
--------------

- Use TID range queries in `scan_table.py`


Version 0.10.0
--------------

- Support version 2.0 of the Swaptacular Messaging Protocol
- Removed `ApproxTs` class
- Improved doc-strings


Version 0.9.6
-------------

- Limit the maximum number of rows per beat in `TableScanner`


Version 0.9.5
-------------

- Added `calc_iri_routing_key` function
- Added `match_str` method to the `ShardingRealm` class


Version 0.9.4
-------------

- Added `ApproxTs` class


Version 0.9.3
-------------

- Change `is_later_event()` time interval to 2 seconds (from 1)
- Optimize the DB serialization failure retry logic



Version 0.9.2
-------------

- Make `get_models_to_flush` a public function
- Use 79-columns "black" formatting


Version 0.9.1
-------------

- Add multiproc_utils.py module
- Minor refactoring
- Do not write a log record, when no messages have been flushed


Version 0.9.0
-------------

- Add type annotations
- Support SQLAlchemy 2 and Flask-SQLAlchemy 3
- Throw away unused code in the `flask_signalbus` module
- Follow PEP8 more closely


Version 0.8.7
-------------

- Improve flush wait logic
- Improve doc-strings


Version 0.8.6
-------------

- Fix package version problem


Version 0.8.5
-------------

- Implement ShardingRealm class


Version 0.8.4
-------------

- Improve flush waiting logic


Version 0.8.3
-------------

- Fix version issue


Version 0.8.2
-------------

- Verify coordinator_id in PrepareTransfer messages


Version 0.8.1
-------------

- Added few utility functions


Version 0.8.0
-------------

- Added protocol_schemas module


Version 0.7.0
-------------

- Added rabbitmq and flask_signalbus modules


Version 0.6.0
-------------

- Initial release
