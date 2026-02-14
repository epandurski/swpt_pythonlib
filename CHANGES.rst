Changelog
=========

0.11.9
--------------

- Fixed tests.


0.11.8
--------------

- In `flask_signalbus`, when the "choose_rows" classmethod is defined,
  always force custom plan.


0.11.7
--------------

- In `flask_signalbus`, when the "choose_rows" classmethod is defined,
  always use nested joins for row selection.


0.11.6
--------------

- In `flask_signalbus`, when the "choose_rows" classmethod is defined,
  always use bitmap scans for row selection.


0.11.5
--------------

- Fixed minor issues.


0.11.4
--------------

- Allow more efficient row selection in `flask_signalbus`. Now if the
  signal class has defined a special classmethod named "choose_rows",
  instead of a "select-in" query, a "with-query" join will be used for
  selecting rows.


0.11.3
--------------

- Added `process_individual_blocks` property to `TableScanner`s. It
  implements an alternative mode of operation, which is to call the
  `process_rows` method separately for each non-empty page (block) in
  the table.


0.11.2
--------------

- Change the way `flask_signalbus` creates the server-side cursor.
  Now, `SET LOCAL enable_seqscan = on` will be send before the cursor
  is created. Also, the random string added to the "select for update"
  query has been removed.


0.11.1
--------------

- In `TableScanner`, update the rhythm when the size of the table
  changes significantly.


0.11.0
--------------

- Changed the way `ThreadPoolProcessor` works. Now, instead of the
  `get_args_collection` argument, the constructor requires the
  `iter_args_collections` argument. The `max_count` argument has been
  removed.


0.10.10
--------------

- Use `execution_options(compiled_cache=None)` for the signal-select query.
- Use `expunge_all` in flask_signalbus


0.10.9
--------------

- Include a random string in the signal-select query. This ensures
  that the query will not be prepared by psycopg3.


0.10.8
--------------

- Use server-side cursor for reading the signals
- Expunge sent signals from the session after commit


0.10.7
--------------

- Minor changes to Support Python 3.14


0.10.6
--------------

- Fixed dependencies' version incompatibilities


0.10.5
--------------

- Fixed dependencies' version specs


Version 0.10.4
--------------

- Added optional `draining_mode` argument to the `Consumer` constructor


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
