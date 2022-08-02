``scan_table`` module
=====================

This module implements a PostgreSQL sequential table scanner. The rows
in the table will be read and processed in batches. Each batch will
consist of rows located physically close to each other, thus saving
precious IO bandwidth.

The scanned table must be defined as an SQLAlchemy Core table, or
using an SQLAlchemy's declarative base class.


.. automodule:: swpt_pythonlib.scan_table
   :members:
