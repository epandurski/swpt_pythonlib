``flask_signalbus`` module
==========================

This module adds to `Flask-SQLAlchemy`_ the capability to conveniently
send messages (*signals*) over a message bus (`RabbitMQ`_ for
example).

The processing of each message involves two steps:

  1. One or more messages are recorded in the SQL database (as rows in
     tables).

  2. The messages are sent over the message bus. This should be explicitly
     triggered with a method call, or through the Flask `Command Line
     Interface`_.


.. _Flask-SQLAlchemy: http://flask-sqlalchemy.pocoo.org/
.. _RabbitMQ: http://www.rabbitmq.com/


Usage
`````

To use the Flask application factory pattern with **flask_signalbus**, you
should subclass the :class:`~flask_sqlalchemy.SQLAlchemy` class, adding the
:class:`~swpt_pythonlib.flask_signalbus.SignalBusMixin` mixin to it. For
example::

  from flask_sqlalchemy import SQLAlchemy
  from swpt_pythonlib.flask_signalbus import SignalBusMixin

  class CustomSQLAlchemy(SignalBusMixin, SQLAlchemy):
      pass

  db = CustomSQLAlchemy()

Note that `SignalBusMixin` should always come before
:class:`~flask_sqlalchemy.SQLAlchemy`.

Each type of message (signal) that we plan to send over the message
bus should have its own database model class defined. For example::

  from flask import Flask
  from flask_sqlalchemy import SQLAlchemy
  from swpt_pythonlib.flask_signalbus import SignalBusMixin

  class CustomSQLAlchemy(SignalBusMixin, SQLAlchemy):
      pass

  app = Flask(__name__)
  db = CustomSQLAlchemy(app)

  class MySignal(db.Model):
      id = db.Column(db.Integer, primary_key=True, autoincrement=True)
      message_text = db.Column(db.Text, nullable=False)

      def send_signalbus_message(self):
          # Write some code here, that sends
          # the message over the message bus!

Here, ``MySignal`` represent one particular type of message that we will be
sending over the message bus. We call this a "signal model".

A *signal model* is an otherwise normal database model class (a subclass of
``db.Model``), which however has a ``send_signalbus_message`` method
defined.

- The ``send_signalbus_message`` method should be implemented in such
  a way that when it returns, the message is guaranteed to be
  successfully sent and stored by the broker. Normally, this means
  that an acknowledge has been received for the message from the
  broker.

- The signal model class **may** have a ``send_signalbus_messages``
  *class method* which accepts one positional argument: an iterable of
  instances of the class. The method should be implemented in such a
  way that when it returns, all messages for the passed instances
  are guaranteed to be successfully sent and stored by the broker.
  Implementing a ``send_signalbus_messages`` class method can greatly
  improve performance, because message brokers are usually optimized
  to process messages in batches much more efficiently.

- The signal model class **may** have a ``signalbus_burst_count``
  integer attribute defined, which determines how many individual
  signals can be sent and deleted at once, as a part of one database
  transaction. This can greatly improve performace in some cases when
  auto-flushing is disabled, especially when the
  ``send_signalbus_messages`` class method is implemented
  efficiently. If not defined, it defaults to ``1``.



Transaction Management Utilities
````````````````````````````````

As a bonus, **flask_signalbus** offers some utilities for transaction
management. See
:class:`~swpt_pythonlib.flask_signalbus.AtomicProceduresMixin` for
details.


Command Line Interface
``````````````````````

**Flask_signalbus** will register a group of Flask CLI commands,
starting with the prefix ``signalbus``. To see all available commands,
use::

    $ flask signalbus --help

To send all pending signals, use::

    $ flask signalbus flushmany

For the last command, you can specify the exact type of signals which to
send.


API Reference
`````````````

.. module:: swpt_pythonlib.flask_signalbus


.. _signal-model:


Functions
---------

.. autofunction:: swpt_pythonlib.flask_signalbus.get_models_to_flush


Classes
-------

.. autoclass:: swpt_pythonlib.flask_signalbus.SignalBus
   :members:


Mixins
------

.. autoclass:: swpt_pythonlib.flask_signalbus.SignalBusMixin
   :members:


.. autoclass:: swpt_pythonlib.flask_signalbus.AtomicProceduresMixin
   :members:


.. autoclass:: swpt_pythonlib.flask_signalbus.atomic._ModelUtilitiesMixin
   :members:


Exceptions
----------

.. autoclass:: swpt_pythonlib.flask_signalbus.DBSerializationError
   :members:
