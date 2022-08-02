``flask_signalbus`` module
==========================

This module adds to `Flask-SQLAlchemy`_ the capability to conveniently
send messages (*signals*) over a message bus (`RabbitMQ`_ for
example).

The processing of each message involves three steps:

  1. One or more messages are recorded in the SQL database (as rows in
     tables).

  2. The messages are sent over the message bus. This can happen
     automatically after each database transaction commit, or be
     triggered explicitly with a method call, or through the Flask
     `Command Line Interface`_.

  3. Messages' corresponding table rows are deleted.


.. _Flask-SQLAlchemy: http://flask-sqlalchemy.pocoo.org/
.. _RabbitMQ: http://www.rabbitmq.com/


Usage
`````

Each type of message (signal) that we plan to send over the message
bus should have its own database model class defined. For example::

  from flask import Flask
  from flask_sqlalchemy import SQLAlchemy
  from swpt_pythonlib.flask_signalbus import SignalBus

  app = Flask(__name__)
  db = SQLAlchemy(app)
  signalbus = SignalBus(db)

  class MySignal(db.Model):
      id = db.Column(db.Integer, primary_key=True, autoincrement=True)
      message_text = db.Column(db.Text, nullable=False)

      def send_signalbus_message(self):
          # Write some code here, that sends
          # the message over the message bus!

Here, ``MySignal`` represent one particular type of message that we
will be sending over the message bus.


Auto-flushing
`````````````

Each time we add a new object of type ``MySignal`` to ``db.session``,
**flask_signalbus** will take note of that, and finally, when the
database transaction is committed, it will call the
``MySignal.send_signalbus_message`` method, and delete the
corresponding row from the database table. All this will happen
automatically, so that the only thing we need to do as a part of the
database transaction, is to add our message to ``db.session``::

  # =========== Our transaction begins here. ===========

  # We may insert/delete/update some database rows here!!!

  # Here we add our message to the database session:
  db.session.add(MySignal(message_text='Message in a Bottle'))

  # We may insert/delete/update some database rows here too!!!

  db.commit()
  
  # Our transaction is committed. The message has been sent
  # over the message bus. The corresponding row in the
  # database table has been deleted. Auto-magically!

Within one database transaction we can add many messages (signals) of
many different types. As long as they have a
``send_signalbus_message`` method defined, they all will be processed
and sent automatically (flushed).

This *auto-flushing* behavior can be disabled if it is not desired. In
this case, the sending of the recorded messages need to be triggered
explicitly. Note that to maximize the message throughput, usually you
will need to disable auto-flushing, and send messages in batches (see
the `send_signalbus_messages`_ class-method).


Pending Signals
```````````````

When auto-flushing is disabled, or when the program has stopped before
the message had been sent over the message bus, the row representing
the message will remain in the database for some time. We call this a
*pending signal*.

To make sure that pending signals are processed in time, even when the
application that generated them is off-line, it is recommended that
pending signals are flushed periodically, independently from the
application that generates them. This can be done in a ``cron`` job
for example. (See `Command Line Interface`_.)


Application Factory Pattern
```````````````````````````

If you want to use the Flask application factory pattern with
**flask_signalbus**, you should subclass the
:class:`~flask_sqlalchemy.SQLAlchemy` class, adding the
:class:`~swpt_pythonlib.flask_signalbus.SignalBusMixin` mixin to
it. For example::

  from flask_sqlalchemy import SQLAlchemy
  from swpt_pythonlib.flask_signalbus import SignalBusMixin

  class CustomSQLAlchemy(SignalBusMixin, SQLAlchemy):
      pass

  db = CustomSQLAlchemy()

Note that `SignalBusMixin` should always come before
:class:`~flask_sqlalchemy.SQLAlchemy`.


Message Ordering, Message Duplication
`````````````````````````````````````

Normally, **flask_signalbus** does not give guarantees about the order
in which the messages are sent over the message bus. Also, sometimes a
single message can be sent more than once. Keep that in mind while
designing your system.

When you want to guarantee that messages are sent in a particular
order, you should disable *auto-flushing*, define a
``signalbus_order_by`` attribute on the model class, and always use
the ``flushordered`` CLI command to flush the messages
explicitly. (Messages can still be sent more than once, though.)


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

To flush pending signals which have failed to auto-flush, use::

    $ flask signalbus flush

To send a potentially huge number of pending signals, use::

    $ flask signalbus flushmany

To send all pending signals in predictable order, use::

    $ flask signalbus flushordered

For each of these commands, you can specify the exact type of signals
on which to operate.


API Reference
`````````````

.. module:: swpt_pythonlib.flask_signalbus


.. _signal-model:

Signal Model
------------

A *signal model* is an otherwise normal database model class (a
subclass of ``db.Model``), which however has a
``send_signalbus_message`` method defined. For example::

  from flask import Flask
  from flask_sqlalchemy import SQLAlchemy

  app = Flask(__name__)
  db = SQLAlchemy(app)

  class MySignal(db.Model):
      id = db.Column(db.Integer, primary_key=True, autoincrement=True)
      message_text = db.Column(db.Text, nullable=False)
      signalbus_autoflush = False
      signalbus_order_by = (id, db.desc(message_text))

      def send_signalbus_message(self):
          # Send the message to the message bus.

- The ``send_signalbus_message`` method should be implemented in such
  a way that when it returns, the message is guaranteed to be
  successfully sent and stored by the broker. Normally, this means
  that an acknowledge has been received for the message from the
  broker.

.. _`send_signalbus_messages`:
  
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

- The signal model class **may** have a ``signalbus_autoflush``
  boolean attribute defined, which determines if signals of that type
  will be automatically sent over the message bus after each
  transaction commit. If not defined, it defaults to `True`.

- The signal model class **may** have a ``signalbus_order_by`` tuple
  attribute defined, which determines the order in which signals will
  be send over the network by the ``flushordered`` CLI command. If not
  defined, signals will not be ordered.


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
