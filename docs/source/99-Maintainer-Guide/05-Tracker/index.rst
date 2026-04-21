Tracker — Cross-Process Locking
==============================================================================
:mod:`sayt2.tracker` provides a SQLite-backed lock manager that prevents
concurrent index builds from corrupting each other.  A single ``.db`` file can
manage locks for multiple datasets (one row per dataset name).


Why SQLite?
------------------------------------------------------------------------------
Index building is a write-heavy operation.  If two processes try to rebuild the
same index simultaneously, the result is undefined.  A lock ensures only one
writer proceeds at a time.

SQLite is the ideal back-end for this lock because:

- **Atomic** — the lock check and acquisition happen in a single SQL
  statement (``INSERT ... ON CONFLICT DO UPDATE ... WHERE``).  There is no gap
  between "is it free?" and "I'll take it" where another process could slip in.
- **Cross-process** — SQLite's built-in file locking works reliably across
  processes on every major OS.
- **Zero-dependency** — ``sqlite3`` ships with the Python standard library.
- **Multi-dataset** — one ``.db`` file holds a row per dataset, avoiding the
  file-per-lock sprawl of file-based locking.


Table schema
------------------------------------------------------------------------------

.. literalinclude:: ../../../../sayt2/tracker.py
   :language: python
   :lines: 20-27

Four columns:

- ``name`` — the dataset identifier (primary key).
- ``lock`` — ``NULL`` when unlocked; a UUID hex string when locked.
- ``lock_at`` — ISO-8601 timestamp of when the lock was acquired.
- ``expire_at`` — ISO-8601 timestamp of when the lock expires.  Used for
  automatic dead-lock recovery if a process crashes while holding the lock.


The UPSERT — atomic lock acquisition
------------------------------------------------------------------------------

.. literalinclude:: ../../../../sayt2/tracker.py
   :language: python
   :lines: 29-45

This single SQL statement handles three cases:

.. list-table::
   :header-rows: 1
   :widths: 40 30 30

   * - Scenario
     - What happens
     - ``rowcount``
   * - Row does not exist
     - ``INSERT`` fires
     - ``1`` (lock acquired)
   * - Row exists, unlocked or expired
     - ``ON CONFLICT UPDATE`` fires
     - ``1`` (lock acquired)
   * - Row exists, actively locked
     - ``WHERE`` fails, update skipped
     - ``0`` (lock denied)

Because the check and the mutation are a single statement executed inside an
implicit SQLite transaction, there is **zero race window**.


Tracker class
------------------------------------------------------------------------------

.. literalinclude:: ../../../../sayt2/tracker.py
   :language: python
   :pyobject: Tracker

Key methods:

:meth:`~sayt2.tracker.Tracker.lock_it`
   Acquire the lock atomically.  Returns a UUID token on success; raises
   :class:`~sayt2.exc.TrackerIsLockedError` if the lock is held.

:meth:`~sayt2.tracker.Tracker.unlock_it`
   Release the lock, but **only** if the caller's token matches.  If the lock
   has already expired and been re-acquired by another process, this becomes a
   safe no-op.

:meth:`~sayt2.tracker.Tracker.lock`
   Context manager for the common acquire-then-release pattern:

   .. code-block:: python

      tracker = Tracker(db_path)
      with tracker.lock("books", expire=60):
          # build index safely ...

   The lock is guaranteed to be released on exit — even if an exception is
   raised.


Lazy table creation
------------------------------------------------------------------------------
The ``locks`` table is **not** created in ``__init__``.  Instead, the private
:meth:`~sayt2.tracker.Tracker._execute` method catches
``sqlite3.OperationalError`` ("no such table"), creates the table, and retries
the original statement.  This keeps the happy path fast (no extra ``CREATE
TABLE IF NOT EXISTS`` round-trip on every call) while remaining self-healing on
first use.
