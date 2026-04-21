# -*- coding: utf-8 -*-

"""
Cross-process lock manager backed by SQLite.

The entire lock acquisition is a single ``INSERT ... ON CONFLICT DO UPDATE
... WHERE`` (UPSERT) statement — one SQL, one rowcount check, truly atomic.

The ``locks`` table is created lazily on first ``OperationalError``.
"""

import uuid
import sqlite3
from pathlib import Path
from datetime import datetime
from datetime import timezone
from datetime import timedelta
from contextlib import contextmanager

from .exc import TrackerIsLockedError

_CREATE_TABLE_SQL = """\
CREATE TABLE IF NOT EXISTS locks (
    name TEXT PRIMARY KEY,
    lock TEXT,
    lock_at TEXT,
    expire_at TEXT
)
"""

# Single atomic UPSERT to acquire a lock.
#
# Three outcomes:
#   1. Row does not exist           → INSERT fires              → rowcount = 1
#   2. Row exists, unlocked/expired → ON CONFLICT UPDATE fires  → rowcount = 1
#   3. Row exists, actively locked  → WHERE fails, skipped      → rowcount = 0
#
# Expiry is checked against the stored expire_at (set by the original locker),
# not the new caller's expire parameter.
_ACQUIRE_SQL = """\
INSERT INTO locks (name, lock, lock_at, expire_at) VALUES (?, ?, ?, ?)
ON CONFLICT(name) DO UPDATE SET
    lock = excluded.lock,
    lock_at = excluded.lock_at,
    expire_at = excluded.expire_at
WHERE locks.lock IS NULL OR locks.expire_at < ?
"""

# Release: only the lock holder (matching token) can release.
_RELEASE_SQL = "UPDATE locks SET lock = NULL, lock_at = NULL, expire_at = NULL WHERE name = ? AND lock = ?"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Tracker:
    """
    SQLite-backed cross-process lock manager.

    A single ``.db`` file can manage locks for multiple datasets (one row per
    dataset name).  The table and parent directories are created lazily on
    first use.

    :param db_path: Path to the SQLite database file.
    """

    def __init__(self, db_path: Path):
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            str(self._db_path),
            timeout=10.0,
        )
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    @staticmethod
    def _ensure_table(conn: sqlite3.Connection) -> None:
        conn.execute(_CREATE_TABLE_SQL)

    def _execute(
        self, conn: sqlite3.Connection, sql: str, params: tuple = ()
    ) -> sqlite3.Cursor:
        """Execute *sql*; if the table does not exist yet, create it and retry."""
        try:
            return conn.execute(sql, params)
        except sqlite3.OperationalError as e:
            if "no such table" in str(e):
                self._ensure_table(conn)
                return conn.execute(sql, params)
            raise

    def lock_it(self, name: str, expire: int = 60) -> str:
        """
        Atomically acquire a lock for *name* using a single UPSERT.

        - ``rowcount == 1`` → lock acquired (new row or unlocked/expired row).
        - ``rowcount == 0`` → lock is actively held → :exc:`TrackerIsLockedError`.

        :returns: The lock token (UUID hex).
        :raises TrackerIsLockedError: if the lock is held by another process.
        """
        lock_token = uuid.uuid4().hex
        now = _utcnow()
        now_iso = now.isoformat()
        expire_at_iso = (now + timedelta(seconds=expire)).isoformat()

        conn = self._get_conn()
        try:
            with conn:
                cursor = self._execute(
                    conn,
                    _ACQUIRE_SQL,
                    (name, lock_token, now_iso, expire_at_iso, now_iso),
                )
                if cursor.rowcount == 1:
                    return lock_token
                raise TrackerIsLockedError(
                    f"Lock {name!r} is held by another process"
                )
        finally:
            conn.close()

    def unlock_it(self, name: str, lock_token: str) -> None:
        """
        Release the lock for *name*, but only if *lock_token* matches.

        If the token does not match (e.g. the lock expired and was re-acquired
        by another process), this is a silent no-op — the caller's lock is
        already gone.
        """
        conn = self._get_conn()
        try:
            with conn:
                self._execute(conn, _RELEASE_SQL, (name, lock_token))
        finally:
            conn.close()

    @contextmanager
    def lock(self, name: str, expire: int = 60):
        """
        Context manager that acquires the lock on entry and guarantees release
        on exit (even if an exception is raised).

        Usage::

            tracker = Tracker(db_path)
            with tracker.lock("books", expire=60):
                # build index ...
        """
        lock_token = self.lock_it(name, expire)
        try:
            yield
        finally:
            self.unlock_it(name, lock_token)
