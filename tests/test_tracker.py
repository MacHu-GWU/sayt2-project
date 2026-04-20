# -*- coding: utf-8 -*-

import time
import sqlite3
import multiprocessing
import pytest
from pathlib import Path

from sayt2.exc import TrackerIsLockedError
from sayt2.tracker import Tracker


@pytest.fixture
def tracker(tmp_path):
    return Tracker(db_path=tmp_path / "test.db")


class TestTracker:
    def test_lock_creates_new_row(self, tracker):
        """Row does not exist → INSERT fires → rowcount=1 → lock acquired."""
        token = tracker.lock_it("books")
        assert isinstance(token, str)
        assert len(token) == 32

    def test_lock_and_unlock_cycle(self, tracker):
        """lock → unlock → lock again should all succeed."""
        tracker.lock_it("books")
        tracker.unlock_it("books")
        token = tracker.lock_it("books")
        assert len(token) == 32

    def test_double_lock_raises(self, tracker):
        """Row exists, actively locked → WHERE fails → rowcount=0 → raises."""
        tracker.lock_it("books")
        with pytest.raises(TrackerIsLockedError):
            tracker.lock_it("books")

    def test_expired_lock_reacquired(self, tracker):
        """Row exists, lock expired → ON CONFLICT UPDATE WHERE matches → rowcount=1."""
        tracker.lock_it("books", expire=1)
        time.sleep(1.5)
        token = tracker.lock_it("books", expire=1)
        assert len(token) == 32

    def test_context_manager_acquires_and_releases(self, tracker):
        with tracker.lock("books"):
            with pytest.raises(TrackerIsLockedError):
                tracker.lock_it("books")
        # released after block
        tracker.lock_it("books")

    def test_context_manager_releases_on_exception(self, tracker):
        with pytest.raises(ValueError):
            with tracker.lock("books"):
                raise ValueError("boom")
        tracker.lock_it("books")

    def test_datasets_are_independent(self, tracker):
        tracker.lock_it("books")
        tracker.lock_it("movies")  # different name, no conflict
        with pytest.raises(TrackerIsLockedError):
            tracker.lock_it("books")

    def test_db_and_dirs_created_automatically(self, tmp_path):
        db_path = tmp_path / "a" / "b" / "tracker.db"
        assert not db_path.exists()
        t = Tracker(db_path)
        t.lock_it("x")
        assert db_path.exists()

    def test_table_created_lazily(self, tmp_path):
        """__init__ does not create the table; first lock_it does."""
        db_path = tmp_path / "lazy.db"
        Tracker(db_path)
        conn = sqlite3.connect(str(db_path))
        tables = [
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        ]
        conn.close()
        assert "locks" not in tables

        Tracker(db_path).lock_it("x")

    def test_unique_tokens(self, tracker):
        """Each lock acquisition returns a distinct token."""
        t1 = tracker.lock_it("a")
        t2 = tracker.lock_it("b")
        assert t1 != t2


def _worker_try_lock(db_path: str, name: str, result_queue):
    tracker = Tracker(db_path=Path(db_path))
    try:
        tracker.lock_it(name, expire=30)
        result_queue.put("acquired")
    except TrackerIsLockedError:
        result_queue.put("blocked")


class TestTrackerConcurrency:
    def test_only_one_process_acquires_lock(self, tmp_path):
        db_path = tmp_path / "concurrent.db"
        Tracker(db_path).lock_it("shared", expire=30)

        q = multiprocessing.Queue()
        p = multiprocessing.Process(
            target=_worker_try_lock,
            args=(str(db_path), "shared", q),
        )
        p.start()
        p.join(timeout=5)
        assert q.get(timeout=1) == "blocked"


if __name__ == "__main__":
    from sayt2.tests import run_cov_test

    run_cov_test(
        __file__,
        "sayt2.tracker",
        preview=False,
    )
