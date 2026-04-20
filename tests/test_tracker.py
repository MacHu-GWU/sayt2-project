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
        token = tracker.lock_it("books")
        tracker.unlock_it("books", token)
        token2 = tracker.lock_it("books")
        assert len(token2) == 32

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

    def test_unlock_requires_matching_token(self, tracker):
        """unlock_it with wrong token is a no-op; the lock stays held."""
        tracker.lock_it("books")
        tracker.unlock_it("books", "wrong_token")
        # lock is still held
        with pytest.raises(TrackerIsLockedError):
            tracker.lock_it("books")

    def test_expired_lock_not_released_by_old_holder(self, tracker):
        """
        Scenario: A holds lock → expires → B re-acquires → A calls unlock.
        A's token no longer matches, so B's lock is NOT released.
        """
        token_a = tracker.lock_it("books", expire=1)
        time.sleep(1.5)
        token_b = tracker.lock_it("books", expire=60)

        # A tries to release — should be no-op (token mismatch)
        tracker.unlock_it("books", token_a)

        # B's lock is still held
        with pytest.raises(TrackerIsLockedError):
            tracker.lock_it("books")

        # B can release its own lock
        tracker.unlock_it("books", token_b)
        tracker.lock_it("books")

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
    def test_preacquired_lock_blocks_child(self, tmp_path):
        """Main process holds lock; child process is blocked."""
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

    def test_concurrent_race(self, tmp_path):
        """N processes race for the same lock; exactly one wins."""
        db_path = tmp_path / "race.db"
        # force table creation so all workers start on equal footing
        token = Tracker(db_path).lock_it("warmup", expire=1)
        Tracker(db_path).unlock_it("warmup", token)

        n = 5
        q = multiprocessing.Queue()
        procs = [
            multiprocessing.Process(
                target=_worker_try_lock, args=(str(db_path), "race", q)
            )
            for _ in range(n)
        ]
        for p in procs:
            p.start()
        for p in procs:
            p.join(timeout=10)

        results = [q.get(timeout=1) for _ in range(n)]
        assert results.count("acquired") == 1
        assert results.count("blocked") == n - 1


if __name__ == "__main__":
    from sayt2.tests import run_cov_test

    run_cov_test(
        __file__,
        "sayt2.tracker",
        preview=False,
    )
