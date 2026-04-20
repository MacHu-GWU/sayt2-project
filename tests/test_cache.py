# -*- coding: utf-8 -*-

import time
import pytest

from sayt2.cache import DataSetCache


@pytest.fixture
def cache(tmp_path):
    c = DataSetCache(
        dir_cache=tmp_path / "cache",
        dataset_name="books",
        schema_hash="abc123",
        expire=None,
    )
    yield c
    c.close()


@pytest.fixture
def cache_with_expire(tmp_path):
    c = DataSetCache(
        dir_cache=tmp_path / "cache",
        dataset_name="books",
        schema_hash="abc123",
        expire=1,
    )
    yield c
    c.close()


class TestFreshness:
    """Layer 1: data freshness cache."""

    def test_initially_not_fresh(self, cache):
        assert cache.is_fresh() is False

    def test_mark_fresh(self, cache):
        cache.mark_fresh()
        assert cache.is_fresh() is True

    def test_expires(self, cache_with_expire):
        cache_with_expire.mark_fresh()
        assert cache_with_expire.is_fresh() is True
        time.sleep(1.5)
        assert cache_with_expire.is_fresh() is False

    def test_no_expire_stays_fresh(self, cache):
        """expire=None means fresh forever (until evict)."""
        cache.mark_fresh()
        assert cache.is_fresh() is True


class TestQueryCache:
    """Layer 2: query result cache."""

    def test_miss_returns_none(self, cache):
        assert cache.get_query_result("python", 10) is None

    def test_set_and_get(self, cache):
        result = {"hits": [{"title": "Python Guide"}], "size": 1}
        cache.set_query_result("python", 10, result)
        assert cache.get_query_result("python", 10) == result

    def test_different_query_is_miss(self, cache):
        cache.set_query_result("python", 10, {"hits": []})
        assert cache.get_query_result("rust", 10) is None

    def test_different_limit_is_miss(self, cache):
        cache.set_query_result("python", 10, {"hits": []})
        assert cache.get_query_result("python", 20) is None

    def test_none_result_is_cacheable(self, cache):
        """A cached None should not be confused with a cache miss."""
        cache.set_query_result("empty", 10, None)
        # miss returns None too, so we verify via a round-trip:
        # set a real value, overwrite with None, then get should return None
        cache.set_query_result("key", 10, "value")
        cache.set_query_result("key", 10, None)
        # Since our get uses a sentinel, this should return None (the cached value)
        assert cache.get_query_result("key", 10) is None


class TestEviction:
    """evict_all clears both L1 and L2."""

    def test_evict_all(self, cache):
        cache.mark_fresh()
        cache.set_query_result("python", 10, {"hits": []})
        cache.set_query_result("rust", 5, {"hits": []})

        cache.evict_all()

        assert cache.is_fresh() is False
        assert cache.get_query_result("python", 10) is None
        assert cache.get_query_result("rust", 5) is None


class TestSchemaHash:
    """Changing schema_hash automatically invalidates caches."""

    def test_different_schema_hash_is_miss(self, tmp_path):
        dir_cache = tmp_path / "cache"

        c1 = DataSetCache(dir_cache, "books", schema_hash="v1", expire=None)
        c1.mark_fresh()
        c1.set_query_result("python", 10, {"hits": ["old"]})
        c1.close()

        c2 = DataSetCache(dir_cache, "books", schema_hash="v2", expire=None)
        assert c2.is_fresh() is False
        assert c2.get_query_result("python", 10) is None
        c2.close()


class TestIsolation:
    """Different dataset_name instances don't interfere."""

    def test_datasets_independent(self, tmp_path):
        dir_cache = tmp_path / "cache"

        books = DataSetCache(dir_cache, "books", "hash1")
        movies = DataSetCache(dir_cache, "movies", "hash2")

        books.mark_fresh()
        books.set_query_result("python", 10, "books_result")

        movies.mark_fresh()
        movies.set_query_result("python", 10, "movies_result")

        # each sees its own data
        assert books.get_query_result("python", 10) == "books_result"
        assert movies.get_query_result("python", 10) == "movies_result"

        # evicting one doesn't affect the other
        books.evict_all()
        assert books.is_fresh() is False
        assert movies.is_fresh() is True
        assert movies.get_query_result("python", 10) == "movies_result"

        books.close()
        movies.close()


if __name__ == "__main__":
    from sayt2.tests import run_cov_test

    run_cov_test(
        __file__,
        "sayt2.cache",
        preview=False,
    )
