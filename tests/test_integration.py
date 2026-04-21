# -*- coding: utf-8 -*-

"""
End-to-end integration tests for sayt2.

Exercises the full DataSet lifecycle: downloader → build_index → search,
including cache hits, data refresh, multi-dataset coexistence, and
search-as-you-type simulation.
"""

import time

import pytest

from sayt2.api import (
    DataSet,
    DataSetCache,
    KeywordField,
    NgramField,
    NumericField,
    SearchResponse,
    SortKey,
    TextField,
    Tracker,
    TrackerIsLockedError,
)


# -- sample data --------------------------------------------------------------

BOOKS_FIELDS = [
    KeywordField(name="id"),
    NgramField(name="title", min_gram=2, max_gram=6),
    TextField(name="author"),
    NumericField(name="year", kind="i64", indexed=False, fast=True),
    NumericField(name="rating", kind="f64", indexed=False, fast=True),
]

BOOKS = [
    {"id": "1", "title": "Python Crash Course", "author": "Eric Matthes", "year": 2019, "rating": 4.7},
    {"id": "2", "title": "Fluent Python", "author": "Luciano Ramalho", "year": 2022, "rating": 4.8},
    {"id": "3", "title": "Python Cookbook", "author": "David Beazley", "year": 2013, "rating": 4.6},
    {"id": "4", "title": "Effective Python", "author": "Brett Slatkin", "year": 2020, "rating": 4.7},
    {"id": "5", "title": "Learning Rust", "author": "Jim Blandy", "year": 2021, "rating": 4.5},
    {"id": "6", "title": "Programming Rust", "author": "Jim Blandy", "year": 2021, "rating": 4.6},
    {"id": "7", "title": "The Rust Programming Language", "author": "Steve Klabnik", "year": 2023, "rating": 4.9},
    {"id": "8", "title": "FastAPI Modern Python", "author": "Bill Lubanovic", "year": 2023, "rating": 4.3},
    {"id": "9", "title": "Architecture Patterns with Python", "author": "Harry Percival", "year": 2020, "rating": 4.4},
    {"id": "10", "title": "pandas Cookbook", "author": "Matt Harrison", "year": 2024, "rating": 4.5},
]

MOVIES_FIELDS = [
    KeywordField(name="id"),
    NgramField(name="title", min_gram=2, max_gram=6),
    TextField(name="director"),
]

MOVIES = [
    {"id": "m1", "title": "The Matrix", "director": "Wachowski Sisters"},
    {"id": "m2", "title": "Inception", "director": "Christopher Nolan"},
    {"id": "m3", "title": "Interstellar", "director": "Christopher Nolan"},
]


# -- tests --------------------------------------------------------------------


class TestFullLifecycle:
    """1. Create DataSet → build_index → search → verify results."""

    def test_build_then_search(self, tmp_path):
        ds = DataSet(
            dir_root=tmp_path,
            name="books",
            fields=BOOKS_FIELDS,
        )
        ds.build_index(data=BOOKS)
        r = ds.search("python")
        assert isinstance(r, SearchResponse)
        assert r.size >= 1
        assert r.fresh is False  # data already fresh from build_index
        assert r.cache is False
        assert all("_score" in h for h in r.hits)

    def test_downloader_lifecycle(self, tmp_path):
        ds = DataSet(
            dir_root=tmp_path,
            name="books",
            fields=BOOKS_FIELDS,
            downloader=lambda: BOOKS,
        )
        # first search triggers download + build
        r1 = ds.search("python")
        assert r1.fresh is True
        assert r1.size >= 1

        # second search hits cache
        r2 = ds.search("python")
        assert r2.cache is True
        assert r2.size == r1.size


class TestSearchAsYouType:
    """2. Simulate incremental typing: p → py → pyt → pyth → python."""

    def test_incremental_search(self, tmp_path):
        ds = DataSet(
            dir_root=tmp_path,
            name="books",
            fields=BOOKS_FIELDS,
            downloader=lambda: BOOKS,
        )
        ds.build_index(data=BOOKS)

        prefixes = ["py", "pyt", "pyth", "pytho", "python"]
        prev_size = None
        for prefix in prefixes:
            r = ds.search(prefix)
            assert r.size >= 1, f"No results for '{prefix}'"
            # all results should contain "Python" in title
            for h in r.hits:
                assert "python" in h["title"].lower() or "python" in h.get("author", "").lower(), (
                    f"'{prefix}' matched unexpected doc: {h['title']}"
                )


class TestMultiDatasetCoexistence:
    """3. Two DataSets sharing the same dir_root don't interfere."""

    def test_independent_datasets(self, tmp_path):
        books = DataSet(
            dir_root=tmp_path, name="books", fields=BOOKS_FIELDS,
            downloader=lambda: BOOKS,
        )
        movies = DataSet(
            dir_root=tmp_path, name="movies", fields=MOVIES_FIELDS,
            downloader=lambda: MOVIES,
        )

        rb = books.search("python")
        rm = movies.search("matrix")

        assert rb.size >= 1
        assert rm.size >= 1

        # books search shouldn't find movies
        rb2 = books.search("matrix")
        assert rb2.size == 0

        # movies search shouldn't find books
        rm2 = movies.search("python")
        assert rm2.size == 0


class TestDataRefresh:
    """4. downloader returns different data → refresh=True → new data indexed."""

    def test_refresh_picks_up_new_data(self, tmp_path):
        version = {"v": 1}

        def downloader():
            if version["v"] == 1:
                return BOOKS[:3]
            else:
                return BOOKS  # full set

        ds = DataSet(
            dir_root=tmp_path,
            name="books",
            fields=BOOKS_FIELDS,
            downloader=downloader,
        )

        r1 = ds.search("python")
        size_v1 = r1.size

        version["v"] = 2
        r2 = ds.search("python", refresh=True)
        assert r2.fresh is True
        assert r2.size >= size_v1  # more data now


class TestSortedSearch:
    """Sorted search end-to-end via DataSet."""

    def test_sorted_by_year(self, tmp_path):
        ds = DataSet(
            dir_root=tmp_path,
            name="books",
            fields=BOOKS_FIELDS,
            downloader=lambda: BOOKS,
            sort=[SortKey(name="year")],
        )
        r = ds.search("python")
        if r.size >= 2:
            years = [h["year"] for h in r.hits]
            assert years == sorted(years, reverse=True)

    def test_sorted_by_rating_then_year(self, tmp_path):
        ds = DataSet(
            dir_root=tmp_path,
            name="books",
            fields=BOOKS_FIELDS,
            downloader=lambda: BOOKS,
            sort=[SortKey(name="rating"), SortKey(name="year")],
        )
        r = ds.search("python")
        if r.size >= 2:
            # ratings should be descending
            ratings = [h["rating"] for h in r.hits]
            assert ratings == sorted(ratings, reverse=True), (
                f"Expected descending ratings, got {ratings}"
            )


class TestPerformanceBaseline:
    """5. Index 1000 records and query — sanity check, not a strict benchmark."""

    def test_index_and_query_1000_docs(self, tmp_path):
        docs = [
            {
                "id": str(i),
                "title": f"Book Title {i} Python" if i % 3 == 0 else f"Book Title {i} Rust",
                "author": f"Author {i}",
                "year": 2000 + (i % 25),
                "rating": 3.0 + (i % 20) / 10.0,
            }
            for i in range(1000)
        ]
        ds = DataSet(
            dir_root=tmp_path,
            name="perf",
            fields=BOOKS_FIELDS,
        )

        t0 = time.monotonic()
        ds.build_index(data=docs)
        index_ms = (time.monotonic() - t0) * 1000

        t0 = time.monotonic()
        r = ds.search("python", limit=20)
        query_ms = (time.monotonic() - t0) * 1000

        assert r.size > 0
        # sanity: indexing 1000 docs should be under 5 seconds
        assert index_ms < 5000, f"Indexing took {index_ms:.0f}ms"
        # sanity: query should be under 500ms
        assert query_ms < 500, f"Query took {query_ms:.0f}ms"


if __name__ == "__main__":
    from sayt2.tests import run_cov_test

    run_cov_test(
        __file__,
        "sayt2.dataset",
        preview=False,
    )
