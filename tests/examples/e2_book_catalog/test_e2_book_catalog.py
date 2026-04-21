# -*- coding: utf-8 -*-

"""
Example 2 — Book Catalog (sort + query language)
==================================================

Demonstrates advanced query language features on a book catalog dataset:

- **Sort**: single-field and multi-field sorting via ``SortKey``
- **Range query**: tantivy's Lucene-style ``field:[min TO max]``, ``field:>N``
- **Field-specific search**: ``author:blandy``
- **Boolean operators**: ``AND``, ``OR`` in query strings
- **Range + text combo**: ``python AND year:[2020 TO 2025]``

The script is both a **standalone usage example** and a **pytest test**.
It is idempotent — the index directory is deleted and rebuilt on every run.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from sayt2.api import (
    DataSet,
    Hit,
    KeywordField,
    NgramField,
    NumericField,
    SearchResponse,
    SortKey,
    TextField,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DIR_HERE = Path(__file__).absolute().parent
PATH_DATA = DIR_HERE / "data.json"
DIR_INDEX = DIR_HERE / ".index"

# ---------------------------------------------------------------------------
# Schema
#
# NumericFields use indexed=True so that range queries work via the query
# language (e.g. "year:[2020 TO 2025]").  fast=True enables sort.
# ---------------------------------------------------------------------------
FIELDS = [
    KeywordField(name="id"),
    NgramField(name="title", min_gram=2, max_gram=6, boost=3.0),
    TextField(name="author", boost=2.0),
    TextField(name="description"),
    NumericField(name="year", kind="i64", indexed=True, fast=True),
    NumericField(name="price", kind="f64", indexed=True, fast=True),
    NumericField(name="rating", kind="f64", indexed=True, fast=True),
    NumericField(name="pages", kind="i64", indexed=True, fast=True),
]


def downloader() -> list[dict]:
    """Read records from the local JSON file — simulates a data source."""
    with open(PATH_DATA) as f:
        return json.load(f)["records"]


def test():
    # ------------------------------------------------------------------
    # 0. Clean up — ensure idempotency
    # ------------------------------------------------------------------
    if DIR_INDEX.exists():
        shutil.rmtree(DIR_INDEX)

    # ==================================================================
    # Part A: Sort
    # ==================================================================

    # ------------------------------------------------------------------
    # 1. Sort by rating DESC — highest rated books first
    # ------------------------------------------------------------------
    with DataSet(
        dir_root=DIR_INDEX,
        name="books",
        fields=FIELDS,
        downloader=downloader,
        sort=[SortKey(name="rating", descending=True)],
    ) as ds_by_rating:
        r = ds_by_rating.search("python")
        # r.jprint()  # for debug only
        assert r.size >= 2
        ratings = [h.source["rating"] for h in r.hits]
        assert ratings == sorted(ratings, reverse=True), f"Expected descending ratings, got {ratings}"

    # ------------------------------------------------------------------
    # 2. Sort by price ASC — cheapest books first
    # ------------------------------------------------------------------
    with DataSet(
        dir_root=DIR_INDEX,
        name="books_by_price",
        fields=FIELDS,
        downloader=downloader,
        sort=[SortKey(name="price", descending=False)],
    ) as ds_by_price:
        r = ds_by_price.search("python")
        # r.jprint()  # for debug only
        assert r.size >= 2
        prices = [h.source["price"] for h in r.hits]
        assert prices == sorted(prices), f"Expected ascending prices, got {prices}"

    # ------------------------------------------------------------------
    # 3. Multi-field sort: rating DESC, then year DESC (tie-breaker)
    # ------------------------------------------------------------------
    with DataSet(
        dir_root=DIR_INDEX,
        name="books_multi",
        fields=FIELDS,
        downloader=downloader,
        sort=[SortKey(name="rating"), SortKey(name="year")],
    ) as ds_multi:
        r = ds_multi.search("python")
        # r.jprint()  # for debug only
        assert r.size >= 2
        # primary sort: ratings must be non-increasing
        ratings = [h.source["rating"] for h in r.hits]
        assert ratings == sorted(ratings, reverse=True)

    # ==================================================================
    # Part B: Query Language — range, field-specific, boolean
    # ==================================================================

    # Use a plain dataset without sort for query language tests
    with DataSet(
        dir_root=DIR_INDEX,
        name="books_plain",
        fields=FIELDS,
        downloader=downloader,
    ) as ds:
        # ------------------------------------------------------------------
        # 4. Range query: year:[2020 TO 2023]
        # ------------------------------------------------------------------
        r = ds.search("year:[2020 TO 2023]")
        # r.jprint()  # for debug only
        assert r.size >= 1
        for h in r.hits:
            assert 2020 <= h.source["year"] <= 2023, f"year {h.source['year']} out of range"

        # ------------------------------------------------------------------
        # 5. Range query with operator: price:>40
        # ------------------------------------------------------------------
        r = ds.search("price:>40")
        # r.jprint()  # for debug only
        assert r.size >= 1
        for h in r.hits:
            assert h.source["price"] > 40, f"price {h.source['price']} not > 40"

        # ------------------------------------------------------------------
        # 6. Field-specific search: author:blandy
        # ------------------------------------------------------------------
        r = ds.search("author:blandy")
        # r.jprint()  # for debug only
        assert r.size >= 1
        for h in r.hits:
            assert "Blandy" in h.source["author"], f"Expected Blandy, got {h.source['author']}"

        # ------------------------------------------------------------------
        # 7. Boolean AND: text + range combined
        #    "python AND year:[2020 TO 2025]" — Python books from 2020+
        # ------------------------------------------------------------------
        r = ds.search("python AND year:[2020 TO 2025]")
        # r.jprint()  # for debug only
        assert r.size >= 1
        for h in r.hits:
            assert h.source["year"] >= 2020, f"year {h.source['year']} < 2020"

        # ------------------------------------------------------------------
        # 8. Range query on pages: short books (< 300 pages)
        # ------------------------------------------------------------------
        r = ds.search("pages:<300")
        # r.jprint()  # for debug only
        assert r.size >= 1
        for h in r.hits:
            assert h.source["pages"] < 300, f"pages {h.source['pages']} not < 300"

    # ==================================================================
    # Part C: Sort + Range combined
    # ==================================================================

    # ------------------------------------------------------------------
    # 9. Python books from 2020+, sorted by rating DESC
    # ------------------------------------------------------------------
    with DataSet(
        dir_root=DIR_INDEX,
        name="books_sorted_range",
        fields=FIELDS,
        downloader=downloader,
        sort=[SortKey(name="rating")],
    ) as ds_sorted:
        r = ds_sorted.search("python AND year:[2020 TO 2025]")
        # r.jprint()  # for debug only
        assert r.size >= 1
        for h in r.hits:
            assert h.source["year"] >= 2020
        ratings = [h.source["rating"] for h in r.hits]
        assert ratings == sorted(ratings, reverse=True)

        # ==================================================================
        # Part D: Cache behavior
        # ==================================================================

        # ------------------------------------------------------------------
        # 10. Same query + same ds → cache hit
        # ------------------------------------------------------------------
        r2 = ds_sorted.search("python AND year:[2020 TO 2025]")
        assert r2.cache is True
        assert r2.size == r.size

        # ------------------------------------------------------------------
        # 11. Different range → cache miss
        # ------------------------------------------------------------------
        r3 = ds_sorted.search("python AND year:[2015 TO 2019]")
        assert r3.cache is False

    # ------------------------------------------------------------------
    # Done
    # ------------------------------------------------------------------
    print("All assertions passed!")


if __name__ == "__main__":
    test()
