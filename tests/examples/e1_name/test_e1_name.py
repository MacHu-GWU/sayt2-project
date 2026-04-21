# -*- coding: utf-8 -*-

"""
Example 1 — Name Search (ngram + full-text + fuzzy)
====================================================

Demonstrates three search modes on a "people directory" dataset:

- **Ngram**: substring matching on the ``name`` field (search-as-you-type)
- **Full-text**: word-level BM25 search on ``title`` and ``bio`` fields
- **Fuzzy**: typo-tolerant search on ``title`` and ``bio`` fields

The script is both a **standalone usage example** and a **pytest test**.
It is idempotent — the index directory is deleted and rebuilt on every run.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from sayt2.api import (
    DataSet,
    NgramField,
    TextField,
    KeywordField,
    StoredField,
    Hit,
    SearchResult,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DIR_HERE = Path(__file__).absolute().parent
PATH_DATA = DIR_HERE / "data.json"
DIR_INDEX = DIR_HERE / ".index"

# ---------------------------------------------------------------------------
# Schema — three search modes in one dataset
# ---------------------------------------------------------------------------
FIELDS = [
    # stored only — returned in results but not searchable
    KeywordField(name="id"),
    # ngram — search-as-you-type on person name
    NgramField(name="name", min_gram=2, max_gram=6, boost=3.0),
    # full-text — word-level BM25 on title and bio
    TextField(name="title", boost=2.0),
    TextField(name="bio"),
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

    # ------------------------------------------------------------------
    # 1. Create the dataset with a downloader
    # ------------------------------------------------------------------
    with DataSet(
        dir_root=DIR_INDEX,
        name="people",
        fields=FIELDS,
        downloader=downloader,
        cache_expire=None,  # no automatic expiry for this example
    ) as ds:
        # ------------------------------------------------------------------
        # 2. Ngram search — substring matching on the "name" field
        #    Typing "ali" should match "Alice Johnson"
        # ------------------------------------------------------------------
        r = ds.search("ali")
        # r.jprint()  # for debug only
        assert isinstance(r, SearchResult)
        assert r.size >= 1
        assert r.fresh is True  # first search triggers build_index
        assert r.cache is False
        names = [h.source["name"] for h in r.hits]
        assert any("Alice" in n for n in names), f"Expected 'Alice' in {names}"

        # partial name: "wan" → "Charlie Wang"
        r = ds.search("wan")
        # r.jprint()  # for debug only
        assert r.size >= 1
        assert any("Wang" in h.source["name"] for h in r.hits)

        # partial name: "mar" → "Bob Martinez"
        r = ds.search("mar")
        # r.jprint()  # for debug only
        assert r.size >= 1
        assert any("Martinez" in h.source["name"] for h in r.hits)

        # ------------------------------------------------------------------
        # 3. Full-text search — word matching on title and bio
        #    "machine learning" should hit Alice (title & bio) and Edward (title)
        # ------------------------------------------------------------------
        r = ds.search("machine learning")
        # r.jprint()  # for debug only
        assert r.size >= 1
        ids = {h.source["id"] for h in r.hits}
        assert "1" in ids or "5" in ids, f"Expected Alice or Edward, got ids={ids}"

        # "kubernetes" appears only in Diana's bio
        r = ds.search("kubernetes")
        # r.jprint()  # for debug only
        assert r.size >= 1
        assert any("Diana" in h.source["name"] for h in r.hits)

        # ------------------------------------------------------------------
        # 4. Cache — second identical query should hit L2 cache
        # ------------------------------------------------------------------
        r1 = ds.search("kubernetes")
        # r1.jprint()  # for debug only
        assert r1.cache is True  # same query, should be cached
        assert r1.size >= 1

        # different query → cache miss
        r2 = ds.search("react typescript")
        # r2.jprint()  # for debug only
        assert r2.cache is False
        assert r2.size >= 1
        assert any("Charlie" in h.source["name"] for h in r2.hits)

        # ------------------------------------------------------------------
        # 5. Refresh — force rebuild, cache should be invalidated
        # ------------------------------------------------------------------
        r3 = ds.search("kubernetes", refresh=True)
        # r3.jprint()  # for debug only
        assert r3.fresh is True
        assert r3.cache is False

        # after refresh, the same query should be cached again
        r4 = ds.search("kubernetes")
        # r4.jprint()  # for debug only
        assert r4.cache is True

        # ------------------------------------------------------------------
        # 6. Build index explicitly with inline data
        # ------------------------------------------------------------------
        count = ds.build_index(data=downloader())
        assert count == 10

        # after explicit build, cache is evicted — next search is a miss
        r5 = ds.search("kubernetes")
        # r5.jprint()  # for debug only
        assert r5.cache is False

        # ------------------------------------------------------------------
        # 7. Verify all stored fields are returned
        # ------------------------------------------------------------------
        r = ds.search("alice")
        # r.jprint()  # for debug only
        assert r.size >= 1
        hit = r.hits[0]
        assert isinstance(hit, Hit)
        assert isinstance(hit.score, float)
        assert "id" in hit.source
        assert "name" in hit.source
        assert "title" in hit.source
        assert "bio" in hit.source

    # ------------------------------------------------------------------
    # Done
    # ------------------------------------------------------------------
    print("All assertions passed!")


if __name__ == "__main__":
    test()
