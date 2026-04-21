# -*- coding: utf-8 -*-

"""
Two-layer disk cache for sayt2 datasets.

Layer 1 — **data freshness**: tracks whether the index is up-to-date.
    Expires after ``expire`` seconds, triggering a downloader + rebuild.

Layer 2 — **query results**: caches ``SearchResult`` objects keyed by
    ``(query, limit)``.  Invalidated whenever L1 expires and a rebuild
    happens.

Both layers live in a single ``diskcache.Cache`` instance, distinguished by
key prefixes and linked by a shared tag for bulk eviction.
"""

from __future__ import annotations

import typing as T
from pathlib import Path

import diskcache

if T.TYPE_CHECKING:
    from .dataset import SearchResult


class DataSetCache:
    """
    Manages a two-layer cache backed by `diskcache <https://pypi.org/project/diskcache/>`__.

    :param dir_cache: Directory for the ``diskcache.Cache`` files.
    :param dataset_name: Logical name of the dataset (e.g. ``"books"``).
    :param schema_hash: Short hash of the field definitions — ensures that
        a schema change automatically invalidates all cached data.
    :param expire: Seconds before L1 (data freshness) expires.
        ``None`` means "never expire automatically".
    """

    def __init__(
        self,
        dir_cache: Path,
        dataset_name: str,
        schema_hash: str,
        expire: int | None = None,
    ):
        self._cache = diskcache.Cache(str(dir_cache))
        self._dataset_name = dataset_name
        self._schema_hash = schema_hash
        self._expire = expire
        self._tag = f"dataset:{dataset_name}"

    # -- keys -----------------------------------------------------------------

    @property
    def _freshness_key(self) -> str:
        """L1 key — includes schema_hash so a schema change = auto miss."""
        return f"fresh:{self._dataset_name}:{self._schema_hash}"

    def _query_key(self, query: str, limit: int) -> str:
        """L2 key — deterministic, based on query string and limit."""
        return f"q:{self._dataset_name}:{self._schema_hash}:{query}:{limit}"

    # -- Layer 1: data freshness ----------------------------------------------

    def is_fresh(self) -> bool:
        """Return ``True`` if the dataset index is still considered fresh."""
        return self._freshness_key in self._cache

    def mark_fresh(self) -> None:
        """
        Mark the dataset as fresh.  Starts the L1 expiry countdown.

        Called after a successful ``downloader() → build_index()`` cycle.
        """
        self._cache.set(
            self._freshness_key,
            True,
            expire=self._expire,
            tag=self._tag,
        )

    # -- Layer 2: query result cache ------------------------------------------

    def get_query_result(self, query: str, limit: int) -> "SearchResult | None":
        """
        Return the cached result for *(query, limit)*, or ``None`` on miss.

        Query results are always ``SearchResult`` objects (never ``None``),
        so a ``None`` return unambiguously means cache miss.
        """
        return self._cache.get(self._query_key(query, limit))

    def set_query_result(self, query: str, limit: int, result: SearchResult) -> None:
        """
        Cache a query result.  L2 entries never expire on their own — they
        are bulk-evicted when L1 triggers a rebuild via :meth:`evict_all`.
        """
        self._cache.set(
            self._query_key(query, limit),
            result,
            tag=self._tag,
        )

    # -- eviction -------------------------------------------------------------

    def evict_all(self) -> None:
        """
        Remove **all** entries (L1 + L2) belonging to this dataset.

        Called before a rebuild so that stale query results are not served.
        """
        self._cache.evict(tag=self._tag)

    # -- lifecycle ------------------------------------------------------------

    def close(self) -> None:
        """Close the underlying ``diskcache.Cache``."""
        self._cache.close()
