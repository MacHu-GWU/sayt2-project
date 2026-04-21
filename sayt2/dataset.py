# -*- coding: utf-8 -*-

"""
Core search engine — the only module that imports ``tantivy``.

Responsibilities:
- Build a tantivy schema from field definitions
- Register custom tokenizers (ngram)
- Write documents into the index
- Query the index with automatic field_boosts
- Fuzzy query for typo tolerance
- Multi-field sorting with over-fetch
"""

from __future__ import annotations

import json
import time
import typing as T
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import asdict
from pathlib import Path

import tantivy
from pydantic import BaseModel
from pydantic import PrivateAttr
from tantivy import Filter
from tantivy import Index
from tantivy import SchemaBuilder
from tantivy import TextAnalyzerBuilder
from tantivy import Tokenizer

from .constants import NumericKindEnum
from .cache import DataSetCache
from .fields import T_Field
from .fields import BaseField
from .fields import StoredField
from .fields import KeywordField
from .fields import TextField
from .fields import NgramField
from .fields import NumericField
from .fields import DatetimeField
from .fields import BooleanField
from .fields import fields_schema_hash
from .tracker import Tracker


class SortKey(BaseModel):
    """One element of a multi-field sort specification."""

    name: str
    descending: bool = True


@dataclass(frozen=True)
class Hit:
    """A single search hit with source document and relevance score."""

    source: dict[str, T.Any]
    score: float


@dataclass(frozen=True)
class SearchResult:
    """Immutable search result returned by :meth:`DataSet.search`."""

    hits: list[Hit]
    size: int
    took_ms: int
    fresh: bool
    cache: bool

    def to_json(self) -> str:  # pragma: no cover
        return json.dumps(asdict(self), indent=2, ensure_ascii=False)

    def jprint(self):  # pragma: no cover
        print(self.to_json())


def _ngram_tokenizer_name(f: NgramField) -> str:
    """Deterministic name for a custom ngram tokenizer so it can be re-registered."""
    return f"__sayt2_ngram_{f.min_gram}_{f.max_gram}_{int(f.prefix_only)}_{int(f.lowercase)}"


def _build_ngram_analyzer(f: NgramField) -> tantivy.TextAnalyzer:
    builder = TextAnalyzerBuilder(
        Tokenizer.ngram(
            min_gram=f.min_gram, max_gram=f.max_gram, prefix_only=f.prefix_only
        )
    )
    if f.lowercase:
        builder = builder.filter(Filter.lowercase())
    return builder.build()


def build_schema(
    fields: list[BaseField],
) -> tuple[tantivy.Schema, dict[str, tantivy.TextAnalyzer]]:
    """
    Convert a list of field definitions into a tantivy ``Schema`` and a dict
    of custom tokenizers that must be registered on the ``Index``.

    Returns ``(schema, analyzers)`` where *analyzers* maps tokenizer name →
    ``TextAnalyzer``.
    """
    sb = SchemaBuilder()
    analyzers: dict[str, tantivy.TextAnalyzer] = {}

    for f in fields:
        if isinstance(f, StoredField):
            # stored-only: use raw tokenizer, indexed=False would be ideal
            # but tantivy text_field must be indexed; use raw + stored
            sb.add_text_field(f.name, stored=True, tokenizer_name="raw")

        elif isinstance(f, KeywordField):
            sb.add_text_field(f.name, stored=f.stored, tokenizer_name="raw")

        elif isinstance(f, TextField):
            sb.add_text_field(f.name, stored=f.stored, tokenizer_name=f.tokenizer)

        elif isinstance(f, NgramField):
            tok_name = _ngram_tokenizer_name(f)
            if tok_name not in analyzers:
                analyzers[tok_name] = _build_ngram_analyzer(f)
            sb.add_text_field(f.name, stored=f.stored, tokenizer_name=tok_name)

        elif isinstance(f, NumericField):
            add_fn = {
                NumericKindEnum.I64.value: sb.add_integer_field,
                NumericKindEnum.U64.value: sb.add_unsigned_field,
                NumericKindEnum.F64.value: sb.add_float_field,
            }[f.kind]
            add_fn(f.name, stored=f.stored, indexed=f.indexed, fast=f.fast)

        elif isinstance(f, DatetimeField):
            sb.add_date_field(f.name, stored=f.stored, indexed=f.indexed, fast=f.fast)

        elif isinstance(f, BooleanField):
            sb.add_boolean_field(f.name, stored=f.stored, indexed=f.indexed)

    return sb.build(), analyzers


def open_index(
    dir_index: Path,
    fields: list[BaseField],
) -> Index:
    """
    Open (or create) a tantivy ``Index`` at *dir_index* and register all
    required custom tokenizers.

    Tantivy does **not** persist tokenizer configuration — only the inverted
    index data.  So every ``Index.open()`` / ``Index(schema, path=...)`` must
    be followed by ``register_tokenizer`` calls.
    """
    dir_index.mkdir(parents=True, exist_ok=True)
    schema, analyzers = build_schema(fields)
    index = Index(schema, path=str(dir_index))
    for name, analyzer in analyzers.items():
        index.register_tokenizer(name, analyzer)
    return index


def write_documents(
    index: Index,
    data: T.Iterable[dict[str, T.Any]],
    memory_budget_bytes: int = 128_000_000,
    num_threads: int | None = None,
) -> int:
    """
    Write *data* into *index*.

    :param data: Iterable of dicts, each dict is one document whose keys
        match the field names in the schema.
    :param memory_budget_bytes: Heap budget for the index writer.
    :param num_threads: Number of indexing threads (``None`` = tantivy default).
    :returns: Number of documents written.
    """
    writer_kwargs: dict[str, T.Any] = {"heap_size": memory_budget_bytes}
    if num_threads is not None:
        writer_kwargs["num_threads"] = num_threads

    writer = index.writer(**writer_kwargs)
    count = 0
    for doc in data:
        writer.add_document(tantivy.Document(**doc))
        count += 1
    writer.commit()
    writer.wait_merging_threads()
    index.reload()
    return count


def _collect_search_config(
    fields: list[BaseField],
) -> tuple[list[str], dict[str, float]]:
    """
    Walk *fields* and return ``(searchable_names, field_boosts)``.

    Only fields with a ``boost`` attribute (KeywordField, TextField, NgramField)
    are searchable.  Boosts equal to 1.0 are omitted from the dict since that
    is tantivy's default.
    """
    names: list[str] = []
    boosts: dict[str, float] = {}
    for f in fields:
        if hasattr(f, "boost"):
            names.append(f.name)
            if f.boost != 1.0:
                boosts[f.name] = f.boost
    return names, boosts


def _extract_hits(
    searcher: tantivy.Searcher,
    results: tantivy.SearchResult,
    stored_names: list[str],
) -> list[Hit]:
    """Materialise search results into a list of :class:`Hit` objects."""
    hits: list[Hit] = []
    for score, addr in results.hits:
        doc = searcher.doc(addr)
        source: dict[str, T.Any] = {}
        for name in stored_names:
            values = doc[name]
            if values:
                source[name] = values[0] if len(values) == 1 else values
        hits.append(Hit(source=source, score=score))
    return hits


def search_index(
    index: Index,
    fields: list[BaseField],
    query_str: str,
    limit: int = 20,
) -> list[Hit]:
    """
    Parse *query_str* against the searchable fields in *fields*, execute the
    search, and return up to *limit* hits as a list of dicts.

    Each hit dict contains every stored field from the document plus
    ``_score`` (BM25 relevance score).

    Field boosts declared on field definitions are applied automatically.
    """
    searchable, boosts = _collect_search_config(fields)
    if not searchable:
        return []

    kwargs: dict[str, T.Any] = {}
    if boosts:
        kwargs["field_boosts"] = boosts
    query = index.parse_query(query_str, searchable, **kwargs)
    searcher = index.searcher()
    results = searcher.search(query, limit=limit)
    return _extract_hits(searcher, results, [f.name for f in fields if f.stored])


def fuzzy_search_index(
    index: Index,
    fields: list[BaseField],
    query_str: str,
    limit: int = 20,
    distance: int = 1,
    transposition_cost_one: bool = True,
    prefix: bool = False,
) -> list[Hit]:
    """
    Fuzzy search using ``Query.fuzzy_term_query`` on each TextField.

    Fuzzy matching works on word-level tokenized fields only (TextField),
    not on NgramField or KeywordField.  One fuzzy query per TextField is
    built and combined with ``Occur.Should`` (boolean OR).

    :param distance: Maximum Levenshtein edit distance (1 or 2).
    :param transposition_cost_one: Count adjacent-char swaps as 1 edit.
    :param prefix: Enable prefix Levenshtein mode.
    """
    schema = index.schema
    fuzzy_fields = [f for f in fields if isinstance(f, TextField)]
    if not fuzzy_fields:
        return []

    terms = query_str.split()
    if not terms:
        return []

    sub_queries = []
    for f in fuzzy_fields:
        for term in terms:
            q = tantivy.Query.fuzzy_term_query(
                schema,
                f.name,
                term,
                distance=distance,
                transposition_cost_one=transposition_cost_one,
                prefix=prefix,
            )
            if f.boost != 1.0:
                q = tantivy.Query.boost_query(q, f.boost)
            sub_queries.append((tantivy.Occur.Should, q))

    query = tantivy.Query.boolean_query(sub_queries)
    searcher = index.searcher()
    results = searcher.search(query, limit=limit)
    return _extract_hits(searcher, results, [f.name for f in fields if f.stored])


def _sort_hits(
    hits: list[Hit],
    sort_keys: list[SortKey],
    limit: int,
) -> list[Hit]:
    """
    Sort *hits* by multiple fields (lexicographic) and return the top *limit*.

    Each ``SortKey`` specifies a field name and direction.  Python's stable
    sort with a tuple key handles mixed ascending/descending via negation
    for numeric values and ``reverse=True`` would not work for mixed
    directions, so we sort in successive passes (least-significant key first).
    """
    # Sort by successive keys, least significant first (stable sort preserves
    # earlier ordering for ties).  Work on a copy to avoid mutating the input.
    result = list(hits)
    for sk in reversed(sort_keys):
        result.sort(
            key=lambda h, _name=sk.name: h.source.get(_name, 0),
            reverse=sk.descending,
        )
    return result[:limit]


def search_index_sorted(
    index: Index,
    fields: list[BaseField],
    query_str: str,
    sort_keys: list[SortKey],
    limit: int = 20,
    over_fetch_factor: int = 10,
) -> list[Hit]:
    """
    Search then sort by multiple fields.

    tantivy-py only supports ``order_by_field`` on a single field, so
    multi-field sorting is done in Python after over-fetching.

    :param sort_keys: List of ``SortKey`` specifying sort order.
    :param over_fetch_factor: Fetch ``limit * over_fetch_factor`` candidates
        before sorting.  Ensures the final top-*limit* is accurate.
    """
    over_limit = limit * over_fetch_factor
    hits = search_index(index, fields, query_str, limit=over_limit)
    return _sort_hits(hits, sort_keys, limit)


# ---------------------------------------------------------------------------
# DataSet — high-level integration of index + cache + tracker
# ---------------------------------------------------------------------------


class DataSet(BaseModel):
    """
    High-level search dataset that integrates index building, caching,
    cross-process locking, and query execution into a single object.

    :param dir_root: Root directory; index, cache, and tracker DB are stored
        inside sub-directories of this path.
    :param name: Logical name (e.g. ``"books"``).  Used as the tracker lock
        key and cache namespace.
    :param fields: Field definitions that determine the tantivy schema.
    :param downloader: Optional callable that returns an iterable of document
        dicts.  Called when the data is stale or on first search.
    :param cache_expire: Seconds before L1 cache expires (``None`` = never).
    :param sort: Optional multi-field sort specification.
    :param memory_budget_bytes: Heap budget for the tantivy index writer.
    :param num_threads: Number of indexing threads (``None`` = tantivy default).
    :param lock_expire: Seconds before the tracker lock expires.
    """

    dir_root: Path
    name: str
    fields: list[T_Field]  # type: ignore[type-arg]

    downloader: Callable[[], T.Iterable[dict[str, T.Any]]] | None = None
    cache_expire: int | None = None
    sort: list[SortKey] | None = None

    memory_budget_bytes: int = 128_000_000
    num_threads: int | None = None
    lock_expire: int = 60

    _cache_instance: DataSetCache | None = PrivateAttr(default=None)

    # -- derived paths --------------------------------------------------------

    @property
    def _dir_index(self) -> Path:
        return self.dir_root / self.name / f"index-{self._schema_hash}"

    @property
    def _dir_cache(self) -> Path:
        return self.dir_root / self.name / "cache"

    @property
    def _db_tracker(self) -> Path:
        return self.dir_root / "tracker.db"

    @property
    def _schema_hash(self) -> str:
        return fields_schema_hash(self.fields)

    # -- internal helpers -----------------------------------------------------

    def _open_index(self) -> Index:
        return open_index(self._dir_index, self.fields)

    @property
    def _cache(self) -> DataSetCache:
        if self._cache_instance is None:
            self._cache_instance = DataSetCache(
                self._dir_cache,
                self.name,
                self._schema_hash,
                expire=self.cache_expire,
            )
        return self._cache_instance

    def _get_tracker(self) -> Tracker:
        return Tracker(self._db_tracker)

    # -- lifecycle ------------------------------------------------------------

    def close(self) -> None:
        """Close the underlying cache (sqlite3 connection).

        Safe to call multiple times.  After closing, the next
        :meth:`search` or :meth:`build_index` call will lazily
        re-open the cache.
        """
        if self._cache_instance is not None:
            self._cache_instance.close()
            self._cache_instance = None

    def __enter__(self) -> DataSet:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- public API -----------------------------------------------------------

    def build_index(
        self,
        data: T.Iterable[dict[str, T.Any]] | None = None,
    ) -> int:
        """
        Build (or rebuild) the index with tracker lock protection.

        If *data* is ``None``, the :attr:`downloader` is called.  Raises
        ``ValueError`` if both are ``None``.

        :returns: Number of documents indexed.
        """
        if data is None:
            if self.downloader is None:
                raise ValueError("Either data or downloader must be provided")
            data = self.downloader()

        tracker = self._get_tracker()
        cache = self._cache
        with tracker.lock(self.name, expire=self.lock_expire):
            cache.evict_all()
            index = self._open_index()
            count = write_documents(
                index,
                data,
                memory_budget_bytes=self.memory_budget_bytes,
                num_threads=self.num_threads,
            )
            cache.mark_fresh()
        return count

    def search(
        self,
        query: str,
        limit: int = 20,
        refresh: bool = False,
    ) -> SearchResult:
        """
        Full search flow:

        1. Check L1 freshness (or ``refresh=True`` forces rebuild).
        2. If stale, call :meth:`build_index` with :attr:`downloader`.
        3. Check L2 query cache.
        4. On cache miss, execute the query, apply sorting, cache the result.

        :param query: Query string.
        :param limit: Maximum number of hits.
        :param refresh: Force a data refresh even if the cache is fresh.
        """
        t0 = time.monotonic()
        cache = self._cache
        fresh = False

        # Step 1-2: ensure data is fresh
        if refresh or not cache.is_fresh():
            if self.downloader is None:
                raise ValueError("Data is stale but no downloader is configured")
            self.build_index()
            fresh = True

        # Step 3: check query cache
        cached = cache.get_query_result(query, limit)
        if cached is not None:
            return cached

        # Step 4: execute query
        index = self._open_index()
        if self.sort:
            hits = search_index_sorted(
                index,
                self.fields,
                query,
                sort_keys=self.sort,
                limit=limit,
            )
        else:
            hits = search_index(index, self.fields, query, limit=limit)

        took_ms = int((time.monotonic() - t0) * 1000)
        response = SearchResult(
            hits=hits,
            size=len(hits),
            took_ms=took_ms,
            fresh=fresh,
            cache=False,
        )

        # cache with cache=True so subsequent reads get the cached flag
        cached_response = SearchResult(
            hits=hits,
            size=len(hits),
            took_ms=took_ms,
            fresh=fresh,
            cache=True,
        )
        cache.set_query_result(query, limit, cached_response)
        return response
