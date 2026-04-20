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

from pathlib import Path
from typing import Any, Iterable, Literal

import tantivy
from pydantic import BaseModel
from tantivy import (
    Filter,
    Index,
    SchemaBuilder,
    TextAnalyzerBuilder,
    Tokenizer,
)

from .fields import (
    T_Field,
    BaseField,
    StoredField,
    KeywordField,
    TextField,
    NgramField,
    NumericField,
    DatetimeField,
    BooleanField,
)


class SortKey(BaseModel):
    """One element of a multi-field sort specification."""

    name: str
    descending: bool = True


def _ngram_tokenizer_name(f: NgramField) -> str:
    """Deterministic name for a custom ngram tokenizer so it can be re-registered."""
    return f"__sayt2_ngram_{f.min_gram}_{f.max_gram}_{int(f.prefix_only)}_{int(f.lowercase)}"


def _build_ngram_analyzer(f: NgramField) -> tantivy.TextAnalyzer:
    builder = TextAnalyzerBuilder(
        Tokenizer.ngram(min_gram=f.min_gram, max_gram=f.max_gram, prefix_only=f.prefix_only)
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
                "i64": sb.add_integer_field,
                "u64": sb.add_unsigned_field,
                "f64": sb.add_float_field,
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
    data: Iterable[dict[str, Any]],
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
    writer_kwargs: dict[str, Any] = {"heap_size": memory_budget_bytes}
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
) -> list[dict[str, Any]]:
    """Materialise search results into a list of hit dicts."""
    hits: list[dict[str, Any]] = []
    for score, addr in results.hits:
        doc = searcher.doc(addr)
        hit: dict[str, Any] = {"_score": score}
        for name in stored_names:
            values = doc[name]
            if values:
                hit[name] = values[0] if len(values) == 1 else values
        hits.append(hit)
    return hits


def search_index(
    index: Index,
    fields: list[BaseField],
    query_str: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
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

    kwargs: dict[str, Any] = {}
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
) -> list[dict[str, Any]]:
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

    sub_queries = []
    for f in fuzzy_fields:
        q = tantivy.Query.fuzzy_term_query(
            schema,
            f.name,
            query_str,
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
    hits: list[dict[str, Any]],
    sort_keys: list[SortKey],
    limit: int,
) -> list[dict[str, Any]]:
    """
    Sort *hits* by multiple fields (lexicographic) and return the top *limit*.

    Each ``SortKey`` specifies a field name and direction.  Python's stable
    sort with a tuple key handles mixed ascending/descending via negation
    for numeric values and ``reverse=True`` would not work for mixed
    directions, so we sort in successive passes (least-significant key first).
    """
    # Sort by successive keys, least significant first (stable sort preserves
    # earlier ordering for ties).
    for sk in reversed(sort_keys):
        hits.sort(
            key=lambda h, _name=sk.name: h.get(_name, 0),
            reverse=sk.descending,
        )
    return hits[:limit]


def search_index_sorted(
    index: Index,
    fields: list[BaseField],
    query_str: str,
    sort_keys: list[SortKey],
    limit: int = 20,
    over_fetch_factor: int = 10,
) -> list[dict[str, Any]]:
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
