# -*- coding: utf-8 -*-

import pytest
from pathlib import Path

import tantivy

from sayt2.fields import (
    StoredField,
    KeywordField,
    TextField,
    NgramField,
    NumericField,
    DatetimeField,
    BooleanField,
)
from sayt2.dataset import (
    build_schema,
    open_index,
    write_documents,
    search_index,
    _collect_search_config,
)


SAMPLE_FIELDS = [
    KeywordField(name="id"),
    NgramField(name="title", min_gram=2, max_gram=6),
    TextField(name="body"),
    NumericField(name="year", kind="i64", indexed=False, fast=True),
    NumericField(name="rating", kind="f64", indexed=False, fast=True),
]

SAMPLE_DOCS = [
    {"id": "1", "title": "Python Tutorial", "body": "Learn Python programming", "year": 2024, "rating": 4.8},
    {"id": "2", "title": "FastAPI Guide", "body": "Build APIs with FastAPI", "year": 2023, "rating": 4.5},
    {"id": "3", "title": "Rust Handbook", "body": "Systems programming with Rust", "year": 2025, "rating": 4.9},
    {"id": "4", "title": "pandas Cookbook", "body": "Data analysis with pandas", "year": 2022, "rating": 4.3},
    {"id": "5", "title": "asyncio Deep Dive", "body": "Async Python patterns", "year": 2024, "rating": 4.6},
]


class TestBuildSchema:
    def test_basic_schema(self):
        schema, analyzers = build_schema(SAMPLE_FIELDS)
        assert isinstance(schema, tantivy.Schema)

    def test_ngram_analyzers_collected(self):
        _, analyzers = build_schema(SAMPLE_FIELDS)
        assert len(analyzers) == 1
        key = list(analyzers.keys())[0]
        assert key.startswith("__sayt2_ngram_")

    def test_multiple_ngram_fields_share_analyzer(self):
        fields = [
            NgramField(name="title", min_gram=2, max_gram=6),
            NgramField(name="author", min_gram=2, max_gram=6),
        ]
        _, analyzers = build_schema(fields)
        assert len(analyzers) == 1  # same config → same analyzer

    def test_different_ngram_configs_separate_analyzers(self):
        fields = [
            NgramField(name="title", min_gram=2, max_gram=6),
            NgramField(name="author", min_gram=3, max_gram=8),
        ]
        _, analyzers = build_schema(fields)
        assert len(analyzers) == 2

    def test_all_field_types(self):
        fields = [
            StoredField(name="raw_html"),
            KeywordField(name="id"),
            TextField(name="body"),
            TextField(name="body_stem", tokenizer="en_stem"),
            NgramField(name="title"),
            NumericField(name="year", kind="i64"),
            NumericField(name="rating", kind="f64"),
            NumericField(name="count", kind="u64"),
            DatetimeField(name="created"),
            BooleanField(name="active"),
        ]
        schema, analyzers = build_schema(fields)
        assert isinstance(schema, tantivy.Schema)


class TestOpenIndex:
    def test_creates_index_dir(self, tmp_path):
        dir_index = tmp_path / "index"
        assert not dir_index.exists()
        index = open_index(dir_index, SAMPLE_FIELDS)
        assert dir_index.exists()
        assert isinstance(index, tantivy.Index)

    def test_reopen_existing_index(self, tmp_path):
        dir_index = tmp_path / "index"
        index1 = open_index(dir_index, SAMPLE_FIELDS)
        write_documents(index1, SAMPLE_DOCS[:2])

        # reopen — should not error, tokenizers re-registered
        index2 = open_index(dir_index, SAMPLE_FIELDS)
        searcher = index2.searcher()
        assert searcher.num_docs == 2


class TestWriteDocuments:
    def test_write_and_count(self, tmp_path):
        index = open_index(tmp_path / "index", SAMPLE_FIELDS)
        count = write_documents(index, SAMPLE_DOCS)
        assert count == 5

    def test_documents_searchable_after_write(self, tmp_path):
        index = open_index(tmp_path / "index", SAMPLE_FIELDS)
        write_documents(index, SAMPLE_DOCS)

        searcher = index.searcher()
        assert searcher.num_docs == 5

    def test_empty_data(self, tmp_path):
        index = open_index(tmp_path / "index", SAMPLE_FIELDS)
        count = write_documents(index, [])
        assert count == 0

    def test_write_with_num_threads(self, tmp_path):
        index = open_index(tmp_path / "index", SAMPLE_FIELDS)
        count = write_documents(index, SAMPLE_DOCS, num_threads=1)
        assert count == 5

    def test_stored_fields_retrievable(self, tmp_path):
        index = open_index(tmp_path / "index", SAMPLE_FIELDS)
        write_documents(index, SAMPLE_DOCS)

        searcher = index.searcher()
        # search for something we know exists
        query = index.parse_query("python", ["body"])
        results = searcher.search(query, limit=10)
        assert len(results.hits) > 0

        # verify stored fields are accessible
        _score, addr = results.hits[0]
        doc = searcher.doc(addr)
        assert len(doc["id"]) > 0
        assert len(doc["title"]) > 0

    def test_ngram_search_works(self, tmp_path):
        """Ngram field enables substring search."""
        fields = [
            NgramField(name="title", min_gram=2, max_gram=6),
            KeywordField(name="id"),
        ]
        docs = [
            {"title": "Python Tutorial", "id": "1"},
            {"title": "FastAPI Guide", "id": "2"},
        ]
        index = open_index(tmp_path / "index", fields)
        write_documents(index, docs)

        searcher = index.searcher()
        # "pyth" is a 4-char substring of "Python" — ngram should match
        query = index.parse_query("pyth", ["title"])
        results = searcher.search(query, limit=10)
        assert len(results.hits) >= 1

        doc = searcher.doc(results.hits[0][1])
        assert "Python" in doc["title"][0]

    def test_numeric_fast_field(self, tmp_path):
        """Numeric fast fields support order_by_field."""
        fields = [
            TextField(name="title"),
            NumericField(name="year", kind="i64", indexed=False, fast=True),
        ]
        docs = [
            {"title": "Old Book", "year": 2020},
            {"title": "New Book", "year": 2025},
        ]
        index = open_index(tmp_path / "index", fields)
        write_documents(index, docs)

        searcher = index.searcher()
        query = index.parse_query("book", ["title"])
        results = searcher.search(query, limit=10, order_by_field="year")
        assert len(results.hits) == 2


class TestCollectSearchConfig:
    def test_collects_searchable_fields(self):
        names, boosts = _collect_search_config(SAMPLE_FIELDS)
        assert "id" in names      # KeywordField
        assert "title" in names   # NgramField
        assert "body" in names    # TextField
        assert "year" not in names   # NumericField — no boost
        assert "rating" not in names

    def test_default_boost_omitted(self):
        fields = [TextField(name="body"), NgramField(name="title")]
        _, boosts = _collect_search_config(fields)
        assert boosts == {}  # all 1.0 → empty dict

    def test_custom_boost_collected(self):
        fields = [
            TextField(name="body", boost=1.0),
            NgramField(name="title", boost=3.0),
            KeywordField(name="id", boost=0.5),
        ]
        names, boosts = _collect_search_config(fields)
        assert set(names) == {"body", "title", "id"}
        assert boosts == {"title": 3.0, "id": 0.5}

    def test_no_searchable_fields(self):
        fields = [StoredField(name="raw"), NumericField(name="year")]
        names, boosts = _collect_search_config(fields)
        assert names == []
        assert boosts == {}


class TestSearchIndex:
    @pytest.fixture()
    def indexed(self, tmp_path):
        index = open_index(tmp_path / "index", SAMPLE_FIELDS)
        write_documents(index, SAMPLE_DOCS)
        return index

    def test_basic_text_search(self, indexed):
        hits = search_index(indexed, SAMPLE_FIELDS, "python")
        assert len(hits) >= 1
        titles = [h["title"] for h in hits]
        assert any("Python" in t for t in titles)

    def test_hit_has_score(self, indexed):
        hits = search_index(indexed, SAMPLE_FIELDS, "python")
        assert all("_score" in h for h in hits)
        assert all(isinstance(h["_score"], float) for h in hits)

    def test_hit_contains_stored_fields(self, indexed):
        hits = search_index(indexed, SAMPLE_FIELDS, "python")
        hit = hits[0]
        assert "id" in hit
        assert "title" in hit
        assert "body" in hit

    def test_ngram_substring_search(self, indexed):
        """Ngram field allows matching partial words like 'pyth'."""
        hits = search_index(indexed, SAMPLE_FIELDS, "pyth")
        assert len(hits) >= 1
        assert any("Python" in h["title"] for h in hits)

    def test_limit_respected(self, indexed):
        hits = search_index(indexed, SAMPLE_FIELDS, "python", limit=1)
        assert len(hits) <= 1

    def test_no_results(self, indexed):
        hits = search_index(indexed, SAMPLE_FIELDS, "xyznonexistent")
        assert hits == []

    def test_no_searchable_fields_returns_empty(self, tmp_path):
        fields = [NumericField(name="year")]
        index = open_index(tmp_path / "index", fields)
        write_documents(index, [{"year": 2024}])
        hits = search_index(index, fields, "2024")
        assert hits == []

    def test_field_boost_affects_ranking(self, tmp_path):
        """Higher-boosted field should rank its matches higher."""
        fields = [
            NgramField(name="title", boost=5.0),
            TextField(name="body", boost=1.0),
            KeywordField(name="id"),
        ]
        docs = [
            {"id": "1", "title": "Rust guide", "body": "Learn Python programming"},
            {"id": "2", "title": "Python guide", "body": "Learn Rust programming"},
        ]
        index = open_index(tmp_path / "index", fields)
        write_documents(index, docs)

        hits = search_index(index, fields, "python")
        assert len(hits) == 2
        # doc with "Python" in title (boost=5.0) should rank first
        assert hits[0]["id"] == "2"

    def test_multi_field_search(self, indexed):
        """Query matches across different field types."""
        # "rust" appears in title (ngram) and body (text)
        hits = search_index(indexed, SAMPLE_FIELDS, "rust")
        assert len(hits) >= 1
        assert any("Rust" in h["title"] for h in hits)


if __name__ == "__main__":
    from sayt2.tests import run_cov_test

    run_cov_test(
        __file__,
        "sayt2.dataset",
        preview=False,
    )
