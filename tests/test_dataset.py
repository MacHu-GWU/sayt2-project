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
from sayt2.dataset import build_schema, open_index, write_documents


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


if __name__ == "__main__":
    from sayt2.tests import run_cov_test

    run_cov_test(
        __file__,
        "sayt2.dataset",
        preview=False,
    )
