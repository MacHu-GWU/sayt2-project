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
    fuzzy_search_index,
    search_index_sorted,
    _collect_search_config,
    _sort_hits,
    SortKey,
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


class TestFuzzySearchIndex:
    """Step 5.3: Fuzzy query using Query.fuzzy_term_query."""

    @pytest.fixture()
    def fuzzy_index(self, tmp_path):
        fields = [
            TextField(name="title"),
            TextField(name="body"),
            KeywordField(name="id"),
        ]
        docs = [
            {"id": "1", "title": "Python Tutorial", "body": "Learn Python programming"},
            {"id": "2", "title": "FastAPI Guide", "body": "Build APIs with FastAPI"},
            {"id": "3", "title": "Rust Handbook", "body": "Systems programming with Rust"},
            {"id": "4", "title": "pandas Cookbook", "body": "Data analysis with pandas"},
        ]
        index = open_index(tmp_path / "index", fields)
        write_documents(index, docs)
        return index, fields

    def test_typo_tolerance(self, fuzzy_index):
        """'pythn' (missing 'o') should match 'python'."""
        index, fields = fuzzy_index
        hits = fuzzy_search_index(index, fields, "pythn", distance=1)
        assert len(hits) >= 1
        assert any("Python" in h["title"] for h in hits)

    def test_transposition(self, fuzzy_index):
        """'pyhton' (transposed 'th') should match 'python'."""
        index, fields = fuzzy_index
        hits = fuzzy_search_index(index, fields, "pyhton", distance=1)
        assert len(hits) >= 1

    def test_distance_2(self, fuzzy_index):
        """Two-char edit: 'pythn' → 'python' (distance=2 should also work)."""
        index, fields = fuzzy_index
        hits = fuzzy_search_index(index, fields, "pythn", distance=2)
        assert len(hits) >= 1

    def test_exact_match_still_works(self, fuzzy_index):
        index, fields = fuzzy_index
        hits = fuzzy_search_index(index, fields, "python")
        assert len(hits) >= 1

    def test_no_match(self, fuzzy_index):
        index, fields = fuzzy_index
        hits = fuzzy_search_index(index, fields, "xyzxyz")
        assert hits == []

    def test_no_text_fields_returns_empty(self, tmp_path):
        """Fuzzy only works on TextField; NgramField/KeywordField are skipped."""
        fields = [NgramField(name="title"), KeywordField(name="id")]
        index = open_index(tmp_path / "index", fields)
        write_documents(index, [{"title": "Python", "id": "1"}])
        hits = fuzzy_search_index(index, fields, "pythn")
        assert hits == []

    def test_multi_field_fuzzy(self, fuzzy_index):
        """Fuzzy queries across multiple TextFields are OR-combined."""
        index, fields = fuzzy_index
        # "programing" (one 'm') should match "programming" in body
        hits = fuzzy_search_index(index, fields, "programing", distance=1)
        assert len(hits) >= 1

    def test_boost_affects_ranking(self, tmp_path):
        """Higher-boosted TextField should rank its matches higher."""
        fields = [
            TextField(name="title", boost=5.0),
            TextField(name="body", boost=1.0),
            KeywordField(name="id"),
        ]
        docs = [
            {"id": "1", "title": "Rust guide", "body": "Learn python basics"},
            {"id": "2", "title": "python guide", "body": "Learn rust basics"},
        ]
        index = open_index(tmp_path / "index", fields)
        write_documents(index, docs)
        hits = fuzzy_search_index(index, fields, "python")
        assert len(hits) == 2
        assert hits[0]["id"] == "2"

    def test_limit_respected(self, fuzzy_index):
        index, fields = fuzzy_index
        hits = fuzzy_search_index(index, fields, "python", limit=1)
        assert len(hits) <= 1


class TestSortHits:
    """Step 5.4: _sort_hits pure function."""

    HITS = [
        {"title": "A", "year": 2020, "rating": 4.5, "_score": 1.0},
        {"title": "B", "year": 2025, "rating": 4.9, "_score": 2.0},
        {"title": "C", "year": 2025, "rating": 4.3, "_score": 3.0},
        {"title": "D", "year": 2022, "rating": 4.5, "_score": 4.0},
    ]

    def test_single_field_desc(self):
        result = _sort_hits(list(self.HITS), [SortKey(name="year")], limit=10)
        years = [h["year"] for h in result]
        assert years == [2025, 2025, 2022, 2020]

    def test_single_field_asc(self):
        result = _sort_hits(
            list(self.HITS), [SortKey(name="year", descending=False)], limit=10
        )
        years = [h["year"] for h in result]
        assert years == [2020, 2022, 2025, 2025]

    def test_multi_field_sort(self):
        """Primary: year DESC, secondary: rating DESC."""
        result = _sort_hits(
            list(self.HITS),
            [SortKey(name="year"), SortKey(name="rating")],
            limit=10,
        )
        assert [(h["year"], h["rating"]) for h in result] == [
            (2025, 4.9),
            (2025, 4.3),
            (2022, 4.5),
            (2020, 4.5),
        ]

    def test_mixed_directions(self):
        """Primary: year ASC, secondary: rating DESC."""
        result = _sort_hits(
            list(self.HITS),
            [SortKey(name="year", descending=False), SortKey(name="rating")],
            limit=10,
        )
        assert [(h["year"], h["rating"]) for h in result] == [
            (2020, 4.5),
            (2022, 4.5),
            (2025, 4.9),
            (2025, 4.3),
        ]

    def test_limit_truncates(self):
        result = _sort_hits(list(self.HITS), [SortKey(name="year")], limit=2)
        assert len(result) == 2

    def test_empty_hits(self):
        result = _sort_hits([], [SortKey(name="year")], limit=10)
        assert result == []


class TestSearchIndexSorted:
    """Step 5.4: search_index_sorted end-to-end."""

    @pytest.fixture()
    def sorted_index(self, tmp_path):
        fields = [
            NgramField(name="title", min_gram=2, max_gram=6),
            TextField(name="body"),
            NumericField(name="year", kind="i64", indexed=False, fast=True),
            NumericField(name="rating", kind="f64", indexed=False, fast=True),
            KeywordField(name="id"),
        ]
        docs = [
            {"id": "1", "title": "Python Tutorial", "body": "Learn Python", "year": 2020, "rating": 4.5},
            {"id": "2", "title": "Python Guide", "body": "Advanced Python", "year": 2025, "rating": 4.9},
            {"id": "3", "title": "Python Cookbook", "body": "Python recipes", "year": 2025, "rating": 4.3},
            {"id": "4", "title": "Python Deep Dive", "body": "Python internals", "year": 2022, "rating": 4.8},
        ]
        index = open_index(tmp_path / "index", fields)
        write_documents(index, docs)
        return index, fields

    def test_sort_by_year_desc(self, sorted_index):
        index, fields = sorted_index
        hits = search_index_sorted(
            index, fields, "python",
            sort_keys=[SortKey(name="year")],
        )
        years = [h["year"] for h in hits]
        assert years == sorted(years, reverse=True)

    def test_sort_by_year_asc(self, sorted_index):
        index, fields = sorted_index
        hits = search_index_sorted(
            index, fields, "python",
            sort_keys=[SortKey(name="year", descending=False)],
        )
        years = [h["year"] for h in hits]
        assert years == sorted(years)

    def test_multi_field_sort(self, sorted_index):
        """year DESC then rating DESC."""
        index, fields = sorted_index
        hits = search_index_sorted(
            index, fields, "python",
            sort_keys=[SortKey(name="year"), SortKey(name="rating")],
        )
        pairs = [(h["year"], h["rating"]) for h in hits]
        # 2025 group first (desc), within 2025: 4.9 > 4.3
        assert pairs[0] == (2025, 4.9)
        assert pairs[1] == (2025, 4.3)

    def test_limit_respected(self, sorted_index):
        index, fields = sorted_index
        hits = search_index_sorted(
            index, fields, "python",
            sort_keys=[SortKey(name="year")],
            limit=2,
        )
        assert len(hits) == 2

    def test_sortkey_model(self):
        sk = SortKey(name="year", descending=True)
        assert sk.name == "year"
        assert sk.descending is True

        sk2 = SortKey(name="rating")
        assert sk2.descending is True  # default


if __name__ == "__main__":
    from sayt2.tests import run_cov_test

    run_cov_test(
        __file__,
        "sayt2.dataset",
        preview=False,
    )
