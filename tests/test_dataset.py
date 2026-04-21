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
from sayt2.exc import TrackerIsLockedError
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
    Hit,
    SearchResponse,
    DataSet,
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
        titles = [h.source["title"] for h in hits]
        assert any("Python" in t for t in titles)

    def test_hit_has_score(self, indexed):
        hits = search_index(indexed, SAMPLE_FIELDS, "python")
        assert all(isinstance(h, Hit) for h in hits)
        assert all(isinstance(h.score, float) for h in hits)

    def test_hit_contains_stored_fields(self, indexed):
        hits = search_index(indexed, SAMPLE_FIELDS, "python")
        hit = hits[0]
        assert "id" in hit.source
        assert "title" in hit.source
        assert "body" in hit.source

    def test_ngram_substring_search(self, indexed):
        """Ngram field allows matching partial words like 'pyth'."""
        hits = search_index(indexed, SAMPLE_FIELDS, "pyth")
        assert len(hits) >= 1
        assert any("Python" in h.source["title"] for h in hits)

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
        assert hits[0].source["id"] == "2"

    def test_multi_field_search(self, indexed):
        """Query matches across different field types."""
        # "rust" appears in title (ngram) and body (text)
        hits = search_index(indexed, SAMPLE_FIELDS, "rust")
        assert len(hits) >= 1
        assert any("Rust" in h.source["title"] for h in hits)


class TestRangeQuery:
    """Range queries via tantivy's query language on indexed numeric fields."""

    @pytest.fixture()
    def range_index(self, tmp_path):
        fields = [
            NgramField(name="title", min_gram=2, max_gram=6),
            KeywordField(name="id"),
            NumericField(name="year", kind="i64", indexed=True, fast=True),
            NumericField(name="price", kind="f64", indexed=True, fast=True),
        ]
        docs = [
            {"id": "1", "title": "Python Basics", "year": 2018, "price": 19.99},
            {"id": "2", "title": "Python Advanced", "year": 2020, "price": 39.99},
            {"id": "3", "title": "Python Cookbook", "year": 2022, "price": 49.99},
            {"id": "4", "title": "Rust Handbook", "year": 2023, "price": 59.99},
            {"id": "5", "title": "Rust Systems", "year": 2025, "price": 29.99},
        ]
        index = open_index(tmp_path / "index", fields)
        write_documents(index, docs)
        return index, fields

    def test_inclusive_range(self, range_index):
        """year:[2020 TO 2023] should match docs with year 2020, 2022, 2023."""
        index, fields = range_index
        hits = search_index(index, fields, "year:[2020 TO 2023]")
        assert len(hits) == 3
        years = {h.source["year"] for h in hits}
        assert years == {2020, 2022, 2023}

    def test_gt_operator(self, range_index):
        """year:>2022 should match 2023 and 2025."""
        index, fields = range_index
        hits = search_index(index, fields, "year:>2022")
        assert len(hits) == 2
        years = {h.source["year"] for h in hits}
        assert years == {2023, 2025}

    def test_lt_operator(self, range_index):
        """year:<2020 should match only 2018."""
        index, fields = range_index
        hits = search_index(index, fields, "year:<2020")
        assert len(hits) == 1
        assert hits[0].source["year"] == 2018

    def test_float_range(self, range_index):
        """price:[20 TO 50] should match 29.99, 39.99, 49.99."""
        index, fields = range_index
        hits = search_index(index, fields, "price:[20 TO 50]")
        assert len(hits) == 3
        prices = {h.source["price"] for h in hits}
        assert prices == {29.99, 39.99, 49.99}

    def test_range_and_text_combined(self, range_index):
        """python AND year:[2020 TO 2025] should match only Python books after 2020."""
        index, fields = range_index
        hits = search_index(index, fields, "python AND year:[2020 TO 2025]")
        assert len(hits) >= 1
        for h in hits:
            assert "Python" in h.source["title"]
            assert 2020 <= h.source["year"] <= 2025

    def test_range_no_results(self, range_index):
        """year:[2030 TO 2040] should return nothing."""
        index, fields = range_index
        hits = search_index(index, fields, "year:[2030 TO 2040]")
        assert hits == []

    def test_fast_field_range_still_works(self, tmp_path):
        """Range query works on fast=True fields even when indexed=False."""
        fields = [
            TextField(name="title"),
            NumericField(name="year", kind="i64", indexed=False, fast=True),
        ]
        index = open_index(tmp_path / "index", fields)
        write_documents(index, [{"title": "Test", "year": 2024}])
        hits = search_index(index, fields, "year:[2020 TO 2025]")
        assert len(hits) == 1


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
        assert any("Python" in h.source["title"] for h in hits)

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
        assert hits[0].source["id"] == "2"

    def test_limit_respected(self, fuzzy_index):
        index, fields = fuzzy_index
        hits = fuzzy_search_index(index, fields, "python", limit=1)
        assert len(hits) <= 1

    def test_multi_word_query(self, fuzzy_index):
        """'pythn tutoral' should match 'Python Tutorial' (two fuzzy terms)."""
        index, fields = fuzzy_index
        hits = fuzzy_search_index(index, fields, "pythn tutoral", distance=1)
        assert len(hits) >= 1
        assert any("Python" in h.source["title"] for h in hits)

    def test_empty_query(self, fuzzy_index):
        index, fields = fuzzy_index
        hits = fuzzy_search_index(index, fields, "")
        assert hits == []


class TestSortHits:
    """Step 5.4: _sort_hits pure function."""

    HITS = [
        Hit(source={"title": "A", "year": 2020, "rating": 4.5}, score=1.0),
        Hit(source={"title": "B", "year": 2025, "rating": 4.9}, score=2.0),
        Hit(source={"title": "C", "year": 2025, "rating": 4.3}, score=3.0),
        Hit(source={"title": "D", "year": 2022, "rating": 4.5}, score=4.0),
    ]

    def test_single_field_desc(self):
        result = _sort_hits(list(self.HITS), [SortKey(name="year")], limit=10)
        years = [h.source["year"] for h in result]
        assert years == [2025, 2025, 2022, 2020]

    def test_single_field_asc(self):
        result = _sort_hits(
            list(self.HITS), [SortKey(name="year", descending=False)], limit=10
        )
        years = [h.source["year"] for h in result]
        assert years == [2020, 2022, 2025, 2025]

    def test_multi_field_sort(self):
        """Primary: year DESC, secondary: rating DESC."""
        result = _sort_hits(
            list(self.HITS),
            [SortKey(name="year"), SortKey(name="rating")],
            limit=10,
        )
        assert [(h.source["year"], h.source["rating"]) for h in result] == [
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
        assert [(h.source["year"], h.source["rating"]) for h in result] == [
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

    def test_does_not_mutate_input(self):
        original = list(self.HITS)
        original_copy = list(original)
        _sort_hits(original, [SortKey(name="year")], limit=10)
        assert original == original_copy

    def test_missing_sort_field_defaults_to_zero(self):
        hits = [
            Hit(source={"title": "A", "year": 2025}, score=1.0),
            Hit(source={"title": "B"}, score=2.0),  # no "year"
        ]
        result = _sort_hits(hits, [SortKey(name="year")], limit=10)
        # missing year → 0, so it sorts last in DESC
        assert result[0].source["title"] == "A"
        assert result[1].source["title"] == "B"


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
        years = [h.source["year"] for h in hits]
        assert years == sorted(years, reverse=True)

    def test_sort_by_year_asc(self, sorted_index):
        index, fields = sorted_index
        hits = search_index_sorted(
            index, fields, "python",
            sort_keys=[SortKey(name="year", descending=False)],
        )
        years = [h.source["year"] for h in hits]
        assert years == sorted(years)

    def test_multi_field_sort(self, sorted_index):
        """year DESC then rating DESC."""
        index, fields = sorted_index
        hits = search_index_sorted(
            index, fields, "python",
            sort_keys=[SortKey(name="year"), SortKey(name="rating")],
        )
        pairs = [(h.source["year"], h.source["rating"]) for h in hits]
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


class TestSearchResponse:
    """Step 5.6: SearchResponse immutable model."""

    def test_creation(self):
        r = SearchResponse(
            hits=[Hit(source={"title": "A"}, score=1.0)],
            size=1,
            took_ms=5,
            fresh=True,
            cache=False,
        )
        assert r.size == 1
        assert r.fresh is True
        assert r.cache is False

    def test_frozen(self):
        r = SearchResponse(hits=[], size=0, took_ms=0, fresh=False, cache=False)
        with pytest.raises(Exception):
            r.size = 99  # type: ignore[misc]


class TestDataSet:
    """Step 5.5: DataSet integration with cache + tracker."""

    FIELDS = [
        NgramField(name="title", min_gram=2, max_gram=6),
        TextField(name="body"),
        KeywordField(name="id"),
        NumericField(name="year", kind="i64", indexed=False, fast=True),
    ]

    DOCS = [
        {"id": "1", "title": "Python Tutorial", "body": "Learn Python programming", "year": 2024},
        {"id": "2", "title": "FastAPI Guide", "body": "Build APIs with FastAPI", "year": 2023},
        {"id": "3", "title": "Rust Handbook", "body": "Systems programming with Rust", "year": 2025},
    ]

    def _make_ds(self, tmp_path, **kwargs):
        defaults = dict(
            dir_root=tmp_path,
            name="books",
            fields=self.FIELDS,
        )
        defaults.update(kwargs)
        return DataSet(**defaults)

    # -- build_index ----------------------------------------------------------

    def test_build_index_with_data(self, tmp_path):
        ds = self._make_ds(tmp_path)
        count = ds.build_index(data=self.DOCS)
        assert count == 3

    def test_build_index_with_downloader(self, tmp_path):
        ds = self._make_ds(tmp_path, downloader=lambda: self.DOCS)
        count = ds.build_index()
        assert count == 3

    def test_build_index_no_data_no_downloader(self, tmp_path):
        ds = self._make_ds(tmp_path)
        with pytest.raises(ValueError, match="data or downloader"):
            ds.build_index()

    # -- search ---------------------------------------------------------------

    def test_first_search_triggers_download(self, tmp_path):
        call_count = {"n": 0}

        def downloader():
            call_count["n"] += 1
            return self.DOCS

        ds = self._make_ds(tmp_path, downloader=downloader)
        result = ds.search("python")
        assert isinstance(result, SearchResponse)
        assert result.fresh is True
        assert result.cache is False
        assert result.size >= 1
        assert call_count["n"] == 1

    def test_second_search_hits_cache(self, tmp_path):
        ds = self._make_ds(tmp_path, downloader=lambda: self.DOCS)
        r1 = ds.search("python")
        assert r1.cache is False

        r2 = ds.search("python")
        assert r2.cache is True
        assert r2.size == r1.size

    def test_different_query_misses_cache(self, tmp_path):
        ds = self._make_ds(tmp_path, downloader=lambda: self.DOCS)
        ds.search("python")
        r2 = ds.search("rust")
        assert r2.cache is False

    def test_refresh_forces_rebuild(self, tmp_path):
        call_count = {"n": 0}

        def downloader():
            call_count["n"] += 1
            return self.DOCS

        ds = self._make_ds(tmp_path, downloader=downloader)
        ds.search("python")
        assert call_count["n"] == 1

        ds.search("python", refresh=True)
        assert call_count["n"] == 2

    def test_stale_data_no_downloader_raises(self, tmp_path):
        ds = self._make_ds(tmp_path)
        with pytest.raises(ValueError, match="no downloader"):
            ds.search("python")

    def test_search_response_has_took_ms(self, tmp_path):
        ds = self._make_ds(tmp_path, downloader=lambda: self.DOCS)
        r = ds.search("python")
        assert r.took_ms >= 0

    def test_search_with_limit(self, tmp_path):
        ds = self._make_ds(tmp_path, downloader=lambda: self.DOCS)
        r = ds.search("python", limit=1)
        assert r.size <= 1

    # -- sort integration -----------------------------------------------------

    def test_search_with_sort(self, tmp_path):
        ds = self._make_ds(
            tmp_path,
            downloader=lambda: self.DOCS,
            sort=[SortKey(name="year")],
        )
        r = ds.search("python")
        if r.size >= 2:
            years = [h.source["year"] for h in r.hits]
            assert years == sorted(years, reverse=True)

    # -- tracker integration --------------------------------------------------

    def test_concurrent_build_raises(self, tmp_path):
        ds = self._make_ds(tmp_path, lock_expire=10)
        tracker = ds._get_tracker()
        # hold the lock externally
        token = tracker.lock_it(ds.name, expire=10)
        try:
            with pytest.raises(TrackerIsLockedError):
                ds.build_index(data=self.DOCS)
        finally:
            tracker.unlock_it(ds.name, token)

    # -- cache + schema hash --------------------------------------------------

    def test_schema_change_invalidates_cache(self, tmp_path):
        ds1 = self._make_ds(tmp_path, downloader=lambda: self.DOCS)
        r1 = ds1.search("python")
        assert r1.cache is False

        # same query, same dataset — should hit cache
        r2 = ds1.search("python")
        assert r2.cache is True

        # change fields → new schema hash → cache miss
        new_fields = list(self.FIELDS) + [TextField(name="extra")]
        new_docs = [dict(d, extra="x") for d in self.DOCS]
        ds2 = self._make_ds(
            tmp_path,
            fields=new_fields,
            downloader=lambda: new_docs,
        )
        r3 = ds2.search("python")
        assert r3.fresh is True
        assert r3.cache is False


if __name__ == "__main__":
    from sayt2.tests import run_cov_test

    run_cov_test(
        __file__,
        "sayt2.dataset",
        preview=False,
    )
