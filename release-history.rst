.. _release_history:

Release and Version History
==============================================================================


x.y.z (Backlog)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

**Minor Improvements**

**Bugfixes**

**Miscellaneous**


0.1.1 (2026-04-20)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- First release, a ground-up rewrite of `sayt <https://github.com/MacHu-GWU/sayt-project>`_ with a modern architecture.
- Migrated from Whoosh to `tantivy <https://github.com/quickwit-oss/tantivy-py>`_ as the underlying search engine for significantly faster indexing and querying.
- Replaced JSON file-based locking with SQLite atomic UPSERT for reliable cross-process coordination.
- Introduced a two-layer disk cache (data freshness + query results) with automatic schema-aware invalidation.
- Added pydantic-based configuration and field type validation with IDE autocompletion support.
- Add the following public API:
    - ``sayt2.api.MalformedFieldSettingError``
    - ``sayt2.api.MalformedDatasetSettingError``
    - ``sayt2.api.TrackerIsLockedError``
    - ``sayt2.api.BaseField``
    - ``sayt2.api.StoredField``
    - ``sayt2.api.KeywordField``
    - ``sayt2.api.TextField``
    - ``sayt2.api.NgramField``
    - ``sayt2.api.NumericField``
    - ``sayt2.api.DatetimeField``
    - ``sayt2.api.BooleanField``
    - ``sayt2.api.T_Field``
    - ``sayt2.api.fields_schema_hash``
    - ``sayt2.api.Tracker``
    - ``sayt2.api.DataSetCache``
    - ``sayt2.api.DataSet``
    - ``sayt2.api.SortKey``
    - ``sayt2.api.Hit``
    - ``sayt2.api.SearchResult``
