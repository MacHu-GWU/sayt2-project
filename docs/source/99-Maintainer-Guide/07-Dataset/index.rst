Dataset — Core Search Engine
==============================================================================
:mod:`sayt2.dataset` is the **only module that imports tantivy**.  It
integrates field definitions, caching, and cross-process locking into a single
high-level :class:`~sayt2.dataset.DataSet` object that handles the full
index-build-search lifecycle.


Module-level functions
------------------------------------------------------------------------------
The module exposes a set of stateless functions that operate on a tantivy
``Index`` directly.  :class:`~sayt2.dataset.DataSet` composes them internally,
but they are also usable standalone for advanced use cases.


Schema construction
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../../../sayt2/dataset.py
   :language: python
   :pyobject: build_schema

:func:`~sayt2.dataset.build_schema` walks the field list and maps each
:class:`~sayt2.fields.BaseField` subclass to the corresponding tantivy
``SchemaBuilder`` call.  For :class:`~sayt2.fields.NgramField`, it also
creates a custom ``TextAnalyzer`` via the helper below:

.. literalinclude:: ../../../../sayt2/dataset.py
   :language: python
   :pyobject: _ngram_tokenizer_name

.. literalinclude:: ../../../../sayt2/dataset.py
   :language: python
   :pyobject: _build_ngram_analyzer

Each :class:`~sayt2.fields.NgramField` gets a deterministic tokenizer name
derived from its parameters.  This ensures that two fields with different gram
ranges get separate tokenizers while identical configurations share one.


Index opening
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../../../sayt2/dataset.py
   :language: python
   :pyobject: open_index

tantivy does **not** persist tokenizer configuration — only the inverted index
data.  Every ``Index`` open must be followed by ``register_tokenizer`` calls,
which :func:`~sayt2.dataset.open_index` handles automatically.


Document writing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../../../sayt2/dataset.py
   :language: python
   :pyobject: write_documents

Documents are written through tantivy's ``IndexWriter``.  After committing,
the writer waits for background merge threads to finish, then reloads the index
so that subsequent searches see the new data.


Query execution
------------------------------------------------------------------------------

Basic search
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../../../sayt2/dataset.py
   :language: python
   :pyobject: search_index

:func:`~sayt2.dataset.search_index` uses ``index.parse_query`` with automatic
field boosts.  Only fields that have a ``boost`` attribute
(:class:`~sayt2.fields.KeywordField`, :class:`~sayt2.fields.TextField`,
:class:`~sayt2.fields.NgramField`) are included in the query.

The helper :func:`~sayt2.dataset._collect_search_config` extracts searchable
field names and non-default boosts:

.. literalinclude:: ../../../../sayt2/dataset.py
   :language: python
   :pyobject: _collect_search_config


Fuzzy search
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../../../sayt2/dataset.py
   :language: python
   :pyobject: fuzzy_search_index

Fuzzy matching uses ``Query.fuzzy_term_query`` (not ``parse_query``'s ``~N``
syntax, which does not work in tantivy-py).  It operates only on
:class:`~sayt2.fields.TextField` — ngram and keyword fields are excluded.
Multiple query terms are split by whitespace and combined with
``Occur.Should`` (boolean OR).


Sorted search
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
tantivy-py only exposes single-field ``order_by_field``, so multi-field sorting
is done in Python after over-fetching.

.. literalinclude:: ../../../../sayt2/dataset.py
   :language: python
   :pyobject: search_index_sorted

.. literalinclude:: ../../../../sayt2/dataset.py
   :language: python
   :pyobject: _sort_hits

The strategy: fetch ``limit * over_fetch_factor`` candidates using BM25
scoring, then re-sort in Python using successive stable sorts (least-significant
key first).  The default ``over_fetch_factor=10`` balances accuracy and
performance.


Data model
------------------------------------------------------------------------------

SortKey
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../../../sayt2/dataset.py
   :language: python
   :pyobject: SortKey

:class:`~sayt2.dataset.SortKey` is a simple pydantic model specifying a field
``name`` and sort ``direction``.  Pass a list of these to
:attr:`~sayt2.dataset.DataSet.sort` for multi-field sorting.


Hit
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../../../sayt2/dataset.py
   :language: python
   :pyobject: Hit

:class:`~sayt2.dataset.Hit` is a **frozen dataclass** representing a single
search result.  Key fields:

- ``source`` — dict of stored document fields (modelled after Elasticsearch's
  ``_source``).
- ``score`` — BM25 relevance score.


SearchResult
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../../../sayt2/dataset.py
   :language: python
   :pyobject: SearchResult

:class:`~sayt2.dataset.SearchResult` is a **frozen dataclass** — immutable
after creation.  Key fields:

- ``hits`` — list of :class:`~sayt2.dataset.Hit` objects.
- ``size`` — number of hits returned.
- ``took_ms`` — wall-clock time for the full search flow.
- ``fresh`` — ``True`` if this search triggered a data refresh.
- ``cache`` — ``True`` if the result was served from L2 cache.


DataSet class
------------------------------------------------------------------------------

.. literalinclude:: ../../../../sayt2/dataset.py
   :language: python
   :pyobject: DataSet

:class:`~sayt2.dataset.DataSet` is the primary user-facing class.  It
orchestrates three subsystems:

- :class:`~sayt2.tracker.Tracker` — ensures only one process rebuilds the index
  at a time.
- :class:`~sayt2.cache.DataSetCache` — avoids redundant rebuilds and repeated
  queries.
- tantivy ``Index`` — the actual search engine.

All state (index files, cache, tracker DB) lives under ``dir_root``:

.. code-block:: text

   dir_root/
   ├── tracker.db                    ← shared across datasets
   └── {name}/
       ├── index-{schema_hash}/      ← tantivy index files
       └── cache/                    ← diskcache files


build_index
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
:meth:`~sayt2.dataset.DataSet.build_index` acquires a tracker lock, evicts all
caches, writes documents, and marks the data as fresh:

.. code-block:: text

   lock(name) → evict_all() → open_index() → write_documents() → mark_fresh() → unlock

If ``data`` is ``None``, the :attr:`~sayt2.dataset.DataSet.downloader` callable
is invoked to fetch data.


search
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
:meth:`~sayt2.dataset.DataSet.search` implements the full search flow:

.. code-block:: text

   1. is_fresh()?  ──no──→  build_index(downloader)
          │
          yes
          │
   2. get_query_result(query, limit)?  ──hit──→  return cached
          │
          miss
          │
   3. Execute query  →  apply sort  →  set_query_result()  →  return

Setting ``refresh=True`` forces step 1 to always rebuild, regardless of L1
freshness.
