Architecture and Design
==============================================================================


What sayt2 Does
------------------------------------------------------------------------------
sayt2 is a **search-as-you-type** library for Python. It lets you build a
full-text search index from structured data (a list of dicts) and query it
with substring matching, fuzzy tolerance, and multi-field sorting — all from a
single :class:`~sayt2.dataset.DataSet` object.

Typical use cases:

- Interactive search boxes (autocomplete) in CLI tools, desktop apps, or web
  back-ends.
- Filtering large reference tables (URLs, documentation pages, product
  catalogues) with instant feedback as the user types.


Design Goals
------------------------------------------------------------------------------

1. **Single-file deployment** — the index, cache, and lock state all live under
   one ``dir_root``.  Copy or delete the directory and you're done.
2. **Zero-config search** — define your fields, feed your data, call
   :meth:`~sayt2.dataset.DataSet.search`.  Tokenisers, caching, and locking
   are handled automatically.
3. **Tantivy isolation** — ``import tantivy`` appears in exactly one module
   (:mod:`sayt2.dataset`).  Every other module is engine-agnostic, so a future
   back-end swap touches one file.
4. **Pydantic configuration** — field definitions are pydantic models.
   Validation, serialisation, and IDE autocompletion come for free.


Module Dependency Layers
------------------------------------------------------------------------------

The library is organised into strict layers.  Each layer may only import from
the layers below it.

.. code-block:: text

   Layer 4  api            ─  Public re-export surface
              │
   Layer 3  dataset        ─  Core search engine (only tantivy consumer)
              │
   Layer 2  cache          ─  Two-layer disk cache (diskcache)
              │
   Layer 1  fields         ─  Field type definitions (pydantic)
            tracker        ─  Cross-process SQLite lock
              │
   Layer 0  constants      ─  Enum constants
            exc            ─  Custom exceptions

Arrows point downward: ``dataset`` depends on ``cache``, ``fields``, and
``tracker``; ``fields`` depends on ``constants``; ``tracker`` depends on
``exc``.  There are no upward or circular imports.


Key Design Decisions
------------------------------------------------------------------------------

Tantivy as the search back-end
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
`tantivy-py <https://github.com/quickwit-oss/tantivy-py>`_ provides Rust-speed
indexing and querying through Python bindings.  Index builds that previously
took seconds now complete in milliseconds, which in turn lets us lower the
default lock expiry from 300 s to 60 s.

SQLite atomic locking
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Cross-process coordination uses a single SQL ``UPSERT`` statement — see
:class:`~sayt2.tracker.Tracker`.  Because the lock check and the lock
acquisition happen in one atomic SQL statement, there is no race window between
"is it free?" and "I'll take it".

Two-layer caching
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
:class:`~sayt2.cache.DataSetCache` manages two concerns independently:

- **L1 — data freshness**: has the index been rebuilt recently enough?
- **L2 — query results**: has this exact ``(query, limit)`` been answered
  before?

Both layers are keyed with a schema hash
(:func:`~sayt2.fields.fields_schema_hash`), so changing the field definitions
automatically invalidates stale caches.

Discriminated-union fields
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The seven field types share a common :class:`~sayt2.fields.BaseField` base and
are assembled into a single discriminated union (:data:`~sayt2.fields.T_Field`)
via pydantic's ``Field(discriminator="type")``.  This allows polymorphic
deserialisation from plain dicts — useful when field definitions are stored in
config files.
