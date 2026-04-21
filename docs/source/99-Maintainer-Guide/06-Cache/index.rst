Cache — Two-Layer Disk Cache
==============================================================================
:mod:`sayt2.cache` manages a two-layer disk cache backed by
`diskcache <https://grantjenks.com/docs/diskcache/>`_.  Both layers live in a
single ``diskcache.Cache`` instance, distinguished by key prefixes and linked
by a shared tag for bulk eviction.


Why two layers?
------------------------------------------------------------------------------
A search dataset has two independent freshness concerns:

.. list-table::
   :header-rows: 1
   :widths: 10 25 35 30

   * - Layer
     - Concern
     - Key pattern
     - Expiry
   * - **L1**
     - Data freshness — is the index up-to-date?
     - ``fresh:{name}:{schema_hash}``
     - After ``expire`` seconds (configurable)
   * - **L2**
     - Query results — has this ``(query, limit)`` been answered before?
     - ``q:{name}:{schema_hash}:{query}:{limit}``
     - Never (bulk-evicted on rebuild)

When L1 expires, :meth:`~sayt2.dataset.DataSet.search` triggers a
``downloader() -> build_index()`` cycle, which calls
:meth:`~sayt2.cache.DataSetCache.evict_all` to wipe **both** layers.  This
guarantees that stale query results are never served after a data refresh.


Schema hash in cache keys
------------------------------------------------------------------------------
Every cache key embeds the schema hash produced by
:func:`~sayt2.fields.fields_schema_hash`.  If you add, remove, or modify a
field definition, the hash changes and all previous cache entries become
invisible — no explicit invalidation required.


DataSetCache class
------------------------------------------------------------------------------

.. literalinclude:: ../../../../sayt2/cache.py
   :language: python
   :pyobject: DataSetCache

Key methods:

:meth:`~sayt2.cache.DataSetCache.is_fresh` / :meth:`~sayt2.cache.DataSetCache.mark_fresh`
   L1 interface.  :meth:`~sayt2.cache.DataSetCache.mark_fresh` is called after
   a successful index build and starts the expiry countdown.

:meth:`~sayt2.cache.DataSetCache.get_query_result` / :meth:`~sayt2.cache.DataSetCache.set_query_result`
   L2 interface.  Results are always :class:`~sayt2.dataset.SearchResult`
   objects, so a ``None`` return unambiguously means cache miss.

:meth:`~sayt2.cache.DataSetCache.evict_all`
   Removes **all** entries (L1 + L2) for this dataset using
   ``diskcache``'s tag-based eviction.  Called at the start of every rebuild.

:meth:`~sayt2.cache.DataSetCache.close`
   Closes the underlying ``diskcache.Cache``.  Always call this when done (or
   rely on :class:`~sayt2.dataset.DataSet`, which handles it automatically).


Lifecycle within DataSet
------------------------------------------------------------------------------
You rarely need to instantiate :class:`~sayt2.cache.DataSetCache` directly.
:class:`~sayt2.dataset.DataSet` creates and manages it internally:

1. :meth:`~sayt2.dataset.DataSet.build_index` — ``evict_all()`` then
   ``mark_fresh()``
2. :meth:`~sayt2.dataset.DataSet.search` — ``is_fresh()`` to decide whether to
   rebuild, then ``get_query_result()`` / ``set_query_result()`` for L2
