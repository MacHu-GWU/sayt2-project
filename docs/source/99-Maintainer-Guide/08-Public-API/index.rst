Public API
==============================================================================
:mod:`sayt2.api` is the **single import surface** for all public names.
Internal modules can be reorganised freely — as long as ``api.py`` continues
to re-export the same names, downstream code is unaffected.


Export list
------------------------------------------------------------------------------

.. literalinclude:: ../../../../sayt2/api.py
   :language: python
   :lines: 9-

The table below groups every exported name by its source module:

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Source module
     - Name
     - Kind
   * - :mod:`~sayt2.exc`
     - :class:`~sayt2.exc.MalformedFieldSettingError`
     - Exception
   * - :mod:`~sayt2.exc`
     - :class:`~sayt2.exc.MalformedDatasetSettingError`
     - Exception
   * - :mod:`~sayt2.exc`
     - :class:`~sayt2.exc.TrackerIsLockedError`
     - Exception
   * - :mod:`~sayt2.fields`
     - :class:`~sayt2.fields.BaseField`
     - Class
   * - :mod:`~sayt2.fields`
     - :class:`~sayt2.fields.StoredField`
     - Class
   * - :mod:`~sayt2.fields`
     - :class:`~sayt2.fields.KeywordField`
     - Class
   * - :mod:`~sayt2.fields`
     - :class:`~sayt2.fields.TextField`
     - Class
   * - :mod:`~sayt2.fields`
     - :class:`~sayt2.fields.NgramField`
     - Class
   * - :mod:`~sayt2.fields`
     - :class:`~sayt2.fields.NumericField`
     - Class
   * - :mod:`~sayt2.fields`
     - :class:`~sayt2.fields.DatetimeField`
     - Class
   * - :mod:`~sayt2.fields`
     - :class:`~sayt2.fields.BooleanField`
     - Class
   * - :mod:`~sayt2.fields`
     - :data:`~sayt2.fields.T_Field`
     - Type alias
   * - :mod:`~sayt2.fields`
     - :func:`~sayt2.fields.fields_schema_hash`
     - Function
   * - :mod:`~sayt2.dataset`
     - :class:`~sayt2.dataset.DataSet`
     - Class
   * - :mod:`~sayt2.dataset`
     - :class:`~sayt2.dataset.SortKey`
     - Class
   * - :mod:`~sayt2.dataset`
     - :class:`~sayt2.dataset.SearchResponse`
     - Class
   * - :mod:`~sayt2.dataset`
     - :class:`~sayt2.tracker.Tracker`
     - Class
   * - :mod:`~sayt2.cache`
     - :class:`~sayt2.cache.DataSetCache`
     - Class


Usage
------------------------------------------------------------------------------
Import everything you need from a single module:

.. code-block:: python

   from sayt2.api import DataSet, NgramField, TextField, KeywordField, SortKey

This is the recommended import style for application code.  Importing directly
from internal modules (e.g. ``from sayt2.fields import NgramField``) also
works but is not covered by the stability guarantee — internal module paths
may change between minor versions.
