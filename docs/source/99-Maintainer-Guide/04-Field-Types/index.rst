Field Types
==============================================================================
:mod:`sayt2.fields` defines **seven field types** that describe how each column
of your data is stored, indexed, and searched.  All types are pydantic
``BaseModel`` subclasses, so validation, serialisation, and IDE autocompletion
work out of the box.

.. important::

   This module has **no dependency on tantivy**.  The mapping from field
   definitions to tantivy schema objects lives entirely in
   :mod:`sayt2.dataset`.


Base class
------------------------------------------------------------------------------

.. literalinclude:: ../../../../sayt2/fields.py
   :language: python
   :pyobject: BaseField

Every field has a ``type`` discriminator (overridden by each subclass as a
``Literal``), a ``name``, and a ``stored`` flag.  The ``type`` value is drawn
from :class:`~sayt2.constants.FieldTypeEnum`.


Text family
------------------------------------------------------------------------------

StoredField
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../../../sayt2/fields.py
   :language: python
   :pyobject: StoredField

:class:`~sayt2.fields.StoredField` keeps the value in the index but does not
make it searchable or sortable.  Use it for payload data you want returned with
search results (e.g. URLs, descriptions).

KeywordField
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../../../sayt2/fields.py
   :language: python
   :pyobject: KeywordField

:class:`~sayt2.fields.KeywordField` performs **exact-match** search.  The
entire field value is treated as a single token (``raw`` tokeniser), making it
ideal for IDs, tags, and enum values.  The ``boost`` parameter controls how
much weight this field carries in relevance scoring.

TextField
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../../../sayt2/fields.py
   :language: python
   :pyobject: TextField

:class:`~sayt2.fields.TextField` is for **full-text search**.  Choose a
``tokenizer`` from :class:`~sayt2.constants.TokenizerEnum`:

- ``"default"`` — Unicode-aware word boundary splitting.
- ``"en_stem"`` — English stemming (e.g. "running" matches "run").

NgramField
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../../../sayt2/fields.py
   :language: python
   :pyobject: NgramField

:class:`~sayt2.fields.NgramField` is the **search-as-you-type** workhorse.
It builds an ngram inverted index so that any substring of length
``[min_gram, max_gram]`` becomes a searchable token.

Key parameters:

- ``min_gram`` / ``max_gram`` — control the ngram window.  A
  ``@model_validator`` ensures ``max_gram >= min_gram``.
- ``prefix_only`` — when ``True``, only prefixes of each word are indexed
  (faster, but no mid-word matching).
- ``lowercase`` — normalise tokens to lowercase before indexing.


Numeric family
------------------------------------------------------------------------------

NumericField
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../../../sayt2/fields.py
   :language: python
   :pyobject: NumericField

:class:`~sayt2.fields.NumericField` stores numbers.  The ``kind`` parameter
selects the underlying type from :class:`~sayt2.constants.NumericKindEnum`.
Defaults to sort-only (``indexed=False, fast=True``), which is the typical
configuration for rating or year columns.

DatetimeField
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../../../sayt2/fields.py
   :language: python
   :pyobject: DatetimeField

:class:`~sayt2.fields.DatetimeField` stores date/time values.  Both
``indexed`` and ``fast`` default to ``True``.

BooleanField
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../../../sayt2/fields.py
   :language: python
   :pyobject: BooleanField

:class:`~sayt2.fields.BooleanField` stores boolean values.


Discriminated union
------------------------------------------------------------------------------
All seven types are assembled into a single type alias:

.. literalinclude:: ../../../../sayt2/fields.py
   :language: python
   :lines: 125-143

:data:`~sayt2.fields.T_Field` uses pydantic's ``Field(discriminator="type")``
so that a plain dict like ``{"type": "ngram", "name": "title"}`` is
automatically deserialised into the correct subclass.  This is especially
useful when field definitions are loaded from configuration files.


Schema hashing
------------------------------------------------------------------------------

.. literalinclude:: ../../../../sayt2/fields.py
   :language: python
   :pyobject: fields_schema_hash

:func:`~sayt2.fields.fields_schema_hash` computes a deterministic SHA-256
digest (truncated to 16 hex characters) of a list of field definitions.  The
hash is used as part of cache keys in :class:`~sayt2.cache.DataSetCache`, so
that changing the schema automatically invalidates stale cached results.
