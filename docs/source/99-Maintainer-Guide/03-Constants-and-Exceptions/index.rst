Constants and Exceptions
==============================================================================
These two modules sit at **Layer 0** — they have zero internal dependencies and
form the foundation that every other module builds on.


Constants — :mod:`sayt2.constants`
------------------------------------------------------------------------------
Three ``str``-based enums define the valid values for field configuration.
Using enums (rather than bare strings) gives IDE autocomplete and catches typos
at validation time.

.. literalinclude:: ../../../../sayt2/constants.py
   :language: python
   :pyobject: FieldTypeEnum

:class:`~sayt2.constants.FieldTypeEnum` lists the seven field types.  Each
value matches the ``type`` discriminator on the corresponding
:class:`~sayt2.fields.BaseField` subclass.

.. literalinclude:: ../../../../sayt2/constants.py
   :language: python
   :pyobject: TokenizerEnum

:class:`~sayt2.constants.TokenizerEnum` enumerates the built-in tokenisers
available for :class:`~sayt2.fields.TextField`.

.. literalinclude:: ../../../../sayt2/constants.py
   :language: python
   :pyobject: NumericKindEnum

:class:`~sayt2.constants.NumericKindEnum` specifies the numeric precision for
:class:`~sayt2.fields.NumericField` — signed 64-bit integer, unsigned 64-bit
integer, or 64-bit float.


Exceptions — :mod:`sayt2.exc`
------------------------------------------------------------------------------
Three custom exceptions provide clear, catchable error signals.

.. literalinclude:: ../../../../sayt2/exc.py
   :language: python
   :lines: 4-

.. list-table::
   :header-rows: 1
   :widths: 35 20 45

   * - Exception
     - Base class
     - When it is raised
   * - :class:`~sayt2.exc.MalformedFieldSettingError`
     - ``ValueError``
     - A field definition fails validation (e.g. ``max_gram < min_gram``).
   * - :class:`~sayt2.exc.MalformedDatasetSettingError`
     - ``ValueError``
     - A :class:`~sayt2.dataset.DataSet` configuration is invalid (e.g.
       duplicate field names).
   * - :class:`~sayt2.exc.TrackerIsLockedError`
     - ``RuntimeError``
     - :meth:`~sayt2.tracker.Tracker.lock_it` cannot acquire the lock because
       another process holds it and it has not expired.
