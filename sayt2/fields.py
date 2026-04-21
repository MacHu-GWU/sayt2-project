# -*- coding: utf-8 -*-

"""
Field type definitions for sayt2.

Seven field types covering all search/store/sort use cases.  Each type is a
pydantic ``BaseModel`` with validation, serialization, and discriminated-union
support for polymorphic deserialization from config files.

Field types carry **no dependency on tantivy** — the mapping from field
definitions to tantivy schema objects lives in ``dataset.py``.
"""

from __future__ import annotations

import hashlib
import json
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field, model_validator

from .constants import FieldTypeEnum, NumericKindEnum, TokenizerEnum

# --- base -------------------------------------------------------------------


class BaseField(BaseModel):
    """
    Common base for all field types.

    Every subclass must override ``type`` with a ``Literal["..."]`` so that
    pydantic's discriminated union can reconstruct the correct class from a
    plain dict.
    """

    type: str  # overridden by each subclass as a Literal
    name: str = Field(min_length=1)
    stored: bool = True


# --- text family -------------------------------------------------------------


class StoredField(BaseField):
    """Store-only field.  Not indexed, not searchable, not sortable."""

    type: Literal["stored"] = FieldTypeEnum.STORED.value


class KeywordField(BaseField):
    """
    Exact-match field (id, tag, enum).  Uses the ``raw`` tokenizer under the
    hood — the entire field value is treated as one token.
    """

    type: Literal["keyword"] = FieldTypeEnum.KEYWORD.value
    boost: float = Field(default=1.0, gt=0)


class TextField(BaseField):
    """
    Full-text search field.  Uses the ``default`` (Unicode-aware word
    boundary) or ``en_stem`` (English stemming) tokenizer.
    """

    type: Literal["text"] = FieldTypeEnum.TEXT.value
    tokenizer: Literal["default", "en_stem"] = TokenizerEnum.DEFAULT.value
    boost: float = Field(default=1.0, gt=0)


class NgramField(BaseField):
    """
    Search-as-you-type field.  Builds an ngram inverted index so that any
    substring of length ``[min_gram, max_gram]`` is a valid query token.
    """

    type: Literal["ngram"] = FieldTypeEnum.NGRAM.value
    min_gram: int = Field(default=2, ge=1)
    max_gram: int = Field(default=6, ge=1)
    prefix_only: bool = False
    lowercase: bool = True
    boost: float = Field(default=1.0, gt=0)

    @model_validator(mode="after")
    def _max_gte_min(self) -> NgramField:
        if self.max_gram < self.min_gram:
            raise ValueError(
                f"max_gram ({self.max_gram}) must be >= min_gram ({self.min_gram})"
            )
        return self


# --- numeric family ----------------------------------------------------------


class NumericField(BaseField):
    """
    Numeric field.  Defaults to sort-only (``indexed=False, fast=True``) which
    is the typical use case for rating/year columns.
    """

    type: Literal["numeric"] = FieldTypeEnum.NUMERIC.value
    kind: Literal["i64", "u64", "f64"] = NumericKindEnum.I64.value
    indexed: bool = False
    fast: bool = True


class DatetimeField(BaseField):
    """Datetime field backed by tantivy's date type."""

    type: Literal["datetime"] = FieldTypeEnum.DATETIME.value
    indexed: bool = True
    fast: bool = True


class BooleanField(BaseField):
    """Boolean field."""

    type: Literal["boolean"] = FieldTypeEnum.BOOLEAN.value
    indexed: bool = True


# --- union & helpers ---------------------------------------------------------

T_Field = Annotated[
    Union[
        StoredField,
        KeywordField,
        TextField,
        NgramField,
        NumericField,
        DatetimeField,
        BooleanField,
    ],
    Field(discriminator="type"),
]
"""Discriminated union of all field types.  Use with ``TypeAdapter`` for
polymorphic deserialization::

    from pydantic import TypeAdapter
    adapter = TypeAdapter(T_Field)
    field = adapter.validate_python({"type": "ngram", "name": "title"})
"""


def fields_schema_hash(fields: list[T_Field]) -> str:  # type: ignore[type-arg]
    """
    Deterministic hash of a list of field definitions.

    Used as part of cache keys so that changing the schema automatically
    invalidates stale caches.
    """
    payload = "|".join(
        json.dumps(f.model_dump(exclude_none=True), sort_keys=True) for f in fields
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:16]
