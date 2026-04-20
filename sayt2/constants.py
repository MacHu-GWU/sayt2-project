# -*- coding: utf-8 -*-

"""
Enumerated constants for field configuration values.

Using ``str``-based enums so that users get IDE autocomplete and don't have to
guess raw string values, while remaining fully compatible with pydantic
serialization / deserialization.
"""

import enum


class FieldTypeEnum(str, enum.Enum):
    STORED = "stored"
    KEYWORD = "keyword"
    TEXT = "text"
    NGRAM = "ngram"
    NUMERIC = "numeric"
    DATETIME = "datetime"
    BOOLEAN = "boolean"


class TokenizerEnum(str, enum.Enum):
    DEFAULT = "default"
    EN_STEM = "en_stem"


class NumericKindEnum(str, enum.Enum):
    I64 = "i64"
    U64 = "u64"
    F64 = "f64"
