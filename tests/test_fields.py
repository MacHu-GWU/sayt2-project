# -*- coding: utf-8 -*-

import pytest
from pydantic import TypeAdapter, ValidationError

from sayt2.fields import (
    BaseField,
    StoredField,
    KeywordField,
    TextField,
    NgramField,
    NumericField,
    DatetimeField,
    BooleanField,
    T_Field,
    fields_schema_hash,
)

# TypeAdapter for polymorphic deserialization
_adapter = TypeAdapter(T_Field)


class TestDefaults:
    """Each field type can be created with only a name and has sensible defaults."""

    def test_stored(self):
        f = StoredField(name="raw_html")
        assert f.type == "stored"
        assert f.stored is True

    def test_keyword(self):
        f = KeywordField(name="id")
        assert f.type == "keyword"
        assert f.stored is True
        assert f.boost == 1.0

    def test_text(self):
        f = TextField(name="body")
        assert f.type == "text"
        assert f.tokenizer == "default"
        assert f.boost == 1.0

    def test_text_en_stem(self):
        f = TextField(name="body", tokenizer="en_stem")
        assert f.tokenizer == "en_stem"

    def test_ngram(self):
        f = NgramField(name="title")
        assert f.type == "ngram"
        assert f.min_gram == 2
        assert f.max_gram == 6
        assert f.prefix_only is False
        assert f.lowercase is True
        assert f.boost == 1.0

    def test_numeric(self):
        f = NumericField(name="year")
        assert f.type == "numeric"
        assert f.kind == "i64"
        assert f.indexed is False
        assert f.fast is True

    def test_datetime(self):
        f = DatetimeField(name="created")
        assert f.type == "datetime"
        assert f.indexed is True
        assert f.fast is True

    def test_boolean(self):
        f = BooleanField(name="active")
        assert f.type == "boolean"
        assert f.indexed is True


class TestValidation:
    """Pydantic rejects invalid configurations."""

    def test_empty_name(self):
        with pytest.raises(ValidationError):
            StoredField(name="")

    def test_ngram_max_lt_min(self):
        with pytest.raises(ValidationError, match="max_gram.*min_gram"):
            NgramField(name="x", min_gram=6, max_gram=2)

    def test_ngram_min_gram_zero(self):
        with pytest.raises(ValidationError):
            NgramField(name="x", min_gram=0)

    def test_boost_zero(self):
        with pytest.raises(ValidationError):
            KeywordField(name="x", boost=0)

    def test_boost_negative(self):
        with pytest.raises(ValidationError):
            TextField(name="x", boost=-1.0)

    def test_invalid_tokenizer(self):
        with pytest.raises(ValidationError):
            TextField(name="x", tokenizer="custom")

    def test_invalid_numeric_kind(self):
        with pytest.raises(ValidationError):
            NumericField(name="x", kind="f32")


class TestSerialization:
    """model_dump → model_validate round-trip preserves all data."""

    FIELD_INSTANCES = [
        StoredField(name="raw"),
        KeywordField(name="id", boost=2.0),
        TextField(name="body", tokenizer="en_stem", boost=1.5),
        NgramField(name="title", min_gram=3, max_gram=8, prefix_only=True),
        NumericField(name="year", kind="u64", indexed=True, fast=False),
        DatetimeField(name="ts", indexed=False, fast=False),
        BooleanField(name="active", indexed=False),
    ]

    @pytest.mark.parametrize("field", FIELD_INSTANCES, ids=lambda f: f.type)
    def test_round_trip(self, field):
        data = field.model_dump()
        restored = _adapter.validate_python(data)
        assert type(restored) is type(field)
        assert restored == field

    @pytest.mark.parametrize("field", FIELD_INSTANCES, ids=lambda f: f.type)
    def test_round_trip_json(self, field):
        json_str = field.model_dump_json()
        restored = _adapter.validate_json(json_str)
        assert type(restored) is type(field)
        assert restored == field


class TestDiscriminatedUnion:
    """TypeAdapter correctly dispatches based on the 'type' discriminator."""

    def test_from_dict(self):
        data = {"type": "ngram", "name": "title", "min_gram": 3}
        field = _adapter.validate_python(data)
        assert isinstance(field, NgramField)
        assert field.min_gram == 3
        assert field.max_gram == 6  # default

    def test_from_dict_numeric(self):
        data = {"type": "numeric", "name": "rating", "kind": "f64"}
        field = _adapter.validate_python(data)
        assert isinstance(field, NumericField)
        assert field.kind == "f64"

    def test_unknown_type_raises(self):
        with pytest.raises(ValidationError):
            _adapter.validate_python({"type": "unknown", "name": "x"})

    def test_list_of_fields(self):
        adapter = TypeAdapter(list[T_Field])
        data = [
            {"type": "keyword", "name": "id"},
            {"type": "ngram", "name": "title"},
            {"type": "numeric", "name": "year"},
        ]
        fields = adapter.validate_python(data)
        assert len(fields) == 3
        assert isinstance(fields[0], KeywordField)
        assert isinstance(fields[1], NgramField)
        assert isinstance(fields[2], NumericField)


class TestSchemaHash:
    def test_deterministic(self):
        fields = [NgramField(name="title"), NumericField(name="year")]
        assert fields_schema_hash(fields) == fields_schema_hash(fields)

    def test_changes_with_field_config(self):
        a = [NgramField(name="title", min_gram=2)]
        b = [NgramField(name="title", min_gram=3)]
        assert fields_schema_hash(a) != fields_schema_hash(b)

    def test_changes_with_field_order(self):
        x = NgramField(name="title")
        y = NumericField(name="year")
        assert fields_schema_hash([x, y]) != fields_schema_hash([y, x])

    def test_length(self):
        h = fields_schema_hash([StoredField(name="x")])
        assert len(h) == 16


if __name__ == "__main__":
    from sayt2.tests import run_cov_test

    run_cov_test(
        __file__,
        "sayt2.fields",
        preview=False,
    )
