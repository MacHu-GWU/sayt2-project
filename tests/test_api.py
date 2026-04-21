# -*- coding: utf-8 -*-

import pytest

from sayt2 import api


_EXPECTED_NAMES = [
    # exc
    "MalformedFieldSettingError",
    "MalformedDatasetSettingError",
    "TrackerIsLockedError",
    # fields
    "BaseField",
    "StoredField",
    "KeywordField",
    "TextField",
    "NgramField",
    "NumericField",
    "DatetimeField",
    "BooleanField",
    "T_Field",
    "fields_schema_hash",
    # dataset
    "DataSet",
    "SortKey",
    "SearchResponse",
    # tracker
    "Tracker",
    # cache
    "DataSetCache",
]


@pytest.mark.parametrize("name", _EXPECTED_NAMES)
def test_public_api_importable(name):
    assert hasattr(api, name), f"sayt2.api.{name} not found"


if __name__ == "__main__":
    from sayt2.tests import run_cov_test

    run_cov_test(
        __file__,
        "sayt2.api",
        preview=False,
    )
