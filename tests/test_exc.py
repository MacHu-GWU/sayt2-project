# -*- coding: utf-8 -*-

from sayt2.exc import MalformedFieldSettingError
from sayt2.exc import MalformedDatasetSettingError
from sayt2.exc import TrackerIsLockedError


class TestExceptions:
    def test(self):
        pass


if __name__ == "__main__":
    from sayt2.tests import run_cov_test

    run_cov_test(
        __file__,
        "sayt2.exc",
        preview=False,
    )
