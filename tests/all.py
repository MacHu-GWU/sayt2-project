# -*- coding: utf-8 -*-

if __name__ == "__main__":
    from sayt2.tests import run_cov_test

    run_cov_test(
        __file__,
        "sayt2",
        is_folder=True,
        preview=False,
    )
