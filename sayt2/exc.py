# -*- coding: utf-8 -*-


class MalformedFieldSettingError(ValueError):
    """Raised when a field configuration is invalid."""

    pass


class MalformedDatasetSettingError(ValueError):
    """Raised when a DataSet configuration is invalid (e.g. duplicate field names)."""

    pass


class TrackerIsLockedError(RuntimeError):
    """Raised when attempting to acquire a lock that is already held and not expired."""

    pass
