"""Explicit handler error categories for bounded retry decisions."""


class RetryableError(Exception):
    """A transient handler failure eligible for another execution attempt."""


class FatalError(Exception):
    """A permanent handler failure that must go directly to the dead-letter topic."""
