"""Integer money helpers."""


def percent_of(cents: int, percent: int) -> int:
    """Return a nonnegative percentage rounded to the nearest cent."""
    if cents < 0:
        raise ValueError("cents must be nonnegative")
    if percent < 0:
        raise ValueError("percent must be nonnegative")
    return (cents * percent + 50) // 100
