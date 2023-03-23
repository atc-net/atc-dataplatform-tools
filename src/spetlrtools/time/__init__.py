from datetime import date, datetime, timezone

from .TimeSequence import TimeSequence  # noqa: F401


def dt_utc(*args, **kwargs) -> datetime:
    if args or kwargs:
        return datetime(*args, tzinfo=timezone.utc, **kwargs)
    else:
        return datetime.now(timezone.utc)


dt_iso = datetime.fromisoformat
d_iso = date.fromisoformat
