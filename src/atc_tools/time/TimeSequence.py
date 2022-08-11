from datetime import datetime, timedelta, timezone
from typing import Union

import dateparser as dateparser


class TimeSequence:
    def __init__(
        self,
        start: Union[datetime, str] = datetime.now(timezone.utc),
        delta_seconds=100,
    ):
        if isinstance(start, datetime):
            self.last = start
        else:
            self.last = dateparser.parse(start).astimezone(timezone.utc)
        self.delta = timedelta(seconds=delta_seconds)

    def reverse(self, steps: int = 1):
        """produce descending timestamps so that first call is latest."""
        return self.next(-1 * steps)

    def next(self, steps: int = 1):
        """produce ascending timestamps so that last call is latest."""
        self.last = self.last + steps * self.delta
        return self.last

    def __next__(self):
        return self.next()
