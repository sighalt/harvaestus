import pickle
import random
from collections import Counter
from io import BytesIO
from typing import Hashable, BinaryIO, Literal, List

from harvaestus.errors import EmptyBacklog, ReAddLimitReached


class Backlog(object):
    """Backlog of keys to process."""

    def __init__(self, strategy: Literal["fifo", "random"] = "fifo", re_add_limit: int = 2):
        """Initialize the Backlog

        :type strategy: {'fifo', 'random'}
        :param strategy: Method of returning the next key.
        :type re_add_limit: int
        :param re_add_limit: Maximum number of times a key can be re-added to the backlog.
        """
        self.strategy = strategy
        self.re_add_limit = re_add_limit
        self._queue: List[Hashable] = []
        self.seen: set[Hashable] = set()
        self.re_adds: dict[Hashable, int] = Counter()

    def add(self, key: Hashable) -> None:
        """Add a key to the backlog."""
        if key not in self.seen:
            self._queue.append(key)
            self.seen.add(key)

    def add_multiple(self, *keys: Hashable) -> None:
        """Add multiple keys to the backlog."""
        for key in keys:
            self.add(key)

    def next(self) -> Hashable:
        """Get the next key from the backlog based on the strategy."""
        if self.strategy == "fifo":
            return self._next_fifo()
        elif self.strategy == "random":
            return self._next_random()

        raise NotImplementedError(f"Backlog strategy `{self.strategy}` not implemented.")

    def _next_fifo(self):
        """Return the next oldest key from the backlog."""
        try:
            return self._queue.pop(0)
        except IndexError:
            raise EmptyBacklog

    def _next_random(self):
        """Return one random key from the backlog."""
        try:
            return random.choice(self._queue)
        except IndexError:
            raise EmptyBacklog

    def is_empty(self):
        """Return True if there are no more items in the backlog."""
        return len(self._queue) == 0

    def re_add(self, key: Hashable) -> None:
        """Add an item to the backlog, even if it already has been processed"""
        if self.re_adds[key] >= self.re_add_limit:
            raise ReAddLimitReached(f"Re-add limit reached for key `{key}`.")

        self._queue.append(key)
        self.re_adds[key] += 1

    def __len__(self):
        """Return the size of the backlog"""
        return len(self._queue)

    def total(self):
        """Return the total number of items already processed or still in the backlog."""
        return len(self.seen)

    def persist(self, fp: BinaryIO) -> None:
        """Write Backlog to `path` if configured."""
        pickle.dump(self, fp)

    @classmethod
    def from_file(cls, file_path: str, not_exists_ok=False) -> "Backlog":
        try:
            with open(file_path, "rb") as f:
                return pickle.load(f)
        except FileNotFoundError:
            return cls()
