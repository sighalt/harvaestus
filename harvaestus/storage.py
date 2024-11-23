import abc
import csv
import os
from collections import defaultdict, deque
from csv import DictWriter
from functools import wraps
from typing import Hashable, Mapping, Iterable, Any, Tuple, TextIO

from harvaestus.errors import DataIsNotAllowed, StorageNotAvailable


class BaseStorage(abc.ABC):
    """Harvaestus Base class for storages.

    Stores arbitraty dictionary-like data indicated with a key.

    The key does not have to be unique, meaning there may be multiple data packets
    associated with the same key. Though, it is required, that the schema of the dictionary must
    stay the same over all dictionaries. There will be a `DataSchemaChanged` exception otherwise.

    Usage:

    ```
    storage = Storage()
    key = "1"
    data = {"value": 1}
    keys = "1", "2"
    data_multiple = [{"value": 1}, {"value": 2}}]

    def data_iterable():
        for i in range(10):
        yield i, {"value": i}

    with storage:
        storage.save(key, data)
        storage.save_multiple(keys, data_multiple)
        storage.save_from_iterable(data_iterable())

    with storage:
        storage.save(key, data)
        storage.commit()

    with storage:
        for key in storage.keys():
            print(storage.key)

            for data in storage.iter_values(key=key):
                print(" " * 5, data)

        for key, value in storage.iter_items():
            print(key, value)
    ```
    """

    @abc.abstractmethod
    def save(self, key: Hashable, data: dict[str, Any]) -> None:
        """Save to storage"""
        raise NotImplementedError

    def save_multiple(self, keys: Iterable[Hashable], dicts: Iterable[dict[str, Any]]) -> None:
        """Save multiple items to storage"""
        for key, value in zip(keys, dicts):
            self.save(key, value)

    def save_from_iterable(self, data_iterable: Iterable[Tuple[Hashable, dict[str, Any]]]) -> None:
        """Save multiple items to storage by iterating the iterable"""
        for key, value in data_iterable:
            self.save(key, value)

    @abc.abstractmethod
    def commit(self) -> None:
        """Commit the buffered data-"""
        raise NotImplementedError

    @abc.abstractmethod
    def keys(self) -> Iterable[str]:
        """Return all keys in storage."""
        raise NotImplementedError

    @abc.abstractmethod
    def iter_values(self, key: str | None = None) -> Iterable[dict[str, Any]]:
        """Return all values in storage. If `key` is given, return only data saved
        with this key.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def iter_items(self) -> Iterable[dict[str, Any]]:
        """Return all data in storage as key, data tuples."""
        raise NotImplementedError

    def __enter__(self) -> "BaseStorage":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.commit()


class InMemoryStorage(BaseStorage):
    """Harvaestus in-memory storage implementation.

    Saves all data non-persistent. Usually used for testing-purposes only.
    """

    def __init__(self):
        """Initialize storage."""
        self.data = defaultdict(deque)

    def save(self, key: Hashable, data: dict[str, Any]) -> None:
        """Save to storage"""
        self.data[key].append(data)

    def commit(self) -> None:
        """Commit the buffered data-"""
        pass

    def keys(self) -> Iterable[str]:
        """Return all keys in storage."""
        return self.data.keys()

    def iter_values(self, key: str | None = None) -> Iterable[dict[str, Any]]:
        """Return all values in storage. If `key` is given, return only data saved
        with this key.
        """
        if key:
            yield from self.data[key]
        else:
            for value in self.data.values():
                yield from value

    def iter_items(self) -> Iterable[dict[str, Any]]:
        """Return all data in storage as key, data tuples."""
        for key, values in self.data.items():
            for value in values:
                yield key, value


def check_fp_availability(fn):

    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        if self.file_pointer is None:
            raise StorageNotAvailable("File was not opened. Did you use a `with` statement?")

        return fn(self, *args, **kwargs)

    return wrapper


class CsvFileStorage(BaseStorage):
    """Harvaestus storage implementation which saves all data to a csv file.
    """

    def __init__(self, file_path: str):
        """Initialize storage."""
        self.file_path: str = file_path
        self.file_pointer: TextIO | None = None
        self.csv_writer: csv.DictWriter | None = None
        self.csv_reader: csv.DictReader | None = None
        self.fieldnames: list | None = None

    @check_fp_availability
    def save(self, key: Hashable, data: dict[str, Any]) -> None:
        """Save to storage"""
        if "_key" in data:
            raise DataIsNotAllowed("The key '_key' is reserved and cannot be used in the data.")

        self.file_pointer.seek(0, 2)
        if self.fieldnames is None:
            self.fieldnames = ["_key"] + list(data.keys())
            self.csv_writer = csv.DictWriter(self.file_pointer, fieldnames=self.fieldnames)
            self.csv_writer.writeheader()
            self.csv_reader = csv.DictReader(self.file_pointer, fieldnames=self.fieldnames)

        data = data.copy()
        data.update({"_key": key})

        try:
            self.csv_writer.writerow(data)
        except ValueError as e:
            raise DataIsNotAllowed(str(e))

    @check_fp_availability
    def commit(self) -> None:
        """Commit the buffered data-"""
        self.file_pointer.flush()

    @check_fp_availability
    def keys(self) -> Iterable[str]:
        """Return all keys in storage.

        This method may be very slow, because it needs to scan the whole csv file.
        """
        self.file_pointer.seek(0)
        next(self.csv_reader)
        seen = []
        
        for row in self.csv_reader:
            if row["_key"] not in seen:
                seen.append(row["_key"])
        
        return seen

    @check_fp_availability
    def iter_values(self, key: str | None = None) -> Iterable[dict[str, Any]]:
        """Return all values in storage. If `key` is given, return only data saved
        with this key.

        This method may be very slow, because it needs to scan the whole csv file.
        """
        self.file_pointer.seek(0)
        next(self.csv_reader)

        for row in self.csv_reader:
            row_key = row.pop("_key")

            if key and row_key == key:
                yield row
            elif key and row_key != key:
                continue
            else:
                yield row

    @check_fp_availability
    def iter_items(self) -> Iterable[dict[str, Any]]:
        """Return all data in storage as key, data tuples."""
        self.file_pointer.seek(0)
        next(self.csv_reader)

        for row in self.csv_reader:
            key = row.pop("_key")
            yield key, row

    def __enter__(self) -> "CsvFileStorage":
        self.file_pointer = os.fdopen(os.open(self.file_path, os.O_RDWR | os.O_CREAT), 'r+')
        # self.file_pointer = open(self.file_path, "r+")
        self.file_pointer.seek(0, 2)

        if self.file_pointer.tell() > 0 and self.fieldnames is None:
            # there is already data
            self.file_pointer.seek(0)
            header_line = self.file_pointer.readline()
            self.fieldnames = header_line.strip().split(",")
            self.csv_writer = csv.DictWriter(self.file_pointer, fieldnames=self.fieldnames)
            self.csv_reader = csv.DictReader(self.file_pointer, fieldnames=self.fieldnames)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.commit()
        self.file_pointer.close()
        self.file_pointer = None
        self.csv_reader = None
        self.csv_writer = None
