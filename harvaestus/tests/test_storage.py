import os
from tempfile import NamedTemporaryFile
from unittest import TestCase, mock
from unittest.mock import patch

from harvaestus.errors import StorageNotAvailable, DataIsNotAllowed
from harvaestus.storage import InMemoryStorage, CsvFileStorage


class StorageFunctionalityMixin(object):

    def setUp(self):
        self.storage: InMemoryStorage = None

    def test_with_as(self):
        with InMemoryStorage() as storage:
            self.assertTrue(isinstance(storage, InMemoryStorage))

    def test_with_auto_commits(self):
        dotted_path_commit = (f"{self.storage.__class__.__module__}."
                              f"{self.storage.__class__.commit.__qualname__}")
        with patch(dotted_path_commit) as mock_commit:
            with self.storage:
                pass

            mock_commit.assert_called_once()

    def test_single_item_save(self):
        with self.storage:
            self.storage.save("my_key", {"value": "a"})
            self.storage.commit()

            result = list(self.storage.iter_values("my_key"))
            self.assertEqual([{"value": "a"}], result)

            result = list(self.storage.iter_values())
            self.assertEqual([{"value": "a"}], result)

            result = list(self.storage.keys())
            self.assertEqual(["my_key"], result)

            result = list(self.storage.iter_items())
            self.assertEqual([("my_key", {"value": "a"})], result)

    def test_many_items_with_multiple(self):
        with self.storage:
            self.storage.save_multiple(["my_key1", "my_key2"], [{"value": "a"}, {"value": "b"}])
            self.storage.commit()

            result = list(self.storage.iter_values("my_key1"))
            self.assertEqual([{"value": "a"}], result)

            result = list(self.storage.iter_values("my_key2"))
            self.assertEqual([{"value": "b"}], result)

            result = list(self.storage.iter_values())
            self.assertEqual(
                [{"value": "a"}, {"value": "b"}],
                result
            )

            result = list(self.storage.keys())
            self.assertEqual(["my_key1", "my_key2"], result)

            result = list(self.storage.iter_items())
            self.assertEqual([("my_key1", {"value": "a"}), ("my_key2", {"value": "b"})], result)

    def test_many_items_with_from_iterable(self):
        def data_generator():
            yield "my_key1", {"value": "a"}
            yield "my_key2", {"value": "b"}

        with self.storage:
            self.storage.save_from_iterable(data_generator())
            self.storage.commit()

            result = list(self.storage.iter_values("my_key1"))
            self.assertEqual([{"value": "a"}], result)

            result = list(self.storage.iter_values("my_key2"))
            self.assertEqual([{"value": "b"}], result)

            result = list(self.storage.iter_values())
            self.assertEqual(
                [{"value": "a"}, {"value": "b"}],
                result
            )

            result = list(self.storage.keys())
            self.assertEqual(["my_key1", "my_key2"], result)

            result = list(self.storage.iter_items())
            self.assertEqual([("my_key1", {"value": "a"}), ("my_key2", {"value": "b"})], result)

    def test_many_items_with_multiple_duplicate_keys(self):
        with self.storage:
            self.storage.save_multiple(["my_key1", "my_key1"], [{"value": "a"}, {"value": "b"}])
            self.storage.commit()

            result = list(self.storage.iter_values("my_key1"))
            self.assertEqual([{"value": "a"}, {"value": "b"}], result)

            result = list(self.storage.iter_values())
            self.assertEqual(
                [{"value": "a"}, {"value": "b"}],
                result
            )

            result = list(self.storage.keys())
            self.assertEqual(["my_key1"], result)

            result = list(self.storage.iter_items())
            self.assertEqual([("my_key1", {"value": "a"}), ("my_key1", {"value": "b"})], result)

    def test_many_items_with_from_iterable_2(self):
        def data_generator():
            yield "my_key1", {"value": "a"}
            yield "my_key1", {"value": "b"}

        with self.storage:
            self.storage.save_from_iterable(data_generator())
            self.storage.commit()

            result = list(self.storage.iter_values("my_key1"))
            self.assertEqual([{"value": "a"}, {"value": "b"}], result)

            result = list(self.storage.iter_values())
            self.assertEqual(
                [{"value": "a"}, {"value": "b"}],
                result
            )

            result = list(self.storage.keys())
            self.assertEqual(["my_key1"], result)

            result = list(self.storage.iter_items())
            self.assertEqual([("my_key1", {"value": "a"}), ("my_key1", {"value": "b"})], result)


class TestInMemoryStorage(StorageFunctionalityMixin, TestCase):

    def setUp(self):
        self.storage = InMemoryStorage()


class TestCsvFileStorage(StorageFunctionalityMixin, TestCase):

    def setUp(self):
        self.csv_file = NamedTemporaryFile(delete=False)
        self.storage = CsvFileStorage(self.csv_file.name)

    def tearDown(self):
        self.csv_file.close()
        os.unlink(self.csv_file.name)

    def test_csv_file_is_written(self):
        with self.storage:
            self.storage.save("my_key", {"value": 1})
            self.storage.save("my_key2", {"value": 1})
            self.storage.save("my_key3", {"value": 1})
            self.storage.commit()

        self.csv_file.seek(0)
        written = self.csv_file.read()
        self.assertEqual(b"_key,value\r\nmy_key,1\r\nmy_key2,1\r\nmy_key3,1\r\n", written)

    def test_error_is_raised_if_not_within_context_manager(self):
        with self.assertRaises(StorageNotAvailable):
            self.storage.save("my_key", {"value": 1})

        with self.storage:
            self.storage.save("my_key", {"value": 1})

        with self.assertRaises(StorageNotAvailable):
            self.storage.save("my_key", {"value": 1})

    def test_error_is_raised_if_special_key_is_used(self):
        with self.assertRaises(DataIsNotAllowed), self.storage:
            self.storage.save("my_key", {"_key": 1})

    def test_error_is_raised_if_data_does_not_match_schema(self):
        with self.assertRaises(DataIsNotAllowed), self.storage:
            self.storage.save("my_key", {"value": "1"})
            self.storage.save("my_key", {"different_value": "1"})

    def test_write_and_read(self):
        with self.storage:
            self.storage.save("my_key", {"value": "1"})
            self.storage.save("my_key2", {"value": "1"})
            expected_keys = list(self.storage.keys())
            expected_items = list(self.storage.iter_items())

        with CsvFileStorage(self.csv_file.name) as second_storage:
            self.assertEqual(expected_keys, list(second_storage.keys()))
            self.assertEqual(expected_items, list(second_storage.iter_items()))

    def test_write_and_read_and_write(self):
        with self.storage:
            self.storage.save("my_key", {"value": "1"})
            expected_keys = list(self.storage.keys())
            expected_items = list(self.storage.iter_items())

        with CsvFileStorage(self.csv_file.name) as second_storage:
            self.assertEqual(expected_keys, list(second_storage.keys()))
            self.assertEqual(expected_items, list(second_storage.iter_items()))
            second_storage.save("my_key2", {"value": "1"})

            self.assertEqual(
                [
                    ("my_key", {"value": "1"}),
                    ("my_key2", {"value": "1"}),
                ],
                list(second_storage.iter_items()),
            )
