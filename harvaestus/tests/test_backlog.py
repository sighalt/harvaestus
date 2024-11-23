import os
from tempfile import NamedTemporaryFile
from unittest import TestCase, mock
from unittest.mock import Mock

from harvaestus.backlog import Backlog
from harvaestus.errors import EmptyBacklog, ReAddLimitReached


class TestBacklog(TestCase):

    def setUp(self):
        self.backlog = Backlog()

    def test_add_and_retrieve(self):
        key = "mykey"
        self.backlog.add(key)
        self.assertEqual(key, self.backlog.next())

    def test_add_multiple_and_retrieve(self):
        keys = ("mykey1", "mykey2", "mykey3")
        self.backlog.add_multiple(*keys)
        self.assertEqual(keys[0], self.backlog.next())
        self.assertEqual(keys[1], self.backlog.next())
        self.assertEqual(keys[2], self.backlog.next())

    def test_next_from_empty_backlog(self):
        for strategy in ["fifo", "random"]:
            with self.subTest(strategy=strategy):
                backlog = Backlog(strategy=strategy)

                with self.assertRaises(EmptyBacklog):
                    backlog.next()

    def test_random_retrieval(self):
        backlog = Backlog(strategy="random")
        backlog.add_multiple((1, 2))
        mocked_choice = Mock(return_value=1337)

        with mock.patch("random.choice", mocked_choice):
            self.assertEqual(1337, backlog.next())

    def test_unknown_strategy(self):
        backlog = Backlog(strategy="unknown")

        with self.assertRaises(NotImplementedError):
            backlog.next()

    def test_backlog_dont_handle_duplicates(self):
        self.backlog.add(1)
        self.backlog.add(1)

        self.assertEqual(self.backlog.next(), 1)

        with self.assertRaises(EmptyBacklog):
            self.backlog.next()

    def test_backlog_dont_handle_duplicates_multiple(self):
        self.backlog.add_multiple(1, 1)

        self.assertEqual(self.backlog.next(), 1)

        with self.assertRaises(EmptyBacklog):
            self.backlog.next()

    def test_backlog_dont_handle_duplicates_even_after_already_processed(self):
        self.backlog.add(1)
        self.assertEqual(self.backlog.next(), 1)
        self.backlog.add(1)

        with self.assertRaises(EmptyBacklog):
            self.backlog.next()

    def test_backlog_empty_start(self):
        self.assertTrue(self.backlog.is_empty())

    def test_backlog_empty_after_processing(self):
        self.backlog.add(1)
        self.assertFalse(self.backlog.is_empty())
        self.backlog.next()
        self.assertTrue(self.backlog.is_empty())

    def test_backlog_empty_after_processing_with_multiples_of_the_same_key(self):
        self.backlog.add_multiple((1, 1))
        self.assertFalse(self.backlog.is_empty())
        self.backlog.next()
        self.assertTrue(self.backlog.is_empty())

    def test_backlog_re_add_a_duplicate_even_after_already_processed(self):
        self.backlog.add(1)
        self.assertEqual(self.backlog.next(), 1)
        self.assertTrue(self.backlog.is_empty())
        self.backlog.re_add(1)
        self.assertFalse(self.backlog.is_empty())
        self.assertEqual(self.backlog.next(), 1)

    def test_backlog_re_add_limit(self):
        self.backlog.re_add_limit = 2
        self.backlog.add(1)
        self.backlog.re_add(1)
        self.backlog.re_add(1)

        with self.assertRaises(ReAddLimitReached):
            self.backlog.re_add(1)

    def test_backlog_is_sized(self):
        self.assertEqual(len(self.backlog), 0)
        self.backlog.add(1)
        self.assertEqual(len(self.backlog), 1)
        self.backlog.add(2)
        self.assertEqual(len(self.backlog), 2)

    def test_backlog_total(self):
        self.assertEqual(self.backlog.total(), 0)
        self.backlog.add(1)
        self.assertEqual(self.backlog.total(), 1)
        self.backlog.next()
        self.assertEqual(self.backlog.total(), 1)
        self.backlog.add(2)
        self.assertEqual(self.backlog.total(), 2)

    def test_backlog_total_ignores_duplicates(self):
        self.assertEqual(self.backlog.total(), 0)
        self.backlog.add(1)
        self.assertEqual(self.backlog.total(), 1)
        self.backlog.add(1)
        self.assertEqual(self.backlog.total(), 1)

    def test_save_and_restore(self):
        self.backlog.add("my_unique_key")
        file = NamedTemporaryFile("wb", delete=False)

        try:
            with open(file.name, "wb") as f:
                self.backlog.persist(f)

            new_backlog = Backlog.from_file(file.name)

            self.assertEqual("my_unique_key", new_backlog.next())
            self.assertTrue(new_backlog.is_empty())
        finally:
            file.close()
            os.unlink(file.name)

    def test_restore_if_not_exists(self):
        backlog = Backlog.from_file("not-existing-file", not_exists_ok=True)
        self.assertEqual(0, len(backlog))

