from unittest import TestCase, mock
from unittest.mock import patch

from harvaestus import crawl
from harvaestus.backlog import Backlog
from harvaestus.crawler import Crawler
from harvaestus.errors import FixableError, IgnoreKey
from harvaestus.storage import InMemoryStorage


class MyTestException(Exception):
    pass


class TestCrawl(TestCase):

    def setUp(self):
        self.fn = mock.Mock()
        self.backlog = Backlog()

    def test_simple(self):
        self.backlog.add(1)
        crawl(self.fn, self.backlog)

        self.fn.assert_called_once_with(1)

    def test_multiple(self):
        self.backlog.add_multiple(1, 2, 3)
        crawl(self.fn, self.backlog)

        self.assertEqual(self.fn.call_count, 3)

    def test_with_adding_to_backlog_at_runtime(self):
        self.backlog.add(1)
        call_count = 0

        def myfn(key):
            nonlocal call_count
            call_count += 1
            self.backlog.add(2)

        crawl(myfn, self.backlog)
        self.assertEqual(call_count, 2)

    def test_return_value_gets_stored(self):
        storage = InMemoryStorage()
        self.backlog.add("mykey")
        self.fn.return_value = 1

        with mock.patch("harvaestus.storage.InMemoryStorage.save") as patched:
            crawl(self.fn, backlog=self.backlog, storage=storage)
            patched.assert_called_once_with("mykey", 1)

    def test_error_policy_fail(self):
        self.fn.side_effect = MyTestException("test")
        self.backlog.add(1)

        with self.assertRaises(MyTestException):
            crawl(self.fn, backlog=self.backlog, error_policy="fail")

    def test_error_policy_ignore(self):
        self.fn.side_effect = MyTestException("test")
        self.backlog.add(1)

        crawl(self.fn, backlog=self.backlog, error_policy="ignore")

    def test_error_policy_fail3_2_errors(self):
        self.fn.side_effect = MyTestException("test")
        self.backlog.add_multiple(1, 2)

        crawl(self.fn, backlog=self.backlog, error_policy="fail3")

    def test_error_policy_fail3_3_errors(self):
        self.fn.side_effect = MyTestException("test")
        self.backlog.add_multiple(1, 2, 3)

        with self.assertRaises(MyTestException):
            crawl(self.fn, backlog=self.backlog, error_policy="fail3")

    def test_error_policy_fail3_3_errors_but_with_okay_result_in_between(self):
        self.fn.side_effect = MyTestException("test")
        self.backlog.add_multiple(1, 2, 3, 4)

        crawler = Crawler(self.fn, backlog=self.backlog, error_policy="fail3")

        crawler.run_once()
        crawler.run_once()
        self.fn.side_effect = None
        crawler.run_once()
        self.fn.side_effect = MyTestException("test")
        crawler.run_once()

    def test_error_handler(self):
        self.fn.side_effect = FixableError("test")
        self.backlog.add(1)
        handler = mock.Mock()

        crawler = Crawler(
            self.fn,
            backlog=self.backlog,
            error_handler={
                "test": handler,
            }
        )
        crawler.run_once()

        handler.assert_called_once_with(self.fn.side_effect)

    def test_fixable_error_without_handler(self):
        error = FixableError(error_key="test")
        self.fn.side_effect = error
        self.backlog.add(1)

        with mock.patch("harvaestus.crawler.Crawler.handle_exception") as patched:
            crawl(
                self.fn,
                backlog=self.backlog,
            )
            patched.assert_called_once_with(error, 1)

    def test_key_with_fixable_error_is_readded_to_backlog(self):
        error = FixableError(error_key="test")
        key = 1
        self.fn.side_effect = error
        self.backlog.add(key)

        with mock.patch("harvaestus.backlog.Backlog.re_add") as patched:
            crawl(
                self.fn,
                backlog=self.backlog,
                error_handler={"test": mock.Mock()}
            )
            patched.assert_called_once_with(key)

    def test_error_handler_with_assert(self):
        def myfn(key):
            assert 1 == 0, "test"

        self.backlog.add(1)
        handler = mock.Mock()

        crawler = Crawler(
            myfn,
            backlog=self.backlog,
            error_handler={
                "test": handler,
            }
        )
        crawler.run_once()

        handler.assert_called_once_with(FixableError("test"))

    def test_tqdm_call_when_tty(self):
        self.backlog.add(1)

        with (mock.patch("tqdm.std.tqdm.update") as patched,
              mock.patch("sys.stdout.isatty", return_value=True)):
            crawl(self.fn, backlog=self.backlog)

            patched.assert_called()

    def test_tqdm_not_called_when_not_tty(self):
        self.backlog.add(1)

        with (mock.patch("tqdm.std.tqdm.update") as patched,
              mock.patch("sys.stdout.isatty", return_value=False)):
            crawl(self.fn, backlog=self.backlog)

            patched.assert_not_called()

    def test_tqdm_total_gets_updated(self):
        self.backlog.add(1)

        def myfn(key):
            self.backlog.add(2)
            self.backlog.add(3)

        with mock.patch("sys.stdout.isatty", return_value=True):
            crawler = Crawler(myfn, backlog=self.backlog)
            crawler.run()
            self.assertEqual(crawler.pbar.total, 3)

    def test_persist_backlog_on_error(self):
        myfn = mock.Mock(side_effect=Exception("test"))
        self.backlog.add(1)

        with (mock.patch("harvaestus.backlog.Backlog.persist", mock.Mock()) as patched,
              mock.patch("builtins.open", mock.mock_open())):
            with self.assertRaises(Exception):
                crawl(myfn,
                      backlog=self.backlog,
                      persist_backlog_to="my-test-backlog.save")
            patched.assert_called_once()

    def test_dont_persist_backlog_on_error_if_not_configured(self):
        myfn = mock.Mock(side_effect=Exception("test"))
        self.backlog.add(1)

        with mock.patch("harvaestus.backlog.Backlog.persist") as patched:
            with self.assertRaises(Exception):
                crawl(myfn, backlog=self.backlog)
            patched.assert_not_called()

    def test_fn_returns_generator(self):
        def generator(key):
            yield {"test": "asd"}
            yield {"test": "bsd"}
        storage = InMemoryStorage()
        self.backlog.add(1)

        with mock.patch("harvaestus.storage.InMemoryStorage.save") as patched:
            crawl(generator, backlog=self.backlog, storage=storage)
            self.assertEqual(2, patched.call_count)

    def test_error_is_raised_on_generator(self):
        def generator(key):
            raise Exception
            yield {"test": "asd"}
            yield {"test": "bsd"}

        self.backlog.add(1)

        with self.assertRaises(Exception):
            crawl(generator, backlog=self.backlog)

    def test_ignore_key_error_ignores_key(self):
        def fn(key):
            raise IgnoreKey

        self.backlog.add(1)
        storage = InMemoryStorage()
        crawl(fn, backlog=self.backlog, storage=storage)

        self.assertNotIn(1, storage.data)