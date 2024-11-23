import logging
from concurrent.futures.thread import ThreadPoolExecutor
import time
from logging import exception
from types import GeneratorType
from typing import Callable, Hashable, Any, Literal, List
from concurrent.futures import Future
import sys

from tqdm import tqdm

from harvaestus.backlog import Backlog
from harvaestus.errors import FixableError
from harvaestus.storage import InMemoryStorage, BaseStorage

logger = logging.getLogger(__name__)


class Crawler(object):

    def __init__(self,
                 fn: Callable[[Hashable], Any],
                 backlog: Backlog,
                 storage: BaseStorage | None = None,
                 error_policy: Literal["fail", "ignore", "fail3"] = "fail",
                 error_handler: dict | None = None,
                 concurrency: int = -1,
                 persist_backlog_to: str | None = None,
                 ):
        self.fn = fn
        self.backlog = backlog
        self.storage = storage
        self.error_policy = error_policy
        self.error_handler = error_handler or {}
        self.concurrency = max(concurrency, 1)
        self.error_counter = 0
        self.persist_backlog_to = persist_backlog_to
        self.pbar = None

    def store_if_necessary(self, key: Hashable, value: Any) -> None:
        if self.storage is None:
            return

        if isinstance(value, list):
            for v in value:
                self.storage.save(key, v)
        else:
            self.storage.save(key, value)

    def run(self) -> None:
        """Run the crawler until the backlog is empty."""
        try:
            self._run()
        finally:
            if self.persist_backlog_to:
                with open(self.persist_backlog_to, "wb") as fp:
                    self.backlog.persist(fp)

    def _run(self) -> None:
        """Run the crawler until the backlog is empty."""
        if sys.stdout.isatty():
            self.pbar = tqdm(total=self.backlog.total())
            self.pbar.update(self.backlog.total() - len(self.backlog))

        executor = ThreadPoolExecutor(max_workers=self.concurrency)
        running: List[Future[None]] = []
        exceptions: List[Exception] = []

        def _remove_future(fut):
            if error := fut.exception():
                exceptions.append(error)

            running.remove(fut)

        with executor:
            while not self.backlog.is_empty() or running:
                for e in exceptions:
                    raise e

                if len(running) >= self.concurrency:
                    time.sleep(0.1)
                    continue

                if not self.backlog.is_empty():
                    future = executor.submit(self._run_once)
                    running.append(future)
                    future.add_done_callback(_remove_future)
                else:
                    time.sleep(0.1)
                    continue

            # make sure there are no more exceptions to be handled
            for e in exceptions:
                raise e

    def run_once(self):
        """Run the crawler with the next item in the backlog."""
        try:
            self._run_once()
        except:
            if self.persist_backlog_to:
                with open(self.persist_backlog_to, "wb") as fp:
                    self.backlog.persist(fp)

            raise

    def _run_once(self):
        """Run the crawler with the next item in the backlog."""
        key = self.backlog.next()

        try:
            value = self.fn(key)

            if isinstance(value, GeneratorType):
                value = list(value)

        except FixableError as e:
            self.handle_fixable_error(e, key)
        except AssertionError as e:
            self.handle_fixable_error(FixableError(e.args[0]), key)
        except Exception as e:
            self.handle_exception(e, key)
        else:
            self.error_counter = 0
            self.store_if_necessary(key, value)
        finally:
            self.update_pbar()

    def handle_exception(self, e: Exception, key: Hashable):
        """Handle the runtime exception according to the error policy."""
        logger.error(f"Encountered unfixable error with key: {key}")

        if self.error_policy == "fail":
            raise e
        elif self.error_policy == "ignore":
            return
        elif self.error_policy == "fail3":
            self.error_counter += 1

            if self.error_counter >= 3:
                raise e

            return

        raise RuntimeError("Unknown error policy")

    def handle_fixable_error(self, error: FixableError, key: Hashable) -> None:
        """Try to handle the FixableError exception with the given error handlers."""
        try:
            handler = self.error_handler[error.error_key]
        except KeyError:
            self.handle_exception(error, key)
        else:
            handler(error)
            self.backlog.re_add(key)

    def update_pbar(self):
        """Increase the progress bar, if there is one."""
        if self.pbar:
            self.pbar.total = self.backlog.total()
            self.pbar.update(1)


def crawl(
        fn: Callable[[Hashable], Any],
        backlog: Backlog,
        storage: BaseStorage | None = None,
        error_policy: Literal["fail", "ignore", "fail3"] = "fail",
        error_handler: dict | None = None,
        concurrency: int = -1,
        persist_backlog_to: str | None = None):
    crawler = Crawler(fn, backlog, storage, error_policy, error_handler, concurrency, persist_backlog_to)
    crawler.run()
