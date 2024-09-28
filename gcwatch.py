from copy import deepcopy
import gc
import logging
from collections import defaultdict
from collections.abc import Callable, Sequence, Mapping
from dataclasses import dataclass
from pprint import pprint
from threading import Lock

from bad_code import Fubar

logger = logging.getLogger(__name__)


class Metric:
    __slots__ = (
        "maximum",
        "minimum",
        "count",
    )
    maximum: int
    minimum: int
    count: int

    def __init__(self) -> None:
        self.reset()
        self.maximum = 0
        self.minimum = 0

    def set_count(self, count: int) -> None:
        self.count = count
        self.maximum = max(self.maximum, count)
        self.minimum = min(self.minimum, count)

    def reset(self) -> None:
        self.count = 0

    def __repr__(self) -> str:
        return f"Metric(count={self.count}, maximum={self.maximum}, minimum={self.minimum})"


ObjId = int


class GcWatcher:

    # ------------
    # Config
    _interval_ms = 250

    # ------------
    # Shared State
    _lock: Lock
    _types: tuple[type]
    _metrics: Mapping[type, Metric]

    def __init__(self, on_sample: Callable | None = None) -> None:
        self._lock = Lock()
        self._types = ()
        self._metrics = defaultdict(Metric)
        self._interval_ms = 250

    def track(self, types: type | tuple[type]) -> None:
        """
        Track the given type in the garbage collector.
        """
        logger.debug("Tracking types: %s", types)

        if isinstance(types, type):
            types = (types,)

        with self._lock:
            self._types = self._types + types

    def untrack(self, types: type | tuple[type]) -> None:
        logger.debug("Untracking types: %s", types)

        if isinstance(types, type):
            types = (types,)

        with self._lock:
            self._types = tuple(ty for ty in self._types if ty not in types)

    def sample(self) -> None:
        objects = iter(obj for obj in gc.get_objects() if isinstance(obj, self._types))

        with self._lock:
            for metric in self._metrics.values():
                metric.reset()

            counter: dict[type, int] = defaultdict(lambda: 0)

            for obj in objects:
                counter[type(obj)] += 1

            for ty, count in counter.items():
                self._metrics[ty].set_count(count)

    def get_counts(self) -> dict[str, int]:
        with self._lock:
            return {
                _make_qualified_name(ty): metric.count
                for ty, metric in self._metrics.items()
            }

    def get_metrics(self, threshold: int = 10) -> dict[str, Metric]:
        with self._lock:
            return {
                _make_qualified_name(ty): deepcopy(metric)
                for ty, metric in self._metrics.items()
                if metric.count >= threshold
            }

    def _spawn(self) -> None:
        raise NotImplementedError("TODO: Spawn background thread")

    def start(self) -> None:
        raise NotImplementedError

    def stop(self) -> None:
        raise NotImplementedError


def _make_qualified_name(ty: type) -> str:
    """Makes a fully qualified name for the given type."""
    return f"{ty.__module__}.{ty.__qualname__}"


# ---------------------------------

logging.basicConfig(level="DEBUG")


for i in range(10):
    Fubar().do_exp(i)


if __name__ == "__main__":
    logger.info("Start...")

    watch = GcWatcher()
    watch.track(Fubar)
    watch.sample()
    logger.info("Counts: %s", watch.get_counts())
    logger.info("Metrics: %s", watch.get_metrics())

    Fubar.do_exp.cache_clear()
    watch.sample()
    logger.info("Metrics: %s", watch.get_metrics())

    logger.info("Done...")
