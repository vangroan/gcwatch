from __future__ import annotations

import gc
import logging
import threading
import time
from collections import defaultdict
from collections.abc import Callable, Mapping
from copy import copy, deepcopy
from types import TracebackType

from bad_code import Fubar
from errors import WatcherAlreadyRunning, WatcherStopped

logger = logging.getLogger(__name__)

__all__ = [
    "GcWatcher",
]


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


class _Monitor:
    _lock: threading.Lock
    _shutdown: threading.Event
    _types: tuple[type]
    _metrics: Mapping[type, Metric]
    _interval_seconds: float

    def __init__(
        self,
        *,
        track_types: tuple[type],
        lock: threading.Lock,
        shutdown_signal: threading.Event,
        interval_ms: int,
    ) -> None:
        logger.debug("Monitor: Tracking types %s", track_types)

        self._lock = lock
        self._shutdown = shutdown_signal
        self._types = track_types
        self._metrics = defaultdict(Metric)
        self._interval_seconds = float(interval_ms) / 1000.0

    def track(self, types: type | tuple[type]) -> None:
        logger.debug("Monitor: Tracking types %s", types)

        if isinstance(types, type):
            types = (types,)

        with self._lock:
            self._types = self._types + types

    def untrack(self, types: type | tuple[type]) -> None:
        logger.debug("Monitor: Untracking types %s", types)

        if isinstance(types, type):
            types = (types,)

        with self._lock:
            self._types = tuple(ty for ty in self._types if ty not in types)

            for ty in types:
                del self._metrics[ty]

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

    def run(self) -> None:
        logger.debug("Monitor: Starting up.")

        while not self._shutdown.is_set():
            self.sample()
            # logger.debug("Monitor: metrics=%s", self._metrics)

            if self._shutdown.is_set():
                break

            # logger.debug("Monitor: Sleep %.3f", self._interval_seconds)
            time.sleep(self._interval_seconds)

        logger.debug("Monitor: Shut down.")


class GcWatcher:

    # ------------
    # Config
    _types: tuple[type] = ()
    _interval_ms: int

    # ------------
    # Shared State
    _lock: threading.Lock
    _shutdown: threading.Event
    _monitor: None | tuple[threading.Thread, _Monitor]
    _on_sample: Callable | None

    def __init__(
        self,
        track: type | tuple[type] | None = None,
        interval_ms: int = 250,
        on_sample: Callable | None = None,
    ) -> None:
        if isinstance(track, tuple):
            self._types = track
        elif isinstance(track, type):
            self._types = (track,)
        else:
            self._types = ()

        self._interval_ms = interval_ms

        self._lock = threading.Lock()
        self._shutdown = threading.Event()
        self._monitor = None
        self._on_sample = on_sample

        if on_sample is not None:
            raise NotImplementedError("TODO: Callback not implemented yet.")

    @property
    def is_running(self) -> bool:
        return self._monitor is not None

    def _assert_is_running(self) -> None:
        if not self.is_running:
            raise WatcherStopped

    def track(self, types: type | tuple[type]) -> None:
        """
        Track the given type in the garbage collector.
        """
        # FIXME: Share _types with monitor
        self._assert_is_running()
        self._monitor[1].track(types)

    def untrack(self, types: type | tuple[type]) -> None:
        # FIXME: Share _types with monitor
        self._assert_is_running()
        self._monitor[1].untrack(types)

    def sample(self) -> None:
        self._assert_is_running()
        self._monitor[1].sample()

    def get_counts(self) -> dict[str, int]:
        self._assert_is_running()

        with self._lock:
            return {
                _make_qualified_name(ty): metric.count
                for ty, metric in self._monitor[1]._metrics.items()
            }

    def get_metrics(self, threshold: int = 10) -> dict[str, Metric]:
        self._assert_is_running()

        with self._lock:
            return {
                _make_qualified_name(ty): deepcopy(metric)
                for ty, metric in self._monitor[1]._metrics.items()
                if metric.count >= threshold
            }

    def start(self) -> None:
        if self.is_running:
            raise WatcherAlreadyRunning

        self._shutdown.clear()

        monitor = _Monitor(
            track_types=copy(self._types),
            lock=self._lock,
            shutdown_signal=self._shutdown,
            interval_ms=self._interval_ms,
        )
        # Seed the monitor's metrics with an immediate measurement
        # for the case where `get_metrics` is called before the
        # first loop iteration.
        monitor.sample()

        worker = threading.Thread(target=monitor.run, daemon=False)

        self._monitor = (worker, monitor)
        self._monitor[0].start()

    def stop(self) -> None:
        # Warning: Called from the finaliser. Avoid module-level state.
        self._assert_is_running()

        logger.debug("GcWatcher: Sending shutdown signal.")

        self._shutdown.set()
        self._monitor[0].join(timeout=self._interval_ms * 1.5)
        self._monitor = None

    def __enter__(self) -> GcWatcher:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.stop()

    def __del__(self) -> None:
        if self.is_running:
            self.stop()


def _make_qualified_name(ty: type) -> str:
    """Makes a fully qualified name for the given type."""
    return f"{ty.__module__}.{ty.__qualname__}"


# ---------------------------------

logging.basicConfig(level="DEBUG")


for i in range(100):
    Fubar().do_exp(i)


if __name__ == "__main__":
    logger.info("Start...")

    with GcWatcher(track=Fubar, interval_ms=25) as watch:
        for _ in range(10):
            time.sleep(0.5)
            for i in range(1000):
                Fubar().do_exp(i)
        time.sleep(0.5)
        logger.info("Metrics: %s", watch.get_metrics())

    logger.info("Done...")
