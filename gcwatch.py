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

DataPoints = list[tuple[str, int, int, int]]
"""Data point (type_name, count, maximum, delta)"""

OnSampleHandler = Callable[[DataPoints], None]


class Metric:
    __slots__ = (
        "count",
        "maximum",
        "delta",
    )
    count: int
    maximum: int
    delta: int

    def __init__(self) -> None:
        self.reset()
        self.maximum = 0
        self.delta = 0

    def set_count(self, count: int) -> None:
        self.delta = count - self.count
        self.count = count
        self.maximum = max(self.maximum, count)

    def reset(self) -> None:
        self.count = 0

    def __repr__(self) -> str:
        return f"Metric(count={self.count}, maximum={self.maximum}, delta={self.delta})"


class _Monitor:
    _lock: threading.Lock
    _shutdown: threading.Event
    _tracked_types: tuple[type]
    _metrics: Mapping[type, Metric]
    _interval_seconds: float
    _on_sample_handler: OnSampleHandler | None

    def __init__(
        self,
        *,
        track_types: tuple[type],
        lock: threading.Lock,
        shutdown_signal: threading.Event,
        interval_ms: int,
        on_sample: OnSampleHandler | None = None,
    ) -> None:
        logger.debug("Monitor: Tracking types %s", track_types)

        self._lock = lock
        self._shutdown = shutdown_signal
        self._tracked_types = track_types
        self._metrics = defaultdict(Metric)
        self._interval_seconds = float(interval_ms) / 1000.0
        self._on_sample_handler = on_sample

    def set_tracked_types(self, types: tuple[type]) -> None:
        with self._lock:
            self._tracked_types = types

    def notify_callback(self) -> None:
        if self._on_sample_handler is not None:
            data_points = [
                (_make_qualified_name(ty), metric.count, metric.maximum, metric.delta)
                for ty, metric in self._metrics.items()
            ]
            self._on_sample_handler(data_points)

    def sample(self) -> None:
        objects = iter(
            obj for obj in gc.get_objects() if isinstance(obj, self._tracked_types)
        )

        with self._lock:
            counter: dict[type, int] = defaultdict(lambda: 0)

            for obj in objects:
                counter[type(obj)] += 1

            for ty, count in counter.items():
                self._metrics[ty].set_count(count)

    def run(self) -> None:
        logger.debug("Monitor: Starting up.")

        while not self._shutdown.is_set():
            self.sample()
            self.notify_callback()

            if self._shutdown.is_set():
                break

            time.sleep(self._interval_seconds)

        logger.debug("Monitor: Shut down.")


class GcWatcher:

    # ------------
    # Config
    _interval_ms: int
    _tracked_types: tuple[type] = ()
    _on_sample_handler: OnSampleHandler | None

    # ------------
    # Shared State
    _lock: threading.Lock
    _shutdown: threading.Event
    _monitor: None | tuple[threading.Thread, _Monitor]

    def __init__(
        self,
        track: type | tuple[type] | None = None,
        interval_ms: int = 250,
        on_sample: OnSampleHandler | None = None,
    ) -> None:
        if isinstance(track, tuple):
            self._tracked_types = track
        elif isinstance(track, type):
            self._tracked_types = (track,)
        else:
            self._tracked_types = ()

        self._interval_ms = interval_ms

        self._lock = threading.Lock()
        self._shutdown = threading.Event()
        self._monitor = None
        self._on_sample_handler = on_sample

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
        if isinstance(types, type):
            types = (types,)

        self._tracked_types = self._tracked_types + types

        if self.is_running:
            self._monitor[1].set_tracked_types(copy(self._tracked_types))

    def untrack(self, types: type | tuple[type]) -> None:
        if isinstance(types, type):
            types = (types,)

        self._tracked_types = tuple(ty for ty in self._tracked_types if ty not in types)

        if self.is_running:
            self._monitor[1].set_tracked_types(copy(self._tracked_types))

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
            track_types=copy(self._tracked_types),
            lock=self._lock,
            shutdown_signal=self._shutdown,
            interval_ms=self._interval_ms,
            on_sample=self._on_sample_handler,
        )
        # Seed the monitor's metrics with an immediate measurement
        # for the case where `get_metrics` is called before the
        # thread can do its first loop iteration.
        monitor.sample()

        worker = threading.Thread(target=monitor.run, daemon=False)

        self._monitor = (worker, monitor)
        self._monitor[0].start()

    def stop(self) -> None:
        # Warning: Called from the finaliser. Avoid module-level state.
        self._assert_is_running()

        logger.debug("GcWatcher: Sending shutdown signal.")

        self._shutdown.set()

        try:
            self._monitor[0].join(timeout=self._interval_ms * 1.5)
        finally:
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
        # Warning: Called during VM shutdown. Don't rely on module-level state.
        if self.is_running:
            self.stop()


def _make_qualified_name(ty: type) -> str:
    """Makes a fully qualified name for the given type."""
    return f"{ty.__module__}.{ty.__qualname__}"


# ---------------------------------

logging.basicConfig(level="DEBUG")


if __name__ == "__main__":
    logger.info("Start...")

    def on_sample(data_points: DataPoints):
        logger.info("Sampled: %s", data_points)

    with GcWatcher(track=Fubar, interval_ms=250, on_sample=on_sample) as watch:
        for _ in range(3):
            for i in range(100):
                Fubar().do_exp(i)
                time.sleep(0.1)
            Fubar.do_exp.cache_clear()
            time.sleep(0.5)
        logger.info("Metrics: %s", watch.get_metrics())

    logger.info("Done...")
