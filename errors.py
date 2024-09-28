class GcWatchError(Exception):
    pass


class WatcherAlreadyRunning(Exception):
    def __init__(self) -> None:
        super().__init__("Watcher is already running.")


class WatcherStopped(Exception):
    def __init__(self) -> None:
        super().__init__("Watcher is stopped.")
