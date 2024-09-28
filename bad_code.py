import functools


class Fubar:

    @functools.lru_cache(maxsize=2048)
    def do_exp(self, x: int):
        return x**2
