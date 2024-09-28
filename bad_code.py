import functools


class Fubar:

    @functools.lru_cache
    def do_exp(self, x: int):
        return x**2
