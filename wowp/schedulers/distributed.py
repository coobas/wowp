"""Executor based on Dask.distributed"""
from __future__ import absolute_import, division, print_function, unicode_literals

import six
from distributed import Executor

from .futures import FutureJob


class DistributedExecutor(object):
    """Executes jobs using distributed

    Args:
        uris: one or more dexecuter URI's (str or list)
        min_engines (int): minimum number of engines
        timeout(float): time to wait for engines
    """

    def __init__(self, uris, min_engines=1, timeout=60):
        if isinstance(uris, six.string_types):
            uris = (uris, )
        self._clients = [Executor(addr) for addr in uris]
        self._current_cli = 0
        # TODO assure

    def _rotate_client(self):
        # TODO pick the first (most) empty one
        current = self._current_cli
        self._current_cli = (self._current_cli + 1) % len(self._clients)
        return self._clients[current]

    def submit(self, func, *args, **kwargs):
        """Submit a function: func(*args, **kwargs) and return a FutureJob.
        """
        cli = self._rotate_client()
        job = cli.submit(func, *args, **kwargs)
        return FutureJob(job)