"""Executor based on IPyparallel"""
from __future__ import absolute_import, division, print_function, unicode_literals

import time
import inspect
from collections import deque

import six
import ipyparallel

from ..logger import logger


class IpyparallelExecutor(object):
    """Executes jobs using ipyparallel

    Args:
        profiles (Optional[iterable]): list of ipyparallel profile names
        profile_dirs (Optional[iterable]): list of ipyparallel profile directories
        display_outputs (Optional[bool]): display stdout/err from actors [False]
        timeout (Optional): timeout in secs for waiting for ipyparallel cluster [60]
        min_engines (Optional[int]): minimum number of engines [1]
        client_kwargs: passed to ipyparallel.Client( **kwargs)
    """

    def __init__(self,
                 profiles=(),
                 profile_dirs=(),
                 display_outputs=False,
                 timeout=60,
                 min_engines=1,
                 client_kwargs=None):

        self.process_pool = []
        # actor: job
        frame = inspect.currentframe()
        args, varargs, keywords, values = inspect.getargvalues(frame)
        self._init_args = [values[k] for k in args[1:]]
        if keywords:
            self._init_kwargs = {k: values[k] for k in keywords}
        else:
            self._init_kwargs = {}
        self.display_outputs = display_outputs
        # get individual clients
        self._ipy_rc = []
        self._current_cli = 0
        self._ipy_lv = []
        self._ipy_dv = []
        # decide whether to use profiles or profile_dirs
        if profiles:
            cli_arg = 'profile'
            cli_args = profiles
        else:
            cli_arg = 'profile_dir'
            cli_args = profile_dirs
        if not cli_args:
            # raise ValueError('Either profiles or profile_dirs must be specified')
            cli_arg = 'profile'
            cli_args = ('default', )
        if isinstance(cli_args, six.string_types):
            cli_args = (cli_args, )

        for value in cli_args:
            # init ipyparallel clients
            kwargs = {cli_arg: value}
            if client_kwargs:
                kwargs.update(client_kwargs)
            # TODO min_engines divide by len(profile_dirs)
            self._ipy_rc.append(self.init_cluster(min_engines, timeout, **
            kwargs))
            self._ipy_dv.append(self._ipy_rc[-1][:])
            # Use dill / cloudpickle by default
            try:
                self._ipy_dv[-1].use_dill()
            except Exception:
                try:
                    self._ipy_dv[-1].use_cloudpickle()
                except Exception as e:
                    logger.warn('Nor dill not cloudpickle can be used for ipyparallel')
            self._ipy_lv.append(self._ipy_rc[-1].load_balanced_view())

        self.running_actors = {}
        self.execution_queue = deque()
        self.wait_queue = []

    @staticmethod
    def init_cluster(min_engines, timeout, *args, **kwargs):
        '''Get a connection (view) to an IPython cluster

        Args:
            *args: passed to ipyparallel.Client(*args, **kwargs)
            **kwargs: passed to ipyparallel.Client(*args, **kwargs)
        '''

        maxtime = time.time() + timeout

        while True:
            try:
                cli = ipyparallel.Client(*args, **kwargs)
            except Exception as e:
                if time.time() > maxtime:
                    # raise the original exception from ipyparallel
                    raise e
                else:
                    # sleep and try again to get the client
                    print("Waiting for ipyparallel cluster Client(*{}, **{})".format(args, kwargs))
                    print(e)
                    time.sleep(timeout * 0.1)
                    continue
            if len(cli.ids) >= min_engines:
                # we have enough clients
                break
            else:
                # free the client
                cli.close()
            if time.time() > maxtime:
                raise Exception('Not enough ipyparallel clients')
            # try ~10 times
            print("Found {}/{} clients, need more ...".format(len(cli.ids), min_engines))
            time.sleep(timeout * 0.1)

        return cli

    def _rotate_client(self):
        # TODO pick the first (most) empty one
        current = self._current_cli
        self._current_cli = (self._current_cli + 1) % len(self._ipy_rc)
        return current

    def submit(self, func, *args, **kwargs):
        """Submit a function: func(*args, **kwargs) and return a FutureJob.
        """

        # This switches ipyparallel clients (ie clusters)
        # TODO this is likely not good - data will have to travel too much
        lv = self._ipy_lv[self._rotate_client()]
        job = lv.apply_async(func, *args, **kwargs)
        return FutureIpyJob(job)


class FutureIpyJob(object):
    """Wraps asynchronous results of different kinds into a future-like object
    """

    def __init__(self, job):
        self._job = job

    def done(self):
        return self._job.ready()

    def result(self, timeout=None):
        return self._job.get()

    def display_outputs(self):
        return self._job.display_outputs()