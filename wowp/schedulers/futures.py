from __future__ import absolute_import, division, print_function, unicode_literals

import time
import traceback
import datetime
import sys

from .shared import ActorRunner
from ..logger import logger


class FuturesScheduler(ActorRunner):
    """Scheduler using PEP 3148 futures

    Args:
        executor (str): executor type: multiprocessing, distributed, ipyparallel, mpi
        display_outputs (Optional[bool]): display stdout/err from actors [False]
        timeout (Optional): timeout in secs for waiting for ipyparallel cluster [60]
        min_engines (Optional[int]): minimum number of engines [1]
        client_kwargs: passed to ipyparallel.Client( **kwargs)
    """

    def __init__(self,
                 executor,
                 display_outputs=False,
                 min_engines=1,
                 timeout=60,
                 executor_kwargs=None,
                 copy_from=None):

        if executor_kwargs is None:
            executor_kwargs = {}

        # for .copy()
        # frame = inspect.currentframe()
        # args, varargs, keywords, values = inspect.getargvalues(frame)
        # self._init_args = [values[k] for k in args[1:]]
        # if keywords:
        #     self._init_kwargs = {k: values[k] for k in keywords if k not in ('copy_from', )}
        # else:
        #     self._init_kwargs = {}
        self._init_args = (executor, )
        self._init_kwargs = {'display_outputs': display_outputs,
                             'min_engines': min_engines,
                             'timeout': timeout,
                             'executor_kwargs': executor_kwargs}
        self.display_outputs = display_outputs

        if copy_from is None:
            if executor == 'multiprocessing':
                if sys.version > (3, 2):
                    self.executor = MultiprocessingExecutor(processes=min_engines)
                else:
                    raise UnsupportedExecutor(executor, "Requires Python version >= 3.2.")
            elif executor == 'distributed':
                try:
                    from .distributed import DistributedExecutor
                    self.executor = DistributedExecutor(uris=executor_kwargs['uris'],
                                                        min_engines=min_engines,
                                                        timeout=timeout)
                except ImportError:
                    raise UnsupportedExecutor(executor, "Library `distributed` not installed.")
            elif executor == 'ipyparallel':
                try:
                    from .ipyparallel import IpyparallelExecutor
                    kwargs = dict(min_engines=min_engines,
                                  timeout=timeout,
                                  display_outputs=display_outputs)
                    kwargs.update(executor_kwargs)
                    self.executor = IpyparallelExecutor(**kwargs)
                except ImportError:
                    raise UnsupportedExecutor(executor, "Library `iparallel` not installed.")

            elif executor == 'mpi':
                try:
                    from .mpi import MPIExecutor
                    self.executor = MPIExecutor()
                except ImportError:
                    raise UnsupportedExecutor(executor, "Library `mpi4py` not installed.")
            else:
                raise UnsupportedExecutor(executor)

            self.system_executor = LocalExecutor()
        else:
            # executors must be shared across copies to avoid their initialization
            self.executor = copy_from.executor
            self.system_executor = copy_from.system_executor

        self.reset()

    def reset(self):
        self.process_pool = []
        self.running_actors = {}
        self.execution_queue = deque()
        self.wait_queue = []
        # will be used as the initial sleep time between polls
        self.last_sleep = 1e-3

    def run_actor(self, actor):
        # print("Run actor {}".format(actor))
        actor.scheduler = self
        args, kwargs = actor.get_run_args()
        # system actors must be run within this process
        res = dict(args=args, kwargs=kwargs)
        if actor.system_actor:
            res['job'] = self.system_executor.submit(actor.run, *args, **
            kwargs)
        else:
            res['job'] = self.executor.submit(actor.run, *args, **kwargs)

        logger.debug('submitted actor {}, len(args)={}, kwargs keys={}'.format(
            actor.name, len(args), list(kwargs.keys())))

        return res

    def copy(self):
        return self.__class__(*self._init_args, copy_from=self, **self._init_kwargs)

    def put_value(self, in_port, value):
        self.execution_queue.appendleft((in_port, value))

    def execute(self):
        while self.execution_queue or self.running_actors or self.wait_queue:
            self.nothing = True

            self._try_empty_execution_queue()
            self._try_empty_wait_queue()
            self._try_empty_ready_jobs()

            # TODO fix against spamming engines - PROBABLY NOT THE BEST WAY!
            if self.nothing:
                # use constant sleep time
                logger.debug('scheduler sleeps for {} s'.format(
                    self.last_sleep))
                time.sleep(self.last_sleep)

                # TODO could we use callbacks?

    def _try_empty_ready_jobs(self):
        pending = {}  # temporary container
        for actor, job_description in self.running_actors.items():

            job = job_description['job']

            if job.done():
                self.nothing = False
                # process result
                # raise RemoteError in case of failure
                try:
                    result = job.result()
                except Exception:
                    logger.error('actor {} failed\n{}'.format(
                        actor.name, traceback.format_exc()))
                    self.reset()
                    raise
                if self.display_outputs:
                    job.display_outputs()
                if result:
                    # empty results don't need any processing
                    out_names = actor.outports.keys()
                    if not hasattr(result, 'items'):
                        raise ValueError(
                            'The execute method must return '
                            'a dict-like object with items method')
                    for name, value in result.items():
                        if name in out_names:
                            outport = actor.outports[name]
                            outport.put(value)
                            self.on_outport_put_value(outport)
                        else:
                            raise ValueError("{} not in output ports".format(
                                name))

            else:
                pending[actor] = job_description
        self.running_actors = pending

    def _try_empty_wait_queue(self):
        pending = []  # temporary container
        for actor in self.wait_queue:
            # run actors only if not already running
            if actor not in self.running_actors:
                self.nothing = False
                # TODO can we iterate and remove at the same time?
                self.running_actors[actor] = self.run_actor(actor)
            else:
                pending.append(actor)
        self.wait_queue = pending

    def _try_empty_execution_queue(self):
        while self.execution_queue:
            in_port, value = self.execution_queue.pop()
            should_run = in_port.put(value)
            if should_run:
                self.nothing = False
                # waiting to be run
                self.wait_queue.append(in_port.owner)
                # self.running_actors((in_port.owner, self.run_actor(in_port.owner)))

    def shutdown(self):
        logger.info('Scheduler is shutting down')
        if hasattr(self.executor, 'shutdown'):
            self.executor.shutdown()

    def __del__(self):
        self.shutdown()
        if hasattr(super(type(self)), '__del__'):
            super(type(self), self).__del__()


if sys.version_info >= (3, 2):
    class MultiprocessingExecutor(object):
        """Executes jobs in local subprocesses using concurrent.futures
        """

        def __init__(self, processes):
            from concurrent.futures import ProcessPoolExecutor
            self._pool = ProcessPoolExecutor(max_workers=processes)

        def submit(self, func, *args, **kwargs):
            """Submit a function: func(*args, **kwargs) and return a FutureJob.
            """
            job = self._pool.submit(func, *args, **kwargs)
            return FutureJob(job)


class LocalExecutor(object):
    """Executes jobs in the current process
    """

    def __init__(self):
        super(LocalExecutor, self).__init__()

    def submit(self, func, *args, **kwargs):
        job = LocalFutureJob(func, *args, **kwargs)
        return job


class FutureJob(object):
    """An asynchronous job with Future-like API
    """

    def __init__(self, future):
        self._future = future

    def __getattr__(self, item):
        return getattr(self._future, item)

    def display_outputs(self):
        # TODO how to display outputs?
        pass


class LocalFutureJob(object):
    """Local (system) job with FutureJob API
    """

    def __init__(self, func, *args, **kwargs):
        self.started = datetime.datetime.now()
        self._result = func(*args, **kwargs)

    def done(self):
        return True

    def result(self, timeout=None):
        return self._result

    def display_outputs(self):
        pass


class UnknownExecutor(RuntimeError):
    def __init__(self, name):
        super(UnknownExecutor, self).__init__("Unknown executor: {0}".format(name))


class UnsupportedExecutor(RuntimeError):
    def __init__(self, name, reason=None):
        if reason:
            super(UnsupportedExecutor, self).__init__("Executor {0} not supported. {1}".format(name, reason))
        else:
            super(UnsupportedExecutor, self).__init__("Executor {0} not supported.".format(name))