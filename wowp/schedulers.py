from __future__ import absolute_import, division, print_function, unicode_literals

import inspect
from collections import deque
import threading
import warnings
import wowp.components
import time
import datetime
import six
import traceback
from .logger import logger
import concurrent.futures

from .util import loads, dumps
try:
    import ipyparallel
except ImportError:
    warnings.warn(
        'ipyparallel not installed: FuturesScheduler cannot use ipyparallel executor')
    ipyparallel = None
try:
    import distributed
except ImportError:
    warnings.warn(
        'distributed not installed: FuturesScheduler cannot use distributed executor')
    distributed = None
try:
    import mpi4py
    import mpi4py.futures
    mpi4py.MPI.pickle.__init__(loads=loads, dumps=dumps)
except ImportError:
    warnings.warn(
        'mpi4py not installed or mpi4py.futures not supported: mpi4py cannot be used')
    mpi4py = None
import itertools


__all__ = [
    "NaiveScheduler",
    "LinearizedScheduler",
    "ThreadedScheduler",
    "FuturesScheduler"]


class _ActorRunner(object):
    """Base class for objects that run actors and process their results.

    It is ok, if the runner is a scheduler at the same time. The separation
    of concepts exists only for the cases when a scheduler needs to run
    actors in parallel (such as ThreadedScheduler).
    """

    def on_outport_put_value(self, outport):
        '''
        Propagates values put into an output port.

        Must be called after outport.put
        :param outport: output port
        :return: None
        '''
        if outport.connections:
            value = outport.pop()
            for inport in outport.connections:
                self.put_value(inport, value)

    def run_actor(self, actor):
        # print("Run actor")
        # TODO replace by an attribute / method call
        if isinstance(actor, wowp.components.Composite):
            self.run_workflow(actor)
        else:
            actor.scheduler = self
            args, kwargs = actor.get_run_args()
            result = actor.run(*args, **kwargs)
            # print("Result: ", result)
            if not result:
                return
            else:
                out_names = actor.outports.keys()
                if not hasattr(result, 'items'):
                    raise ValueError('The execute method must return '
                                     'a dict-like object with items method')
                for name, value in result.items():
                    if name in out_names:
                        outport = actor.outports[name]
                        outport.put(value)
                        self.on_outport_put_value(outport)
                    else:
                        raise ValueError("{} not in output ports".format(name))

    def run_workflow(self, workflow, **kwargs):
        inport_names = tuple(port.name for port in workflow.inports)
        if workflow.scheduler is not None:
            # TODO this seems a bit strange
            scheduler = workflow.scheduler
        else:
            scheduler = self
        for key, value in kwargs.items():
            if key not in inport_names:
                raise ValueError('{} is not an inport name'.format(key))
            inport = workflow.inports[key]
            # put values to connected ports
            scheduler.put_value(inport, kwargs[inport.name])
        # TODO can this be run inside self.execute itsef?
        scheduler.execute()

    def reset(self):
        """Reset the scheduler
        """
        # by default, this method does nothing
        pass

    def shutdown(self):
        pass

    # def __del__(self):
    #     self.shutdown()
    #     super(_ActorRunner, self).__del__()


class NaiveScheduler(_ActorRunner):
    """Scheduler that directly calls connected actors.

    Problem: recursion quickly ends in full call stack.
    """

    def copy(self):
        return self

    def put_value(self, in_port, value):
        should_run = in_port.put(value)
        if should_run:
            self.run_actor(in_port.owner)

    def execute(self):
        pass


class LinearizedScheduler(_ActorRunner):
    """Scheduler that stacks all inputs in a queue and executes them in FIFO order."""

    def __init__(self):
        self.execution_queue = deque()

    def copy(self):
        return self.__class__()

    def put_value(self, in_port, value):
        self.execution_queue.appendleft((in_port, value))

    def execute(self):
        while self.execution_queue:
            in_port, value = self.execution_queue.pop()
            should_run = in_port.put(value)
            if should_run:
                self.run_actor(in_port.owner)


class DistributedExecutor(object):
    """Executes jobs using distributed

    Args:
        uris: one or more dexecuter URI's (str or list)
        min_engines (int): minimum number of engines
        timeout(float): time to wait for engines
    """

    def __init__(self, uris=None, min_engines=1, timeout=60):
        from dask.distributed import Client
        if isinstance(uris, six.string_types):
            uris = (uris, )
        elif uris is None:
            uris = (None, )
        self._clients = [Client(addr) for addr in uris]
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
        executor = cli.get_executor()
        job = executor.submit(func, *args, **kwargs)
        return job


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


class MPIExecutor(object):
    """Executes jobs in local subprocesses using concurrent.futures
    """

    def __init__(self, max_workers=None):
        self.mpi_comm = mpi4py.MPI.COMM_WORLD
        self.mpi_size = self.mpi_comm.size
        self.mpi_rank = self.mpi_comm.rank
        self.mpi_status = mpi4py.MPI.Status()

        # this must be rank 0 process
        assert self.mpi_rank == 0

        self.executor = mpi4py.futures.MPIPoolExecutor(max_workers=None)

    def submit(self, func, *args, **kwargs):
        """Submit a function: func(*args, **kwargs) and return a FutureJob.
        """
        return self.executor.submit(func, *args, **kwargs)

    def shutdown(self):
        pass


class LocalExecutor(object):
    """Executes jobs in the current process
    """

    def __init__(self):
        super(LocalExecutor, self).__init__()

    def submit(self, func, *args, **kwargs):
        job = LocalFutureJob(func, *args, **kwargs)
        return job


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
        self._condition = threading.Condition()
        self._state = concurrent.futures._base.FINISHED

    def done(self):
        return True

    def result(self, timeout=None):
        return self._result

    def display_outputs(self):
        pass


class FuturesScheduler(_ActorRunner):
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
                self.executor = MultiprocessingExecutor(processes=min_engines)
            elif distributed is not None and executor == 'distributed':
                self.executor = DistributedExecutor(uris=executor_kwargs.get('uris', None),
                                                    min_engines=min_engines,
                                                    timeout=timeout)
            elif ipyparallel is not None and executor == 'ipyparallel':
                kwargs = dict(min_engines=min_engines,
                              timeout=timeout,
                              display_outputs=display_outputs)
                kwargs.update(executor_kwargs)
                self.executor = IpyparallelExecutor(**kwargs)
            elif mpi4py is not None and executor == 'mpi':
                self.executor = MPIExecutor(**executor_kwargs)
            # elif executor == 'scoop':
            #     self.executor = ScoopExecutor()
            else:
                raise ValueError('Executor {} not supported'.format(executor))
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

    def _try_empty_ready_jobs(self):
        if not self.running_actors:
            return

        # TODO could we use callbacks?
        jobs = [job_description['job'] for job_description in self.running_actors.values()]
        # wait for the first completed job
        done, not_done = concurrent.futures.wait(jobs, timeout=None,
                                                 return_when=concurrent.futures.FIRST_COMPLETED)
        for job in done:
            # TODO implement a better way to find actor by its job object
            for actor, job_description in self.running_actors.items():
                if job == job_description['job']:
                    break
            else:
                raise RuntimeError("job's actor not found")
            # delete the completed job from running_actors
            del self.running_actors[actor]
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


class ThreadedSchedulerWorker(threading.Thread, _ActorRunner):
    """Thread object that executes run after run of the ThreadedScheduler actors.
    """

    def __init__(self, scheduler, inner_id):
        threading.Thread.__init__(self)
        self.scheduler = scheduler
        self.executing = False
        self.finished = False
        self.inner_id = inner_id
        self.state_mutex = threading.RLock()

    def run(self):
        from time import sleep
        while not self.finished:
            pv = self.scheduler.pop_idle_task()
            if pv:
                port, value = pv
                should_run = port.put(value)  # Change to if
                if should_run:
                    # print(self.inner_id, port.owner.name, value)
                    self.run_actor(port.owner)
                else:
                    pass
                    # print(self.inner_id, "Won't run", )
                self.scheduler.on_actor_finished(port.owner)
            else:
                pass
                # print(self.inner_id, "Nothing to do")
                sleep(0.02)
                # print(self.inner_id, "End")

    def put_value(self, in_port, value):
        # print(self.inner_id, " worker put ", value)
        self.scheduler.put_value(in_port, value)

    def finish(self):
        """Finish after the current running job is done."""
        with self.state_mutex:
            self.finished = True


class ThreadedScheduler(object):
    def __init__(self, max_threads=2):
        self.max_threads = max_threads
        self.threads = []
        self.execution_queue = deque()
        self.running_actors = []
        self.state_mutex = threading.RLock()

    def copy(self):
        return self.__class__(max_threads=self.max_threads)

    def pop_idle_task(self):
        with self.state_mutex:
            for port, value in self.execution_queue:
                if port.owner not in self.running_actors:
                    # Removes first occurrence - it's probably safe
                    # print("Removing", value)
                    self.execution_queue.remove((port, value))
                    self.running_actors.append(port.owner)
                    return port, value
            else:
                return None

    def put_value(self, in_port, value):
        with self.state_mutex:  # Probably not necessary
            # print("put ", value, ", in queue: ", len(self.queue))
            self.execution_queue.append((in_port, value))

    def is_running(self):
        with self.state_mutex:
            return bool(self.running_actors or self.execution_queue)

    def on_actor_finished(self, actor):
        with self.state_mutex:
            self.running_actors.remove(actor)
            if not self.is_running():
                self.finish_all_threads()

    def execute(self):
        with self.state_mutex:  # Probably not necessary
            for i in range(self.max_threads):
                thread = ThreadedSchedulerWorker(self, i)
                self.threads.append(thread)
                thread.start()
                # print("Thread started", thread.ident)
        for thread in self.threads:
            # print("Join thread")
            thread.join()

    def run_workflow(self, workflow, **kwargs):
        inport_names = tuple(port.name for port in workflow.inports)
        if workflow.scheduler is not None:
            # TODO this seems a bit strange
            scheduler = workflow.scheduler
        else:
            scheduler = self
        for key, value in kwargs.items():
            if key not in inport_names:
                raise ValueError('{} is not an inport name'.format(key))
            inport = workflow.inports[key]
            # put values to connected ports
            scheduler.put_value(inport, kwargs[inport.name])
        # TODO can this be run inside self.execute itsef?
        scheduler.execute()

    def finish_all_threads(self):
        # print("Everything finished. Waiting for threads to end.")
        for thread in self.threads:
            thread.finish()

    def shutdown(self):
        pass
