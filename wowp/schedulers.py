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
import os
import random

try:
    from ipyparallel import Client, RemoteError
except ImportError:
    warnings.warn(
        'ipyparallel not installed: IPyClusterScheduler cannot be used')
try:
    from mpi4py import MPI
except ImportError:
    warnings.warn(
        'mpi4py not installed: mpi4py cannot be used')
from .util import MPI_TAGS, loads, dumps
import itertools


__all__ = [
    "NaiveScheduler",
    "LinearizedScheduler",
    "ThreadedScheduler",
    "IPyClusterScheduler",
    "MultiprocessingExecutor",
    "MultiIpyClusterScheduler",
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

    def __init__(self, uris, min_engines=1, timeout=60):
        from distributed import Executor
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


def mpi_worker():
    """Start a single MPI worker node
    """
    from mpi4py import MPI
    from wowp.util import MPI_TAGS, loads, dumps

    comm = MPI.COMM_WORLD   # get MPI communicator object
    rank = comm.rank        # rank of this process
    status = MPI.Status()   # get MPI status object

    # say hello to the master
    name = MPI.Get_processor_name()
    print("I am a WOW:-P MPI worker with rank %d on %s." % (rank, name))
    msg = {"rank": rank, "name": name, 'pid': os.getpid()}
    comm.send(msg, dest=0, tag=MPI_TAGS.READY)
    myid = 'worker {rank}/{name}/{pid}'.format(**msg)

    # wait for jobs
    while True:
        job_pickle = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()

        if tag == MPI_TAGS.START:
            jobid, job = loads(job_pickle)
            func, args, kwargs = job
            print("{myid}: runnin {func}".format(myid=myid, func=func))
            # Do the work here
            res = func(*args, **kwargs)
            print("{myid}: finished {func}".format(myid=myid, func=func))
            res_pickle = dumps(res)
            comm.send(res_pickle, dest=0, tag=MPI_TAGS.DONE)
            print("{myid}: sending result".format(myid=myid))
        elif tag == MPI_TAGS.EXIT:
            print("{myid}: exit".format(myid=myid))
            break


class MPIExecutor(object):
    """Executes jobs in local subprocesses using concurrent.futures
    """

    _jobid_counter = itertools.count()
    available_workers = []

    def __init__(self):
        self.comm = MPI.COMM_WORLD   # get MPI communicator object
        self.size = self.comm.size        # total number of processes
        self.rank = self.comm.rank        # rank of this process
        self.status = MPI.Status()   # get MPI status object
        # list of workers + their status
        self.workers = {}

        self.num_workers = self.size - 1
        if self.num_workers < 1:
            raise Exception('Not enough MPI workers')
        print("Master starting with %d workers" % self.num_workers)
        # collect worker READY messages
        # TODO choose minimum number and timeout
        while len(self.workers) < self.num_workers:
            data = self.comm.recv(source=MPI.ANY_SOURCE, tag=MPI_TAGS.READY, status=self.status)
            source = self.status.Get_source()
            self.workers[source] = {'online': True, 'ready_message': data}
            logger.info('worker {} in, {}/{}'.format(source, len(self.workers), self.num_workers))
            self.available_workers.append(source)
        self.workers_cycle = itertools.cycle(self.workers.keys())

    def submit(self, func, *args, **kwargs):
        """Submit a function: func(*args, **kwargs) and return a FutureJob.
        """
        # worker = self.available_workers.pop()
        # random choice workers
        # worker = random.choice(self.available_workers)
        # Round-Robin
        worker = next(self.workers_cycle)
        job = (func, args, kwargs)
        # jobid = hash(job)
        jobid = next(self._jobid_counter)
        job_pickle = dumps((jobid, job))
        self.comm.send(job_pickle, dest=worker, tag=MPI_TAGS.START)
        # job = {'worker': worker, 'jobid': jobid}
        return FutureMPIJob(jobid, job, worker, self)

    def shutdown(self):
        """Shut down the workers
        """
        logger.debug('MPI executor shutdown')
        for source, worker in self.workers.items():
            if worker['online']:
                logger.debug('Shut down MPI worker {}'.format(source))
                self.comm.send(None, dest=source, tag=MPI_TAGS.EXIT)
                worker['online'] = False

    def __del__(self):
        self.shutdown()
        if hasattr(super(type(self)), '__del__'):
            super(type(self), self).__del__()


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
                cli = Client(*args, **kwargs)
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

        # print("Run actor {}".format(actor))
        lv = self._ipy_lv[self._rotate_client()]
        job = lv.apply_async(func, *args, **kwargs)
        return FutureIpyJob(job)


class FutureJob(object):
    """An asynchronous job with Future-like API
    """

    def __init__(self, future):
        self._future = future

    def __getattr__(self, item):
        return getattr(self._future, item)

    def display_outpus(self):
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


class FutureMPIJob(object):
    """Wraps asynchronous results of different kinds into a future-like object
    """

    _submitted = {}
    _received = {}

    def __init__(self, jobid, job, worker, executor):
        func, args, kwargs = job
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.worker = worker
        self.started = datetime.datetime.now()
        self.completed = None
        self.engine_id = worker
        self.error = None
        self.executor = executor
        self.request = MPI.COMM_WORLD.irecv(source=worker, tag=MPI_TAGS.DONE)
        self._done = False
        self._result = None

    def done(self):
        if not self._done:
            flag, rmess = self.request.test()
            self._done = flag
            self._result = rmess
        if self._done:
            self.completed = datetime.datetime.now()
            # self.executor.available_workers.append(self.worker)
        return self._done

    def result(self, timeout=None):
        if timeout is not None:
            raise NotImplementedError('finite timeout not implemented yet')
        if self.done():
            res_pickle = self._result
        else:
            res_pickle = self.request.wait()
            self._done = True
            self._result = res_pickle
            self.completed = datetime.datetime.now()
            # self.executor.available_workers.append(self.worker)
        res = loads(res_pickle)
        return res

    def display_outputs(self):
        return 'Sorry, not implemented for MPI :-('


class _IPySystemJob(object):
    """
    System (local run) IPython parallel like job
    """

    def __init__(self, actor, *args, **kwargs):
        self.actor = actor
        self.args = args
        self.kwargs = kwargs
        self.started = datetime.datetime.now()
        self._res = self.actor.run(*self.args, **self.kwargs)
        self.completed = datetime.datetime.now()
        self.engine_id = None
        self.error = None

    def ready(self):
        return True

    def get(self):
        return self._res

    def display_outputs(self):
        pass


class IPyClusterScheduler(_ActorRunner):
    """
    Scheduler using ipyparallel cluster.

    Args:
        display_outputs (Optional[bool]): display stdout/err from actors [False]
        timeout (Optional): timeout in secs for waiting for ipyparallel cluster [60]
        min_engines (Optional[int]): minimum number of engines [1]
        *args: passed to self.init_cluster(*args, **kwargs)
        **kwargs: passed to self.init_cluster(*args, **kwargs)
    """

    def __init__(self, *args, **kwargs):
        self.process_pool = []
        # actor: job
        self._init_args = args
        self._init_kwargs = kwargs
        self.display_outputs = kwargs.pop('display_outputs', False)
        self.timeout = kwargs.pop('timeout', 60)
        self.min_engines = kwargs.pop('min_engines', 1)
        self.running_actors = {}
        self.execution_queue = deque()
        self.wait_queue = []
        self._ipy_rc = self.init_cluster(self.min_engines, self.timeout, *args,
                                         **kwargs)
        self._ipy_dv = self._ipy_rc[:]
        self._ipy_lv = self._ipy_rc.load_balanced_view()

    def copy(self):
        return self.__class__(*self._init_args, **self._init_kwargs)

    def put_value(self, in_port, value):
        self.execution_queue.appendleft((in_port, value))

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
                cli = Client(*args, **kwargs)
            except Exception as e:
                if time.time() > maxtime:
                    # raise the original exception from ipyparallel
                    raise e
                else:
                    # sleep and try again to get the client
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
            time.sleep(timeout * 0.1)

        return cli

    def execute(self):

        self.reset()

        while self.execution_queue or self.running_actors or self.wait_queue:
            self.nothing = True

            self._try_empty_execution_queue()
            self._try_empty_wait_queue()
            self._try_empty_ready_jobs()

            # TODO fix against spamming engines - PROBABLY NOT THE BEST WAY!
            if self.nothing:
                # increase sleep time if nothing is happening
                self.last_sleep = min(1, max(0.001, self.last_sleep * 2))
                logger.debug('scheduler sleeps for {} ms'.format(
                    self.last_sleep * 1e3))
                time.sleep(self.last_sleep)
            else:
                self.last_sleep = 0

    def _try_empty_ready_jobs(self):
        pending = {}  # temporary container
        for actor, job_description in self.running_actors.items():

            job = job_description['job']

            if 'started' not in job_description and job_description[
                    'job'].started:
                # log the started time
                job_description['started'] = job_description['job'].started
                job_description['engine'] = job_description['job'].engine_id
                logger.debug(
                    "started {started} actor {actor}({args}, {kwargs})"
                    " on engine {engine}".format(actor=actor.name,
                                                 **job_description))
            if job.ready():
                self.nothing = False
                if 'started' not in job_description and job_description[
                        'job'].started:
                    # log the started time
                    job_description['started'] = job_description['job'].started
                    job_description['engine'] = job_description[
                        'job'].engine_id
                    logger.debug(
                        "started {started} actor {actor}({args}, {kwargs})"
                        " on engine {engine} at {started}".format(
                            actor=actor.name,
                            **job_description))

                job_description['completed'] = job_description['job'].completed
                logger.debug('completed actor {actor} at {completed}'.format(
                    actor=actor.name,
                    **job_description))

                # process result
                # raise RemoteError in case of failure
                try:
                    result = job.get()
                except RemoteError:
                    logger.error('actor {} failed\n{}'.format(
                        actor.name, job_description['job'].error))
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

    def run_actor(self, actor):
        # print("Run actor {}".format(actor))
        actor.scheduler = self
        args, kwargs = actor.get_run_args()
        # system actors must be run within this process
        res = dict(args=args, kwargs=kwargs)
        if actor.system_actor:
            res['job'] = _IPySystemJob(actor, *args, **kwargs)
        else:
            res['job'] = self._ipy_lv.apply_async(actor.run, *args, **kwargs)

        logger.debug('submitted actor {}({}, {})'.format(actor.name, args,
                                                         kwargs))

        return res


class MultiIpyClusterScheduler(IPyClusterScheduler):
    """Scheduler using multiple ipyparallel clusters.

    Can use more than 1 ipyparallel cluster in order to avoid limitations for the number of engines.
    Either profiles or profile_dirs must be specified.

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
        self.timeout = timeout
        self.min_engines = min_engines
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
            raise ValueError(
                'Either profiles or profile_dirs must be specified')

        for value in cli_args:
            # init ipyparallel clients
            kwargs = {cli_arg: value}
            if client_kwargs:
                kwargs.update(client_kwargs)
            self._ipy_rc.append(self.init_cluster(min_engines, timeout, **
                                                  kwargs))
            self._ipy_dv.append(self._ipy_rc[-1][:])
            self._ipy_lv.append(self._ipy_rc[-1].load_balanced_view())

        self.running_actors = {}
        self.execution_queue = deque()
        self.wait_queue = []

    def _rotate_client(self):
        current = self._current_cli
        self._current_cli += 1
        if self._current_cli == len(self._ipy_rc):
            self._current_cli = 0
        return current

    def run_actor(self, actor):
        # print("Run actor {}".format(actor))
        actor.scheduler = self
        args, kwargs = actor.get_run_args()
        # system actors must be run within this process
        res = dict(args=args, kwargs=kwargs)
        lv = self._ipy_lv[self._rotate_client()]
        if actor.system_actor:
            res['job'] = _IPySystemJob(actor, *args, **kwargs)
        else:
            res['job'] = lv.apply_async(actor.run, *args, **kwargs)

        logger.debug('submitted actor {}({}, {})'.format(actor.name, args,
                                                         kwargs))

        return res


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
            elif executor == 'distributed':
                self.executor = DistributedExecutor(uris=executor_kwargs['uris'],
                                                    min_engines=min_engines,
                                                    timeout=timeout)
            elif executor == 'ipyparallel':
                kwargs = dict(min_engines=min_engines,
                              timeout=timeout,
                              display_outputs=display_outputs)
                kwargs.update(executor_kwargs)
                self.executor = IpyparallelExecutor(**kwargs)
            elif executor == 'mpi':
                self.executor = MPIExecutor()
            # elif executor == 'scoop':
            #     self.executor = ScoopExecutor()
            else:
                raise ValueError('Executor {} unknown'.format(executor))
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
        self.last_sleep = 0

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

        logger.debug('submitted actor {}({}, {})'.format(actor.name, args,
                                                         kwargs))

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
                # increase sleep time if nothing is happening
                self.last_sleep = min(1, max(0.001, self.last_sleep * 2))
                logger.debug('scheduler sleeps for {} ms'.format(
                    self.last_sleep * 1e3))
                time.sleep(self.last_sleep)
            else:
                self.last_sleep = 0

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

    def finish_all_threads(self):
        # print("Everything finished. Waiting for threads to end.")
        for thread in self.threads:
            thread.finish()

    def shutdown(self):
        pass
