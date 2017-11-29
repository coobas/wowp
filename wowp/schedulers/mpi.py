"""Executor based on MPI"""
from __future__ import absolute_import, division, print_function, unicode_literals

import datetime
import itertools

from mpi4py import MPI

from wowp.util import MPI_TAGS, loads, dumps
from wowp.logger import logger


def mpi_worker():
    """Start a single MPI worker node
    """
    from mpi4py import MPI

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