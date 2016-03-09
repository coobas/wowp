from __future__ import absolute_import, division, print_function, unicode_literals
from collections import deque
import threading
import warnings
import wowp.components


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


class _IPySystemJob(object):
    """
    System (local run) IPython parallel like job
    """
    def __init__(self, actor, *args, **kwargs):
        self.actor = actor
        self.args = args
        self.kwargs = kwargs

    def ready(self):
        return True

    def get(self):
        return self.actor.run(*self.args, **self.kwargs)


class IPyClusterScheduler(_ActorRunner):
    """
    Scheduler using IPython Cluster.

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
        self.display_outputs = kwargs.pop('display_outputs', False)
        self.timeout = kwargs.pop('timeout', 60)
        self.min_engines = kwargs.pop('min_engines', 1)
        self.running_actors = {}
        self.execution_queue = deque()
        self.wait_queue = []
        self.init_cluster(*args, **kwargs)

    def copy(self):
        return self.__class__(max_procs=self.max_procs)

    def put_value(self, in_port, value):
        self.execution_queue.appendleft((in_port, value))

    def init_cluster(self, *args, **kwargs):
        '''Get a connection (view) to an IPython cluster

        Args:
            *args: passed to ipyparallel.Client(*args, **kwargs)
            **kwargs: passed to ipyparallel.Client(*args, **kwargs)
        '''

        from ipyparallel import Client
        import time

        maxtime = time.time() + self.timeout

        while True:
            try:
                cli = Client(*args, **kwargs)
            except Exception as e:
                if time.time() > maxtime:
                    # raise the original exception from ipyparallel
                    raise e
                else:
                    # sleep and try again to get the client
                    time.sleep(self.timeout * 0.1)
                    continue
            if len(cli.ids) >= self.min_engines:
                # we have enough clients
                break
            else:
                # free the client
                cli.close()
            if time.time() > maxtime:
                raise Exception('Not enough ipyparallel clients')
            # try ~10 times
            time.sleep(self.timeout * 0.1)

        self._ipy_rc = cli
        self._ipy_dv = self._ipy_rc[:]
        self._ipy_lv = self._ipy_rc.load_balanced_view()

    def execute(self):

        while self.execution_queue or self.running_actors or self.wait_queue:
            self._try_empty_execution_queue()
            self._try_empty_wait_queue()
            self._try_empty_ready_jobs()

    def _try_empty_ready_jobs(self):
        pending = {}  # temporary container
        for actor, job in self.running_actors.items():
            if job.ready():
                # process result
                # raise RemoteException in case of failure
                result = job.get()
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
                pending[actor] = job
        self.running_actors = pending

    def _try_empty_wait_queue(self):
        pending = []  # temporary container
        for actor in self.wait_queue:
            # run actors only if not already running
            if actor not in self.running_actors:
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
                # waiting to be run
                self.wait_queue.append(in_port.owner)
                # self.running_actors((in_port.owner, self.run_actor(in_port.owner)))

    def run_actor(self, actor):
        # print("Run actor {}".format(actor))
        actor.scheduler = self
        args, kwargs = actor.get_run_args()
        # system actors must be run within this process
        if actor.system_actor:
            return _IPySystemJob(actor, *args, **kwargs)
        else:
            return self._ipy_lv.apply_async(actor.run, *args, **kwargs)


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
