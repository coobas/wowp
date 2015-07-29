from collections import deque
import threading
import warnings


class _ActorRunner(object):

    def on_outport_put_value(self, outport):
        if outport.connections:
            value = outport.pop()
            for inport in outport.connections:
                self.put_value(inport, value)

    def run_actor(self, actor):
        # print("Run actor")
        result = actor.run()
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


class IPyClusterScheduler(_ActorRunner):

    """
    Scheduler using IPython Cluster.
    """

    def __init__(self, profile=None):
        self.process_pool = []
        self.running_actors = []
        self.execution_queue = deque()
        self.init_cluster(profile)

    def copy(self):
        return self.__class__(max_procs=self.max_procs)

    def put_value(self, in_port, value):
        self.execution_queue.appendleft((in_port, value))

    def init_cluster(self, profile):
        '''Get a connection (view) to an IPython cluster
        '''

        from IPython.parallel import Client

        self._ipy_rc = Client(profile=profile)
        self._ipy_dv = self._ipy_rc[:]
        self._ipy_lv = self._ipy_rc.load_balanced_view()

    def execute(self):

        while self.execution_queue or self.running_actors:
            while self.execution_queue:
                in_port, value = self.execution_queue.pop()
                should_run = in_port.put(value)
                if should_run:
                    self.running_actors.append((in_port.owner, self.run_actor(in_port.owner)))
            pending = []
            for actor, job in self.running_actors:
                if job.ready():
                    # process result
                    # raise RemoteException in case of failure
                    result = job.get()
                    if result:
                        # empty results don't need any processing
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

                else:
                    pending.append((actor, job))
            self.running_actors = pending

    def run_actor(self, actor):
        # print("Run actor {}".format(actor))
        args, kwargs = actor.get_args()
        return self._ipy_lv.apply_async(actor.get_result, *args, **kwargs)


class SchedulerWorker(threading.Thread, _ActorRunner):

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
        print(self.inner_id, "End")

    def put_value(self, in_port, value):
        # print(self.inner_id, " worker put ", value)
        self.scheduler.put_value(in_port, value)

    def finish(self):
        with self.state_mutex:
            self.finished = True


class ThreadedScheduler(object):

    def __init__(self, max_threads=2):
        self.max_threads = max_threads
        self.threads = []
        self.queue = deque()
        self.running_actors = []
        self.state_mutex = threading.RLock()

    def copy(self):
        return self.__class__(max_threads=self.max_threads)

    def pop_idle_task(self):
        with self.state_mutex:
            for port, value in self.queue:
                if port.owner not in self.running_actors:
                    # Removes first occurrence - it's probably safe
                    # print("Removing", value)
                    self.queue.remove((port, value))
                    self.running_actors.append(port.owner)
                    return port, value
            else:
                return None

    def put_value(self, in_port, value):
        with self.state_mutex:     # Probably not necessary
            # print("put ", value, ", in queue: ", len(self.queue))
            self.queue.append((in_port, value))

    def is_running(self):
        with self.state_mutex:
            return bool(self.running_actors or self.queue)

    def on_actor_finished(self, actor):
        with self.state_mutex:
            self.running_actors.remove(actor)
            if not self.is_running():
                self.finish_all_threads()

    def execute(self):
        with self.state_mutex:     # Probably not necessary
            for i in range(self.max_threads):
                thread = SchedulerWorker(self, i)
                self.threads.append(thread)
                thread.start()
                # print("Thread started", thread.ident)
        for thread in self.threads:
            print("Join thread")
            thread.join()

    def finish_all_threads(self):
        print("Everything finished. Waiting for threads to end.")
        for thread in self.threads:
            thread.finish()
