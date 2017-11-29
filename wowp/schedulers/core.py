"""Basic schedulers that don't require external dependencies."""
from __future__ import absolute_import, division, print_function, unicode_literals

import threading

from collections import deque

from .shared import ActorRunner


class NaiveScheduler(ActorRunner):
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


class LinearizedScheduler(ActorRunner):
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


class ThreadedSchedulerWorker(threading.Thread, ActorRunner):
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
