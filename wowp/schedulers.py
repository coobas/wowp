from collections import deque
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import random


class NaiveScheduler(object):
    """Scheduler that directly calls connected actors.

    Problem: recursion quickly ends in full call stack.
    """
    def put_value(self, in_port, value):
        in_port.put(value)


class LinearizedScheduler(object):
    """Scheduler that stacks all inputs in a queue and executes them in FIFO order."""
    def __init__(self):
        self.execution_queue = deque()

    def put_value(self, in_port, value):
        in_port.owner.scheduler = self
        self.execution_queue.appendleft((in_port, value))

    def execute(self):
        while self.execution_queue:
            in_port, value = self.execution_queue.pop()
            in_port.put(value)


class RandomScheduler(LinearizedScheduler):
    """Scheduler that queues inputs but inserts them in random order."""
    def execute(self):
        while self.execution_queue:
            self.execution_queue.rotate(random.randint(0, len(self.execution_queue)))
            in_port, value = self.execution_queue.pop()
            in_port.put(value)


class _ConcurrentScheduler(object):
    def __init__(self, executor_class, max_threads=4):
        self.executor = executor_class(max_workers=max_threads)
        self.running = False
        self.begin_queue = deque()

    def put_value(self, in_port, value):
        if self.running:
            self.executor.submit(in_port.put, value)
        else:
            self.begin_queue.appendleft((in_port, value))

    def execute(self):
        self.running = True
        with self.executor:
            while len(self.begin_queue):
                in_port, value = self.begin_queue.pop()
                self.put_value(in_port, value)
        self.running = False

class ThreadedScheduler(_ConcurrentScheduler):
    """Scheduler that uses thread pool from concurrent.futures module."""
    def __init__(self,  max_threads=4):
        super(ThreadedScheduler, self).__init__(ThreadPoolExecutor, max_threads=max_threads)