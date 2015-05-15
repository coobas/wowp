from collections import deque
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

