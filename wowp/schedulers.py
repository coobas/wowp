from collections import deque


class NaiveScheduler(object):
    def put_value(self, in_port, value):
        in_port.put(value)


class LinearizedScheduler(object):
    def __init__(self):
        self.execution_queue = deque()

    def put_value(self, in_port, value):
        in_port.owner.scheduler = self
        self.execution_queue.appendleft((in_port, value))

    def execute(self):
        while self.execution_queue:
            in_port, value = self.execution_queue.pop()
            in_port.put(value)

