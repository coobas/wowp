from __future__ import absolute_import, division, print_function

import itertools

from ..components import Actor
from ..schedulers import _ActorRunner, ThreadedScheduler
from future.builtins import super


class Concat(Actor):

    _system_actor = True

    def __init__(self, n_ports, name='concat'):
        super().__init__(name=name)
        for i in range(n_ports):
            self.inports.append('in_{}'.format(i))
        self.outports.append('out')
        self._last_connected = -1

    def get_run_args(self):
        return (), {}

    def run(self, *args, **kwargs):
        res = tuple((port.pop() for port in self.inports))
        return {'out': res}

    def connect_input(self, port, i=None):
        """
        Connects i-th input. If i is None, the last value + 1 is used.

        Args:
            port (OutPort): output port to connect
            i (int): input port index
        """

        if i is None:
            i = self._last_connected + 1
        self._last_connected = i
        self.inports['in_{}'.format(i)].connect(port)


class MultiConcat(Actor):

    _system_actor = True

    def __init__(self, name='multiconcat'):
        super().__init__(name=name)
        self._port_names = []
        self._last_connected = -1

    def get_run_args(self):
        return (), {}

    def run(self, *args, **kwargs):
        res = {}
        for inport, outport in zip(self.inports, itertools.cycle(self.outports)):
            res.setdefault(outport.name, [])
            res[outport.name].append(inport.pop())
        return res

    def add_and_connect(self, actor):
        if self._last_connected > -1:
            assert self._port_names == [port.name for port in actor.outports]
        self._last_connected += 1
        i = self._last_connected
        for port in actor.outports:
            self.inports.append('{}_{}'.format(port.name, i))
            self.inports.at(-1).connect(port)
            if i == 0:
                self.outports.append(port.name)
                self._port_names.append(port.name)


class Map(Actor):
    """
    Maps a given actor on input token elements.

    The mapped actor is assumed to have a single input and a single output port.

    Args:
        actor (Actor): Actor class to be mapped to inputs
        args (list): positional arguments for actor.__init__
        kwargs (dict): keyword arguments for actor.__init__
        scheduler (Scheduler): scheduler to use, default is not to change the scheduler
        name (string): actor name

    Ports:
        in (iterable): contains items to be passed to the mapped actor
        out (tuple): items after applying the map actor
    """

    _system_actor = True

    def __init__(self, actor_class, args=(), kwargs={}, scheduler=None, name='map'):
        super().__init__(name=name)
        self.actor_class = actor_class
        self.actor_args = args
        self.actor_kwargs = kwargs
        self.map_scheduler = scheduler
        # get port names from an actor instance
        actor = self.actor_class(*self.actor_args, **self.actor_kwargs)
        for pname in actor.inports.keys():
            self.inports.append(pname)
        for pname in actor.outports.keys():
            self.outports.append(pname)

    def get_run_args(self):
        return (), {}

    def run(self, *args, **kwargs):
        # run is not a classfunction for Map
        # bacause it needs to change the workflow
        if self.map_scheduler is None:
            # self.schduler is set by the calling scheduler
            map_scheduler = self.scheduler.copy()
        elif isinstance(self.map_scheduler, (_ActorRunner, ThreadedScheduler)):
            # we need a copy of an existing scheduler
            map_scheduler = self.map_scheduler.copy()
        else:
            # in this case, we assume self.map_scheduler is a class
            map_scheduler = self.map_scheduler()
        # destinations = [port for port in self.outports['out'].connections]
        # disconnect the output port
        # for port in destinations:
        #     self.outports['out'].disconnect(port)
        # create the map actors, connect and put inputs
        map_actors = []
        concat_actor = MultiConcat()
        # items will iterate over all ports inputs, i.e. will contain n-th actor input
        # TODO ensure equal input lengths
        for items in zip(*(port.pop() for port in self.inports)):
            # get actor instance
            actor = self.actor_class(*self.actor_args, **self.actor_kwargs)
            map_actors.append(actor)
            concat_actor.add_and_connect(actor)
            # put the input data item
            for port, value in zip(actor.inports, items):
                # value is a single input item of the port
                map_scheduler.put_value(port, value)
        # run the mapping sub-workflows
        # map_workflow = concat_actor.get_workflow()
        # result = map_scheduler.run_workflow(map_workflow)
        map_scheduler.execute()

        result = {port.name: port.pop() for port in concat_actor.outports}
        return result


class PassWID(Actor):
    """
    Pass input argument with hostname and process ID attached
    """
    def __init__(self, name='passwid'):
        super().__init__(name=name)
        self.inports.append('inp')
        self.outports.append('out')

    def get_run_args(self):
        return (self.inports['inp'].pop(), ), {}

    @staticmethod
    def run(*args, **kwargs):
        from os import getpid
        from socket import gethostname
        return {'out':
                    {'inp': args[0],
                     'host': gethostname(),
                     'pid': getpid()}}
