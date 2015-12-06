from __future__ import absolute_import, division, print_function
from ..components import Actor
from future.builtins import super


class Concat(Actor):
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

    def __init__(self, actor, args=(), kwargs={}, scheduler=None, name='map'):
        super().__init__(name=name)
        self.actor = actor
        self.actor_args = args
        self.actor_kwargs = kwargs
        self.map_scheduler = scheduler
        self.inports.append('inp')
        self.outports.append('out')

    def get_run_args(self):
        return (), {}

    def run(self, *args, **kwargs):
        # run is not a classfunction for Map
        # bacause it needs to change the workflow
        if self.map_scheduler is None:
            # self.schduler is set by the calling scheduler
            map_scheduler = self.scheduler
        else:
            map_scheduler = self.map_scheduler
        destinations = [port for port in self.outports['out'].connections]
        # disconnect the output port
        # for port in destinations:
        #     self.outports['out'].disconnect(port)
        # create the map actors, connect and put inputs
        map_actors = []
        items = self.inports['inp'].pop()
        concat_actor = Concat(n_ports=len(items))
        for i, item in enumerate(items):
            actor = self.actor(*self.actor_args, **self.actor_kwargs)
            if not map_actors:
                # the first item - do some extra work
                assert len(actor.inports) == 1
                assert len(actor.outports) == 1
                inport_name = actor.inports.keys()[0]
                outport_name = actor.outports.keys()[0]
            map_actors.append(actor)
            # append to concatenate actor
            concat_actor.connect_input(actor.outports[outport_name])
            # put the input data item
            map_scheduler.put_value(actor.inports[inport_name], item)
            # actor.inports[inport_name].put(item)
        # run the mapping sub-workflows
        # map_workflow = concat_actor.get_workflow()
        # result = map_scheduler.run_workflow(map_workflow)
        map_scheduler.execute()
        # result = (actor.outports[outport_name].pop() for actor in map_actors)
        result = concat_actor.outports['out'].pop()

        return result
