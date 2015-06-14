from .util import ListDict, deprecated
from collections import deque
from . import logger
from .schedulers import NaiveScheduler, LinearizedScheduler

import networkx as nx
import functools
import keyword


__all__ = "Component", "Actor", "Workflow", "Composite", "draw_graph"


class NoValue(object):

    """A unique no value object

    Note that we cannot use None as this can be used by users
    """

    def __init__(self):
        raise Exception('NoValue cannot be instantiated')


def has_value(value):
    """Returns True for any value except NoValue
    """
    return value is not NoValue


class Component(object):

    """Base WOWP component class
    """

    def __init__(self, name=None, scheduler=NaiveScheduler()):
        if name is None:
            name = self.__class__.__name__.lower()
        self.name = name
        self.scheduler = scheduler
        self._inports = Ports(InPort, self)
        self._outports = Ports(OutPort, self)

    def fire(self):
        """This is a virtual method
        """
        raise NotImplementedError('Calling a virtual method')

    def can_run(self):
        # print("on_input", all(not port.isempty() for port in self.inports))
        return all(not port.isempty() for port in self.inports)

    def run(self):
        """
        Run the actor
        """
        # first get the result of the fire method
        res = self.fire()
        if res is None:
            res = {}
        # put values to output ports
        out_names = self.outports.keys()
        if not hasattr(res, 'items'):
            raise ValueError('The fire method must return a dict-like object with items method')
        for name, value in res.items():
            if name in out_names:
                self.outports[name].put(value)
            else:
                raise ValueError("{} not in output ports".format(name))

    @property
    def inports(self):
        return self._inports

    @inports.setter
    def inports(self, value):
        raise TypeError('Cannot set inports directly')

    @inports.deleter
    def inports(self):
        raise TypeError('Cannot delete inports directly')

    @property
    def outports(self):
        return self._outports

    @outports.setter
    def outports(self, value):
        raise TypeError('Cannot set outports directly')

    @outports.deleter
    def outports(self):
        raise TypeError('Cannot delete outports directly')

    def put_outputs(self, **kwargs):
        """Put outputs to ports, specified by port_name, value pairs
        """
        for port_name, value in kwargs.items():
            self.outports[port_name].put(value)

    @property
    def graph(self):
        """Construct NetworX call graph
        """
        return build_nx_graph(self)


# TODO create workflow (composite) from actor's connections
    def get_workflow(self):
        '''Creates a workflow form actor's connections
        '''
        raise NotImplementedError('TBI')


class Actor(Component):

    """Actor class
    """

    def __call__(self, **kwargs):
        """
        Run the component with input ports filled from keyword arguments.

        :param kwargs: input ports values
        :return: output port(s) value(s)
        :rtype: dict for multiple ports
        """

        for inport in self.inports:
            if inport.name in kwargs:
                inport.buffer.appendleft(kwargs[inport.name])
        # run the actor
        res = self.fire()
        return res


class Composite(Component):

    """Composite = a group of actors
    """

    def __call__(self, scheduler_=None, **kwargs):
        """
        Run the component with input ports filled from keyword arguments.

        :param scheduler_: execution scheduler (defaul
        :param kwargs: input ports values
        :return: output port(s) value(s)
        :rtype: dict for multiple ports
        """

        if scheduler_ is None:
            scheduler_ = LinearizedScheduler()
        # TODO
        for inport in self.inports:
            if inport.name in kwargs:
                inport.buffer.appendleft(kwargs[inport.name])
        # run the actor
        # vnutit scheduler
        # zavolat pro vsechny on_input
        res = self.fire()
        # TODO return res
        return res

    def fire(self):
        # TODO
        pass


class Workflow(Composite):

    """Workflow class
    """
    pass


class Ports(object):

    """Port collection
    """

    def __init__(self, port_class, owner):
        # TODO port_class can differ for individual ports
        self._ports = ListDict()
        # port class is used to create new ports
        self._port_class = port_class
        self._owner = owner

    def isempty(self):
        return bool(self._ports)

    def __len__(self):
        return len(self._ports)

    def __iter__(self):
        return iter(self._ports.values())

    def __new_port(self, name, **kwargs):
        return self._port_class(name=name, owner=self._owner, **kwargs)

    def __getitem__(self, item):
        """

        :rtype: Port
        """
        # TODO add security
        return self._ports[item]

    def __getattr__(self, item):
        # TODO add security
        return self._ports[item]

    def __setitem__(self, key, value):
        # must be implemented for +=, -= operators
        # TODO add security
        self._ports[key] = value

    def insert_after(self, existing_port_name, new_port_name):
        self._ports.insert_after(existing_port_name,
                                 (new_port_name, self.__new_port(new_port_name)))

    def append(self, new_port_name, **kwargs):
        self._ports[new_port_name] = self.__new_port(new_port_name, **kwargs)

    def keys(self):
        return list(self._ports.keys())


class Port(object):

    """Represents a single input/output actor port
    """

    def __init__(self, name, owner, persistent=False, default=NoValue):
        self.name = name
        self.owner = owner
        self.persistent = persistent
        self.buffer = deque()
        self._default = default
        self._connections = []
        self._last_value = NoValue

    @property
    def default(self):
        if has_value(self._default):
            return self._default
        else:
            raise AttributeError('No default value specified')

    @default.setter
    def default(self, value):
        self._default = value

    @default.deleter
    def default(self):
        self._default = NoValue

    @property
    def connections(self):
        return self._connections

    def is_connected_to(self, other):
        return self in other.connections

    def connect(self, other):
        if isinstance(self, OutPort):
            assert isinstance(other, InPort)
        else:
            assert isinstance(other, OutPort)
        if other not in self._connections:
            # cannot use connect as it created an infinite recursion
            self._connections.append(other)
            # TODO this creates a circular reference - is it a good idea?
            other._connections.append(self)
        else:
            logger.warn('connecting an already connected actor {}'.format(other))

    @deprecated
    def __bool__(self):
        """True if the port buffer is not empty
        """
        return not self.isempty()

    def isempty(self):
        """True if the port buffer is empty
        """
        if self.buffer or has_value(self._default) or (self.persistent and has_value(self._last_value)):
            return False
        else:
            return True

    def disconnect(self, other):
        if other not in self._connections:
            logger.warn('actor {} not currently connected'.format(other))
        else:
            other._connections.remove(self)
            self._connections.remove(other)

    def pop(self):
        """Get single input
        """
        res = NoValue
        if self.buffer:
            # input item is in the buffer
            res = self.buffer.pop()
            if self.persistent:
                self._last_value = res
        elif self.persistent and has_value(self._last_value):
            # persistent port, last value exists
            res = self._last_value
        elif has_value(self._default):
            # port with default value
            res = self._default
        # chack whether any result value is available
        if has_value(res):
            return res
        else:
            raise IndexError('Port buffer is empty')

    def pop_all(self):
        """Get all values
        """
        values = self.buffer
        self.buffer = deque()
        return values


class OutPort(Port):

    """A single, named output port
    """

    def put(self, value):
        """Put output value

        Value is sent to connected ports (or stored if not connected)
        """
        self.buffer.appendleft(value)


class InPort(Port):

    """A single, named input port
    """

    def __iadd__(self, other):
        self.connect(other)
        # self must be returned because __setattr__ or __setitem__ is finally used
        return self

    def __isub__(self, other):
        self.disconnect(other)
        # self must be returned because __setattr__ or __setitem__ is finally used
        return self

    def put(self, value):
        """Put single input

        :rtype: bool
        :return: Whether the actor is ready to perform
        """
        self.buffer.appendleft(value)
        return self.owner.can_run()


def valid_name(name):
    """Validate name (for actors, ports etc.)
    """
    if name.isidentifier() and not keyword.iskeyword(name):
        return True
    else:
        return False


def build_nx_graph(actor):
    """Create graph with all actors + ports as nodes.

    It walks over all connections.

    Prerequisities:
    * networkx package
    """
    actors = []
    edges = []
    ports = []
    graph = nx.DiGraph()

    def _get_name(obj):
        return str(hash(obj))

    def _add_actor_node(actor):
        attrs = {"fontsize": "12", "color": "#0093d0"}
        graph.add_node(_get_name(actor), label=actor.name, shape="box", **attrs)
        actors.append(actor)

    def _walk_node(actor):
        name = _get_name(actor)
        if actor not in actors:
            _add_actor_node(actor)
        for port in actor.outports:
            if port not in ports:
                attrs = {}
                if not port.connections:
                    # terminal node
                    attrs["style"] = "filled"
                    attrs["color"] = "#ef4135"
                else:
                    attrs["color"] = "#ffe28a"
                graph.add_node(_get_name(port), label=port.name, **attrs)
                graph.add_edge(name, _get_name(port))
            for other in port.connections:
                if (port, other) not in edges:
                    edges.append((port, other))
                    _walk_node(other.owner)
                    graph.add_edge(_get_name(port), _get_name(other))
        for port in actor.inports:
            if port not in ports:
                attrs = {}
                if not port.connections:
                    # terminal node
                    attrs["style"] = "filled"
                    attrs["color"] = "#ffffff"
                else:
                    attrs["color"] = "#9ed8f5"
                graph.add_node(_get_name(port), label=port.name, **attrs)
                graph.add_edge(_get_name(port), name, )
                ports.append(port)
            for other in port.connections:
                if (other, port) not in edges:
                    _walk_node(other.owner)

    _walk_node(actor)
    return graph


def draw_graph(graph, layout='spectral', with_labels=True, node_size=500,
               pos_kwargs=None, draw_kwargs=None):
    """Draw a workflow graph using NetworkX
    """
    kwargs = {}
    if pos_kwargs is not None:
        kwargs.update(pos_kwargs)
    if layout == 'spectral':
        lfunc = functools.partial(nx.spectral_layout, **kwargs)
    else:
        raise ValueError('{} layout not supported'.format(layout))
    # get colors and labels
    colors = [graph.node[n].get('color', '#ffffff') for n in graph.nodes_iter()]
    shapes = [graph.node[n].get('shape', 'o') for n in graph.nodes_iter()]
    labels = {n: graph.node[n].get('label', '') for n in graph.nodes_iter()}
    pos = lfunc(graph)
    nx.draw_networkx(graph, pos=pos, with_labels=with_labels, labels=labels,
                     node_color=colors, node_size=node_size)
