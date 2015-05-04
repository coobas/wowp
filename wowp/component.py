import logging
from .util import ListDict

# set up logger
logger = logging.getLogger()


class Component(object):
    """Base WOWP component class
    """

    def __init__(self, name=None):
        if name is None:
            name = self.__class__.__name__.lower()
        self.name = name
        self._inports = Ports(InPort, self)
        self._outports = Ports(OutPort, self)

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


class Actor(Component):
    pass


class Composite(Component):
    pass


class Workflow(Composite):
    pass


class Ports(object):
    """Port collection
    """
    def __init__(self, port_class, actor, type=None):
        self._ports = ListDict()
        # port class is used to create new ports
        self._port_class = port_class
        self._actor = actor
        self._type = type

    def __new_port(self, name):
        return self._port_class(name=name, actor=self._actor, type=self._type)

    def __getitem__(self, item):
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
        self._ports.insert_after(existing_port_name, (new_port_name, self.__new_port(new_port_name)))

    def append(self, new_port_name):
        self._ports[new_port_name] = self.__new_port(new_port_name)


class Port(object):
    def __init__(self, name, actor, type=None):
        self.name = name
        self.actor = actor
        self.type = type
        self._connections = []

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

    def disconnect(self, other):
        if other not in self._connections:
            logger.warn('actor {} not currently connected'.format(other))
        else:
            other._connections.remove(self)
            self._connections.remove(other)


class OutPort(Port):
    """A single, named output port
    """
    pass


class InPort(Port):
    """A single, named input port
    """
    def __init__(self, name, actor, type=None):
        self.name = name
        self.actor = actor
        self.type = type
        self._connections = []

    def __iadd__(self, other):
        self.connect(other)
        # self must be returned because __setattr__ or __setitem__ is finally used
        return self

    def __isub__(self, other):
        self.disconnect(other)
        # self must be returned because __setattr__ or __setitem__ is finally used
        return self


def valid_name(name):
    """Validate name (for actors, ports etc.)
    """
    try:
        # name must be a string
        assert isinstance(name, str)
        # non-empty
        assert name
        # first is an alpha char
        assert name[0].isalpha()
        # only alphanumeric chars or _ or space
        assert all((ch.isalnum() or ch in ('_', ' ') for ch in name[1:-1]))
        assert name[-1].isalnum() or name[-1] in ('_', )
    except AssertionError:
        return False
    return True


# class Composite(Component):
#     """Composite class is used for workflows and composite actors"""
#     def __init__(self, name=None):
#         super(Composite, self).__init__(name=name)
#
#     def add(self, actor):
#         self.components.append(actor)
#
#     def build_graph(self):
#         self.graph = nx.DiGraph()
#         for phase in (0, 1):
#             for actor in self.components:
#                 if phase == 0:
#                     self.graph.add_node(actor.id, type='a')
#                     for in_port in actor.input_ports():
#                         port_node = '%s:%s' % (actor.id, in_port)
#                         self.graph.add_node(port_node, type='i')
#                         self.graph.add_edge(port_node, actor.id)
#                         print('%s -> %s' % (port_node, actor.id))
#                     for out_port in actor.output_ports():
#                         port_node = '%s:%s' % (actor.id, out_port)
#                         self.graph.add_node(port_node, type='o')
#                         self.graph.add_edge(actor.id, port_node)
#                         print('%s -> %s' % (actor.id, port_node))
#                 if phase == 1:
#                     for out_port, conns in actor.connections.iteritems():
#                         source_node = '%s:%s' % (actor.id, out_port)
#                         for conn in conns:
#                             dest_node = '%s:%s' % (conn['actor'].id, conn['port'])
#                             self.graph.add_edge(source_node, dest_node)
#                             print('%s -> %s' % (source_node, dest_node))
#
#     def draw_graph(self):
#         import matplotlib.pyplot as plt
#         # pos = nx.graphviz_layout(self.graph, prog='dot')
#         colors = {'a': 'r', 'i': 'g', 'o': 'y'}
#         node_color = [colors[self.graph.node[n]['type']] for n in self.graph.nodes()]
#         plt.figure()
#         # nx.draw(self.graph, pos=pos, node_color=node_color)
#         nx.draw(self.graph, node_color=node_color)
#         plt.show()


# class Actor(object):
#     """Actor base class"""
#
#     count = 0
#
#     def __init__(self, name=None):
#         super(Actor, self).__init__()
#         self.__class__.count += 1
#         self.id = "%s/%i" % (self.__class__.__name__, self.__class__.count)
#         self.name = name if name is not None else self.id
#         self.logger = logging.getLogger(self.id)
#         self.connections = {}
#         self.inputs = {}
#
#     def put_input(self, port, value):
#         self.logger.debug('put_input')
#         self.inputs[port].append(value)
#         fire = self.eval_inputs()
#         if fire:
#             # notice supervisor here
#             self.fire()
#
#     def setup_input_ports(self, ports):
#         # setup dynamic input ports
#         self.inputs = {port: [] for port in ports}
#
#     def setup_output_ports(self, ports):
#         # setup dynamic input ports
#         self.connections = {port: [] for port in ports}
#
#     def input_ports(self):
#         # TODO make it a property with setter
#         return self.inputs.keys()
#
#     def output_ports(self):
#         # TODO similar to input_ports
#         return self.connections.keys()
#
#     def clear_inputs(self):
#         self.logger.debug('clear inputs')
#         for port in self.inputs:
#             self.inputs[port] = []
#
#     def connect_to(self, source_port, dest_actor, dest_port):
#         if source_port not in self.output_ports():
#             raise Exception('output port %s is not defined' % source_port)
#         if dest_port not in dest_actor.input_ports():
#             raise Exception('input port %s not defined in %s' % (dest_port, dest_actor.id))
#         self.connections[source_port].append({'actor': dest_actor, 'port': dest_port})
