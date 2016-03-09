from __future__ import absolute_import, division, print_function, unicode_literals
import tempfile
import os


def build_graph(actor):
    """Create graph with all actors + ports as nodes.
    
    It walks over all connections.
    
    Prerequisities:
    * graphviz package
    * dot executable (graphviz)
    
    Empty input ports are yellow.
    Empty output ports are red.
    """
    import graphviz
    actors = []
    edges = []
    ports = []
    graph = graphviz.Digraph()

    def _get_name(obj):
        return str(hash(obj))

    def _add_node(actor):
        attrs = {"fontsize": "12"}
        graph.node(_get_name(actor), label=actor.name, shape="box", **attrs)
        actors.append(actor)

    def _walk_node(actor):
        name = _get_name(actor)
        if not actor in actors:
            _add_node(actor)
        for port in actor.outports:
            if port not in ports:
                attrs = {}
                if not port.connections:
                    attrs["style"] = "filled"
                    attrs["color"] = "#ff0000"
                graph.node(_get_name(port), label=port.name, **attrs)
                graph.edge(name, _get_name(port))
                ports.append(port)
            for other in port.connections:
                if (port, other) not in edges:
                    edges.append((port, other))
                    _walk_node(other.owner)
                    graph.edge(_get_name(port), _get_name(other))
        for port in actor.inports:
            if port not in ports:
                attrs = {}
                if not port.connections:
                    attrs["style"] = "filled"
                    attrs["color"] = "#ffff00"
                graph.node(_get_name(port), label=port.name, **attrs)
                graph.edge(_get_name(port), name, )
                ports.append(port)
            for other in port.connections:
                if (other, port) not in edges:
                    _walk_node(other.owner)

    _walk_node(actor)
    return graph


def ipy_show(actor):
    """Display graph in IPython.

    """
    graph = build_graph(actor)
    graph.format = "png"

    from IPython.display import Image
    d = tempfile.mkdtemp()
    basename = os.path.join(d, "image")
    graph.render(basename)

    return Image(os.path.join(d, 'image.png'))

    # TODO: clean-up tempdir
