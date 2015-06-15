from . import Actor


class GeneratorActor(Actor):

    class PseudoDict(object):
        def __init__(self, iterator):
            self.iterator = iterator

        def items(self):
            return self.iterator

    def run(self):
        return GeneratorActor.PseudoDict(self.iterate())

    def iterate(self):
        raise NotImplementedError("It is necessary to implement iterate method that yields pairs key, value")


class LineReader(GeneratorActor):
    """Sequentially put all lines in a file."""

    def __init__(self, name="line_reader", inport_name="path", outport_name="line"):
        Actor.__init__(self, name=name)
        self.inports.append(inport_name)
        self.outports.append(outport_name)
        self.inport_name = inport_name
        self.outport_name = outport_name

    def iterate(self):
        path = self.inports[self.inport_name].pop()
        with open(path, "rt") as f:
            for line in f:
                yield self.outport_name, line.strip()


class IteratorActor(GeneratorActor):

    def __init__(self, name="iterator", inport_name="collection", outport_name="item"):
        Actor.__init__(self, name)
        self.inports.append(inport_name)
        self.outports.append(outport_name)
        self.inport_name = inport_name
        self.outport_name = outport_name

    def iterate(self):
        collection = self.inports[self.inport_name].pop()
        for item in collection:
            yield self.outport_name, item