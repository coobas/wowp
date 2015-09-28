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
        Actor.__init__(self, name=name)
        self.inports.append(inport_name)
        self.outports.append(outport_name)
        self.inport_name = inport_name
        self.outport_name = outport_name

    def iterate(self):
        collection = self.inports[self.inport_name].pop()
        for item in collection:
            yield self.outport_name, item


class Splitter(Actor):
    def __init__(self, name="splitter", inport_name="in", multiplicity=2):
        import itertools
        Actor.__init__(self, name=name)
        self.inport_name = inport_name
        self.multiplicity = multiplicity

        self.inports.append(inport_name)
        for i in range(1, multiplicity + 1):
            self.outports.append("%s_%d" % (inport_name, i))

        self._outports_cycle = itertools.cycle(range(1, multiplicity + 1))

    def run(self):
        value = self.inports[self.inport_name].pop()
        i = next(self._outports_cycle)
        outport = "%s_%d" % (self.inport_name, i)
        return {outport: value}


        # TODO: Add SequentialMerger
        # TODO: Add RandomMerger
