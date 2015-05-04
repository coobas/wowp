from .components import Actor

class PlainActor(Actor):
    """Actor defined simply by a function
    """
    def __init__(self, func, outports=('out', )):
        self.func = func
        # TODO derive inports from func signature
        # TODO setup outports

    def on_input(self):
        # TODO check input ports
        # + call fire if enough inputs

    def fire(self):
        args = (port.pop() for port in self.inports)
        func_res = self.func(args)
        for out_port, value in zip(self.outports, func_res):
            out_port.put(value)
