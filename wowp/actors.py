from .components import Actor
import inspect
from . import logger

class FuncActor(Actor):
    """Actor defined simply by a function
    """
    # TODO create a derived class instead of an instance
    def __init__(self, func, outports='out'):
        super(FuncActor, self).__init__(name=func.__name__)
        # get function signature
        sig = inspect.signature(func)
        self.func = func
        # derive inports from func signature
        for par in sig.parameters.values():
            self.inports.append(par.name)
        if sig.return_annotation is not inspect.Signature.empty:
            # if func has a return annotation, use it for outports names
            outports = sig.return_annotation
        # setup outports
        if isinstance(outports, str):
            outports = (outports, )
        for name in outports:
            self.outports.append(name)

    def on_input(self):
        print('on_input')
        if all(port for port in self.inports):
            self.fire()

    def fire(self):
        print('fire')
        args = (port.pop() for port in self.inports)
        func_res = self.func(*args)
        
        if len(self.outports) == 1:
            func_res = (func_res,)
        # iterate over ports and return values
        for out_port, value in zip(self.outports, func_res):
            out_port.put(value)
        
    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)
