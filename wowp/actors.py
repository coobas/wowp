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


class LoopWhile(Actor):
    """While loop actor
    """

    def __init__(self, name=None, condition_func=None):
        super(LoopWhile, self).__init__(name=name)
        # self.inports.append('initial')
        self.inports.append('loop_in')
        self.outports.append('loop_out')
        self.outports.append('final')
        if condition_func is None:
            # TODO create in and out ports for condition
            raise NotImplementedError('To be implemented')
            self.condition_func = None
        else:
            self.condition_func = condition_func

    def on_input(self):
        # an input arrived --> fire
        # the condition is evaluated in fire
        self.fire()

    def fire(self):
        input_val = self.inports['loop_in'].pop()
        if self.condition_func:
            if self.condition_func(input_val)
                self.outports['loop_out'] = input_val
            else:
                self.outports['final'] = input_val
        else:
            # TODO use condition via ports
            raise NotImplementedError('To be implemented')
        