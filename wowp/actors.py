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
        # print('on_input')
        if all(port for port in self.inports):
            self.fire()

    def fire(self):
        # print('fire')
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

    def __init__(self, name=None, condition_func=None, inner_actor=None):
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
        if inner_actor:
            if len(inner_actor.inports) != 1:
                raise RuntimeError("Inner actor has to have exactly one input port.")
            if len(inner_actor.outports) != 1:
                raise RuntimeError("Inner actor has to have exactly one output port.")
            self.outports['loop_out'].connect(list(inner_actor.inports._ports.values())[0])
            self.inports['loop_in'].connect(list(inner_actor.outports._ports.values())[0])

    def on_input(self):
        # an input arrived --> fire
        # the condition is evaluated in fire
        self.fire()

    def fire(self):
        input_val = self.inports['loop_in'].pop()
        if self.condition_func:
            if self.condition_func(input_val):
                self.outports['loop_out'].put(input_val)
            else:
                self.outports['final'].put(input_val)
        else:
            # TODO use condition via ports
            raise NotImplementedError('To be implemented')


class ShellRunner(Actor):
    """An actor executing external command."""
    def __init__(self, base_command, name=None, binary=False, shell=False):
        super(ShellRunner, self).__init__(name=name)

        if isinstance(base_command, str):
            self.base_command = (base_command,)
        else:
            self.base_command = base_command

        self.binary = binary
        self.shell = shell
        self.inports.append('in')
        self.outports.append('stdout')
        self.outports.append('stderr')
        self.outports.append('return')

    def on_input(self):
        self.fire()

    def fire(self):
        import subprocess
        import tempfile

        vals = self.inports['in'].pop()
        if isinstance(vals, str):
            vals = (vals,)
        args = self.base_command + vals

        if self.binary:
            mode = "w+b"
        else:
            mode = "w+t"

        with tempfile.TemporaryFile(mode=mode) as fout, tempfile.TemporaryFile(mode=mode) as ferr:
            result = subprocess.call(args, stdout=fout, stderr=ferr, shell=self.shell)
            fout.seek(0)
            ferr.seek(0)

            cout = fout.read()
            cerr = ferr.read()
        self.outports['return'].put(result)
        self.outports['stdout'].put(cout)
        self.outports['stderr'].put(cerr)
