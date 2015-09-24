from ..components import Actor
import inspect


class FuncActor(Actor):

    """Actor defined simply by a function
    """
    # TODO create a derived class instead of an instance

    def __init__(self, func, outports=None, inports=None, name=None):
        if not name:
            name = func.__name__
        super(FuncActor, self).__init__(name=name)
        # get function signature
        try:
            sig = inspect.signature(func)
            return_annotation = sig.return_annotation
            # derive ports from func signature
            if inports is None:
                inports = (par.name for par in sig.parameters.values())
            if outports is None and return_annotation is not inspect.Signature.empty:
                # if func has a return annotation, use it for outports names
                outports = return_annotation
        except ValueError:
            # e.g. numpy has no support for inspect.signature
            # --> using manual inports
            if inports is None:
                inports = ('inp', )
            elif isinstance(inports, str):
                inports = (inports, )
        # save func as attribute
        self.func = func
        # setup inports
        for name in inports:
            self.inports.append(name)
        # setup outports
        if outports is None:
            outports = ('out', )
        elif isinstance(outports, str):
            outports = (outports, )
        for name in outports:
            self.outports.append(name)

    def get_run_args(self):
        args = tuple(port.pop() for port in self.inports)
        kwargs = {'func': self.func,
                  'outports': tuple(port.name for port in self.outports)}
        # kwargs['connected_ports'] = list((name for name, port in self.outports.items()
        #                                   if port.isconnected()))

        return args, kwargs

    @classmethod
    def run(cls, *args, **kwargs):
        func_res = kwargs['func'](*args)
        outports = kwargs['outports']

        if len(outports) == 1:
            func_res = (func_res, )
        # iterate over ports and return values
        res = {name: value for name, value in zip(outports, func_res)}
        return res

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


class GeneralizedFunctionActor(Actor):
    """
    Generalized function actor that supports argument binding.
    """

    def __init__(self, func, inport_args, bound_args = {}, outports = ["_"], name=None):
        """
        :param func: Any callable object
        :param inport_args: mapping from inport names to func arguments (for identical mapping, use list/tuple)
        :param bound_args: fixed arguments for func (argument name -> value)
        :param outports: a list of outport names
        :param name: name of the functor

        The argument identifiers can be strings (for kwargs) and/or ints (for args).
        """
        if not name:
            name = func.__name__    # If it has it
        super(GeneralizedFunctionActor, self).__init__(name=name)
        if not callable(func):
            raise Exception("Not callable")

        self.function = func

        if isinstance(inport_args, list) or isinstance(inport_args, tuple):
            inport_args = { k : k for k in inport_args }
        self.inport_arguments = inport_args
        for name in inport_args.keys():
            self.inports.append(name)

        self.bound_arguments = bound_args
        for name in outports:
            self.outports.append(name)

    def get_run_args(self):
        inport_args = { port.name : port.pop() for port in self.inports }
        args = self, inport_args
        kwargs = {}
        return args, kwargs

    def _run(self, inport_args):
        call_args = { self.inport_arguments[key] : value for key, value in inport_args.items() }   # Map the inports to arguments
        call_args.update(self.bound_arguments)

        kwargs = { k : v for k, v in call_args.items() if isinstance(k, str) }
        args_length = len(call_args) - len(kwargs)
        args = [ call_args[i] for i in range(args_length) ]

        return self.function(*args, **kwargs)

    @classmethod
    def run(cls, *args, **kwargs):
        obj, inport_args = args
        func_res = obj._run(inport_args)

        if len(obj.outports) == 1:
            func_res = (func_res,)
        res = {name: value for name, value in zip((o.name for o in obj.outports), func_res)}
        return res


class Switch(Actor):

    """While loop actor
    """

    def __init__(self, name=None, condition_func=None, inner_actor=None):
        super(Switch, self).__init__(name=name)
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

    def get_run_args(self):
        kwargs = {}
        kwargs['input_val'] = self.inports['loop_in'].pop()
        kwargs['condition_func'] = self.condition_func
        args = ()
        return args, kwargs

    @classmethod
    def run(cls, *args, condition_func=None, input_val=None):
        res = {}
        if condition_func:
            if condition_func(input_val):
                res['loop_out'] = input_val
            else:
                res['final'] = input_val
        else:
            # TODO use condition via ports
            raise NotImplementedError('To be implemented')
        return res


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
        self.inports.append('inp')
        self.outports.append('stdout')
        self.outports.append('stderr')
        self.outports.append('ret')

    def get_run_args(self):
        vals = self.inports['inp'].pop()
        if isinstance(vals, str):
            vals = (vals,)
        args = self.base_command + vals
        kwargs = {
            'shell': self.shell,
            'binary': self.binary,
        }
        return args, kwargs

    @classmethod
    def run(cls, *args, **kwargs):
        import subprocess
        import tempfile

        print(args)

        if kwargs['binary']:
            mode = "w+b"
        else:
            mode = "w+t"

        with tempfile.TemporaryFile(mode=mode) as fout, tempfile.TemporaryFile(mode=mode) as ferr:
            if kwargs['shell']:
                result = subprocess.call(' '.join(args), stdout=fout, stderr=ferr, shell=kwargs['shell'])
            else:
                result = subprocess.call(args, stdout=fout, stderr=ferr, shell=kwargs['shell'])
            fout.seek(0)
            ferr.seek(0)
            cout = fout.read()
            cerr = ferr.read()
        res = {
            'ret': result,
            'stdout': cout,
            'stderr': cerr
        }
        return res


class Sink(Actor):

    """Dumps everything
    """

    def can_run(self):
        return True

    def get_run_args(self):
        for port in self.inports:
            port.pop()
        return (), {}

    @staticmethod
    def run(*args, **kwargs):
        pass

