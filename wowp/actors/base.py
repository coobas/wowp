from __future__ import absolute_import, division, print_function
from ..components import Actor
import inspect
import itertools
import six

__all__ = ['FuncActor', 'Switch', 'ShellRunner', 'Sink', 'DictionaryMerge', 'LoopWhile']


class FuncActor(Actor):
    """Actor defined simply by a function

    Args:
        func (callable): The function to be called on actor execution
        args (list): Fixed function positional arguments
        kwargs(dict): Fixed function keyword arguments
            args and kwargs behave as functools.partial args and keywords
        outports: output port name(s)
        inports: input port name(s)
        name (str): actor name
    """

    def __init__(self, func, args=(), kwargs={}, outports=None, inports=None, name=None):
        if not name:
            name = func.__name__
        super(FuncActor, self).__init__(name=name)
        try:
            # try to derive ports from function signature
            if six.PY2:
                # Python 2 does not have signatures
                fargs = inspect.getargspec(func)
                if inports is None:
                    inports = (par
                               for par in itertools.islice(fargs.args, len(args), None)
                               if par not in kwargs)
            else:
                sig = inspect.signature(func)
                return_annotation = sig.return_annotation
                # derive ports from func signature
                if inports is None:
                    # filter out args (first len(args) arguments and kwargs)
                    inports = (
                        par.name
                        for par in itertools.islice(sig.parameters.values(), len(args), None)
                        if par.name not in kwargs)
                if outports is None and return_annotation is not inspect.Signature.empty:
                    # if func has a return annotation, use it for outports names
                    outports = return_annotation
        except (ValueError, TypeError):
            # e.g. numpy has no support for inspect.signature
            # --> using manual inports
            if inports is None:
                inports = ('inp', )
            elif isinstance(inports, six.string_types):
                inports = (inports, )
        # save func as attribute
        self.func = func
        self._func_args = args
        self._func_kwargs = kwargs
        # setup inports
        for name in inports:
            self.inports.append(name)
        # setup outports
        if outports is None:
            outports = ('out', )
        elif isinstance(outports, six.string_types):
            outports = (outports, )
        for name in outports:
            self.outports.append(name)

    def get_run_args(self):
        args = tuple(port.pop() for port in self.inports)
        kwargs = {'runfunc': self.func,
                  'func_args': self._func_args,
                  'func_kwargs': self._func_kwargs,
                  'outports': tuple(port.name for port in self.outports)}
        # kwargs['connected_ports'] = list((name for name, port in self.outports.items()
        #                                   if port.isconnected()))

        return args, kwargs

    @staticmethod
    def run(*args, **kwargs):
        args = kwargs['func_args'] + args
        func_res = kwargs['runfunc'](*args, **kwargs['func_kwargs'])
        outports = kwargs['outports']

        if len(outports) == 1:
            func_res = (func_res, )
        # iterate over ports and return values
        res = {name: value for name, value in zip(outports, func_res)}
        return res

    def __call__(self, *args, **kwargs):
        args = self._func_args + args
        kwargs.update(self._func_kwargs)
        return self.func(*args, **kwargs)


class Switch(Actor):
    """Redirects to either 'true' or 'false' output based on the condition value
    """

    _system_actor = True

    def __init__(self, name=None, condition_func=None):
        super(Switch, self).__init__(name=name)
        self._in_condition = False
        self.inports.append('inp')
        self.outports.append('true')
        self.outports.append('false')
        self.outports.append('condition_in')
        self.inports.append('condition_out')
        if condition_func is None:
            self._condition_func = None
        elif callable(condition_func):
            self._condition_func = condition_func
        else:
            raise Exception('condition_func must be a callable object')

    def get_run_args(self):
        # everything is done inside run
        return (), {}

    def is_condition_actor(self):
        """Returns True if condition actor is connected
        """
        if (self.inports['condition_out'].isconnected() and
                self.outports['condition_in'].isconnected()):
            return True
        elif (self.inports['condition_out'].isconnected() or
              self.outports['condition_in'].isconnected()):
            raise Exception('Both condition_in and out must be connected')
        return False

    def run(self, *args, **kwargs):
        res = {}
        condition_out = None
        if not self._in_condition:
            # input on init port
            value = self.inports['inp'].pop()

        elif not self.inports['condition_out'].isempty():
            # we receive the condition actor output
            # the value was stored
            value = self._last_value
            condition_out = self.inports['condition_out'].pop()
            self._in_condition = False
        else:
            raise Exception('Enexpected error')

        if condition_out is None:
            # we have to evaluate the condition
            if self.is_condition_actor():
                self._last_value = value
                self._in_condition = True
                res['condition_in'] = value
                # we have to return here to execute the condition actor
                return res
            else:
                condition_out = self._condition_func(value)

        if condition_out:
            res['true'] = value
        else:
            res['false'] = value
        return res

    def can_run(self):
        if self._in_condition:
            # waiting for the condition actor
            res = not self.inports['condition_out'].isempty()
        else:
            res = not self.inports['inp'].isempty()
        return res


class ShellRunner(Actor):
    """An actor executing external command.

    Basically, it calls subprocess.call(base_command + inp) or
    subprocess.call(base_command.format(inp)) in case format_inp is True.

    Args:
        base_command: the command to be run, may be a template
        binary: input/output in binary mode
        shell: shell parameter in subprocess.call
               if it's a string then it indicates the executable parameter in subprocess.call
        format_inp: 'args' triggers base_command.format(*inp),
                    'kwargs' triggers base_command.format(**inp)
        single_out: join outputs into a single dict
        debug_print: print debug info
        print_output: print standard output and standard error
    """

    def __init__(self,
                 base_command,
                 name=None,
                 binary=False,
                 shell=False,
                 format_inp=False,
                 single_out=False,
                 print_output=False,
                 debug_print=False):
        super(ShellRunner, self).__init__(name=name)

        if isinstance(base_command, six.string_types):
            self.base_command = (base_command, )
        else:
            self.base_command = base_command

        self.binary = binary
        self.shell = shell
        self.format_inp = format_inp
        self.inports.append('inp')
        self.single_out = single_out
        self.debug_print = debug_print
        self.print_output = print_output
        if single_out:
            self.outports.append('out')
        else:
            self.outports.append('stdout')
            self.outports.append('stderr')
            self.outports.append('ret')

    def get_run_args(self):
        vals = self.inports['inp'].pop()
        if self.format_inp == 'args':
            args = (self.base_command[0].format(*vals), )
        elif self.format_inp == 'kwargs':
            args = (self.base_command[0].format(**vals), )
        elif self.format_inp == 'trigger':
            args = self.base_command
        elif isinstance(vals, six.string_types):
            args = self.base_command + (vals, )
        else:
            args = self.base_command + vals
        kwargs = {
            'shell': self.shell,
            'binary': self.binary,
            'single_out': self.single_out,
            'debug_print': self.debug_print,
            'print_output': self.print_output,
        }
        return args, kwargs

    @staticmethod
    def run(*args, **kwargs):
        import subprocess
        import tempfile
        import sys

        if kwargs['debug_print']:
            print('run command:\n{}'.format(' '.join(args)))

        if kwargs['binary']:
            mode = "w+b"
        else:
            mode = "w+t"

        with tempfile.TemporaryFile(mode=mode) as fout, tempfile.TemporaryFile(
                mode=mode) as ferr:
            if kwargs['shell']:

                executable = kwargs['shell']
                if not isinstance(kwargs['shell'], six.string_types):
                    executable = None

                result = subprocess.call(' '.join(args),
                                         stdout=fout,
                                         stderr=ferr,
                                         executable=executable,
                                         shell=kwargs['shell'])
            else:
                result = subprocess.call(args,
                                         stdout=fout,
                                         stderr=ferr,
                                         shell=kwargs['shell'])
            fout.seek(0)
            ferr.seek(0)
            cout = fout.read()
            cerr = ferr.read()
        res = {'ret': result, 'stdout': cout, 'stderr': cerr}
        if kwargs['debug_print']:
            print('result:\n{}'.format(res))
        if kwargs['print_output']:
            sys.stdout.write(cout)
            sys.stderr.write(cerr)
        if kwargs['single_out']:
            res = {'out': res}
        return res


class Sink(Actor):
    """Dumps everything
    """

    _system_actor = True

    def can_run(self):
        return True

    def get_run_args(self):
        for port in self.inports:
            port.pop()
        return (), {}

    @staticmethod
    def run(*args, **kwargs):
        pass


class DictionaryMerge(Actor):
    """This actor merges inputs from all its inports into one dictionary.

    The keys of the dictionary will be equal to inport names.
    """

    def __init__(self, name="packager", inport_names=("in"), outport_name="out"):
        super(DictionaryMerge, self).__init__(name=name)
        for in_name in inport_names:
            self.inports.append(in_name)
        self.outport_name = outport_name
        self.outports.append(outport_name)

    def get_run_args(self):
        return (), {
            "values": {port.name: port.pop()
                       for port in self.inports},
            "outport_name": self.outport_name
        }

    @staticmethod
    def run(*args, **kwargs):
        return {kwargs.get("outport_name"): kwargs.get("values")}


class LoopWhile(Actor):
    """A while loop actor"""

    _system_actor = True

    def __init__(self, name='LoopWhile', condition_func=None):
        super(LoopWhile, self).__init__(name=name)
        # flag for being inside a loop
        self._in_loop = False
        # flag for evaluating the condition
        self._in_condition = False
        # setup ports
        self.inports.append('init')
        self.inports.append('loop')
        self.outports.append('loop')
        self.outports.append('exit')
        # TODO: we could possibly create condition ports only if condition_func is None
        # --> would have to fix the logic in run
        self.outports.append('condition_in')
        self.inports.append('condition_out')
        if condition_func is None:
            self._condition_func = None
        elif callable(condition_func):
            self._condition_func = condition_func
        else:
            raise Exception('condition_func must be a callable object')

    def get_run_args(self):
        # everything is done inside run
        return (), {}

    def is_condition_actor(self):
        """Returns True if condition actor is connected
        """
        if (self.inports['condition_out'].isconnected() and
                self.outports['condition_in'].isconnected()):
            return True
        elif (self.inports['condition_out'].isconnected() or
              self.outports['condition_in'].isconnected()):
            raise Exception('Both condition_in and out must be connected')
        return False

    def run(self, *args, **kwargs):
        res = {}
        condition_out = None
        if not self._in_loop:
            # input on init port
            value = self.inports['init'].pop()
            self._in_loop = True
        elif not self.inports['loop'].isempty():
            # input value from the loop
            value = self.inports['loop'].pop()
        elif not self.inports['condition_out'].isempty():
            # we receive the condition actor output
            # the value was stored
            value = self._last_value
            condition_out = self.inports['condition_out'].pop()
        else:
            raise Exception('Enexpected error')

        if condition_out is None:
            # we have to evaluate the condition
            if self.is_condition_actor():
                self._last_value = value
                res['condition_in'] = value
                # we have to return here to execute the condition actor
                return res
            else:
                condition_out = self._condition_func(value)
        if condition_out:
            res['loop'] = value
        else:
            # this is the end - condition is False
            self._in_loop = False
            res['exit'] = value
        return res

    def can_run(self):
        if self._in_loop:
            # waiting for loop
            res = (not self.inports['loop'].isempty() or
                   not self.inports['condition_out'].isempty())
        else:
            res = not self.inports['init'].isempty()
        return res
