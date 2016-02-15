from . import Actor
from julia import Julia, JuliaError
import six

class JuliaMethod(Actor):
    def __init__(self, method_name, package_name=None, inports=(), outports='result'):
        self.method_name = method_name
        self.package_name = package_name
        super(JuliaMethod, self).__init__(name=self._full_method_name)

        if isinstance(inports, six.string_types):
            inports = (inports,)
        for p in inports:
            self.inports.append(p)

        if isinstance(outports, six.string_types):
            outports = (outports,)
        for p in outports:
            self.outports.append(p)

    @property
    def _julia_method(self):
        if self.package_name:
            self._julia.eval("using %s" % self.package_name)
        return self._julia.eval(self._full_method_name)

    @property
    def _full_method_name(self):
        if self.package_name:
            return "%s.%s" % (self.package_name, self.method_name)
        else:
            return self.method_name

    def run(self):
        args = (port.pop() for port in self.inports)
        func_res = self.__call__(*args)

        if len(self.outports) == 1:
            func_res = (func_res,)

        res = {}
        for name, value in zip(self.outports.keys(), func_res):
            res[name] = value
        return res

    def __call__(self, *args, **kwargs):
        self._julia = Julia()
        return self._julia_method(*args)
