# -*- coding: utf-8 -*-
'''
Matlab actors

Requires MATLAB Engine for Python
(http://www.mathworks.com/help/matlab/matlab-engine-for-python.htm).
'''

from . import Actor

import matlab.engine


class MatlabMethod(Actor):

    def __init__(self, method_name, inports=(), outports='result'):
        self.method_name = method_name

        super().__init__(name=self.method_name)

        if isinstance(inports, str):
            inports = (inports,)
        for p in inports:
            self.inports.append(p)

        if isinstance(outports, str):
            outports = (outports,)
        for p in outports:
            self.outports.append(p)

        self._engine = None

    @property
    def engine(self):
        if self._engine is None:
            self._engine = matlab.engine.start_matlab()
        return self._engine

    @property
    def _matlab_method(self):
        return getattr(self.engine, self.method_name)

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
        return self._matlab_method(*args, **kwargs)
