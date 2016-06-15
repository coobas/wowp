# -*- coding: utf-8 -*-
'''
Matlab actors

Requires MATLAB Engine for Python
(http://www.mathworks.com/help/matlab/matlab-engine-for-python.htm).
'''

from . import Actor
import stopit
import matlab.engine
from warnings import warn
import six


class EngineManager(object):
    """Matlab engine with a shared pool"""

    _engine_pool = []

    def __init__(self, engine_args=None, engine_kwargs=None):
        if engine_args is None:
            engine_args = ()
        if engine_kwargs is None:
            engine_kwargs = {}
        self.engine_args = engine_args
        self.engine_kwargs = engine_kwargs

    def _new_engine(self):
        return matlab.engine.start_matlab(*self.engine_args, **self.engine_kwargs)

    def pop(self):
        try:
            eng = self.__class__._engine_pool.pop()
        except IndexError:
            eng = self._new_engine()
        return eng

    def push(self, eng):
        self.__class__._engine_pool.append(eng)

    @classmethod
    def close_all(cls, timeout=10):
        while cls._engine_pool:
            eng = cls._engine_pool.pop()
            with stopit.ThreadingTimeout(timeout) as to_ctx_mgr:
                assert to_ctx_mgr.state == to_ctx_mgr.EXECUTING
                try:
                    eng.quit()
                except Exception:
                    # already closed or not working
                    pass
            if not to_ctx_mgr:
                warn('engine {} not responding for {} seconds'.format(str(eng), timeout))

    def __enter__(self):
        self.engine = self.pop()
        return self.engine

    def __exit__(self, exc_type, exc_value, traceback):
        self.push(self.engine)
        self.engine = None
        return exc_type is None


class MatlabMethod(Actor):
    def __init__(self, method_name, inports=(), outports='result'):
        self.method_name = method_name

        super(MatlabMethod, self).__init__(name=self.method_name)

        if isinstance(inports, six.string_types):
            inports = (inports,)
        for p in inports:
            self.inports.append(p)

        if isinstance(outports, six.string_types):
            outports = (outports,)
        for p in outports:
            self.outports.append(p)

        self._engine = None

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
        with EngineManager() as engine:
            mfunc = getattr(engine, self.method_name)
            res = mfunc(*args, **kwargs)
        return res
