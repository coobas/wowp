from __future__ import absolute_import, division, print_function, unicode_literals

import random

from wowp.actors import FuncActor, Switch, ShellRunner, DictionaryMerge, LoopWhile
from wowp.schedulers import NaiveScheduler
from wowp.components import Actor
import nose
from nose.plugins.skip import SkipTest
import six


def test_FuncActor_return_annotation():
    if six.PY2:
        raise SkipTest
    def func(x, y):
        return x + 1, y + 2

    x, y = 2, 3.1

    fa = FuncActor(func, outports=('a', 'b'))
    fa.inports.x.put(x)
    fa.inports.y.put(y)

    NaiveScheduler().run_actor(fa)

    a, b = func(x, y)

    assert (fa.outports.a.pop() == a)
    assert (fa.outports.b.pop() == b)


def test_FuncActor_call():
    def func(x, y):
        return x + 1, y + 2

    x, y = 2, 3.1

    fa = FuncActor(func, outports=('a', 'b'))

    assert func(x, y) == fa(x, y)


def test_FuncActor_partial_args():
    def func(x, y):
        return x + 1, y + 2

    x, y = 2, 3.1

    fa = FuncActor(func, args=(x, ), outports=('a', 'b'))
    fa.inports.y.put(y)

    NaiveScheduler().run_actor(fa)

    a, b = func(x, y)

    assert (fa.outports.a.pop() == a)
    assert (fa.outports.b.pop() == b)


def test_FuncActor_partial_kwargs():
    def func(x, y):
        return x + 1, y + 2

    x, y = 2, 3.1

    fa = FuncActor(func, kwargs={'y': y}, outports=('a', 'b'))
    fa.inports.x.put(x)

    NaiveScheduler().run_actor(fa)

    a, b = func(x, y)

    assert (fa.outports.a.pop() == a)
    assert (fa.outports.b.pop() == b)


def test_custom_actor_call():
    class StrActor(Actor):
        def __init__(self, *args, **kwargs):
            super(StrActor, self).__init__(*args, **kwargs)
            # specify input port
            self.inports.append('input')
            # and output ports
            self.outports.append('output')

        def get_run_args(self):
            # get input value(s) using .pop()
            args = (self.inports['input'].pop(),)
            kwargs = {}
            return args, kwargs

        @staticmethod
        def run(value):
            # return a dictionary with port names as keys
            res = {'output': str(value)}
            return res

    actor = StrActor(name='str_actor')
    value = 123
    print(actor(input=value))
    # and check that the output is as expected
    assert actor(input=value)['output'] == str(value)


def test_LoopWhileActor():
    def condition(x):
        return x < 10

    def func(x):
        return x + 1

    fa = FuncActor(func, outports=('x', ))
    lw = LoopWhile("a_loop", condition_func=condition)
    fa.inports['x'] += lw.outports['loop']
    lw.inports['loop'] += fa.outports['x']
    lw.inports['init'].put(0)
    NaiveScheduler().run_actor(lw)
    result = lw.outports['exit'].pop()
    assert (result == 10)

    # test with condition actor
    fa = FuncActor(func, outports=('x', ))
    ca = FuncActor(condition, outports=('out', ))
    lw = LoopWhile("a_loop")
    lw.inports['condition_out'] += ca.outports['out']
    ca.inports['x'] += lw.outports['condition_in']
    fa.inports['x'] += lw.outports['loop']
    lw.inports['loop'] += fa.outports['x']
    lw.inports['init'].put(0)
    NaiveScheduler().run_actor(lw)
    result = lw.outports['exit'].pop()
    assert (result == 10)


def test_SwitchActor():

    for val in (True, False):
        token = random.randint(0, 100)
        sw = Switch("switch", lambda x: val)
        sw.inports['inp'].put(token)
        pname = 'true' if val else 'false'
        NaiveScheduler().run_actor(sw)
        assert sw.outports[pname].pop() == token
        assert not sw._in_condition

    for val in (True, False):
        token = random.randint(0, 100)
        sw = Switch("switch")
        ca = FuncActor(lambda x: val)
        sw.inports['condition_out'] += ca.outports['out']
        ca.inports['x'] += sw.outports['condition_in']
        sw.inports['inp'].put(token)
        pname = 'true' if val else 'false'
        NaiveScheduler().run_actor(sw)
        assert sw.outports[pname].pop() == token
        assert not sw._in_condition


def test_Shellrunner():
    import platform

    command = "echo"
    arg = "test"

    if platform.system() == 'Windows':
        runner = ShellRunner(command, shell=True)
    else:
        runner = ShellRunner(command, shell=False)
    runner.inports['inp'].put(arg)

    NaiveScheduler().run_actor(runner)

    rvalue = runner.outports['ret'].pop()
    stdout = runner.outports['stdout'].pop()
    stderr = runner.outports['stderr'].pop()

    print("Return value: ", rvalue)
    print("Std out: ", stdout.strip())
    print("Std err: ", stderr.strip())

    assert (rvalue == 0)
    assert (stdout.strip() == arg)
    assert (stderr.strip() == "")


def test_DictionaryMerge():
    d = DictionaryMerge(inport_names=("a", "b"))
    d.inports["a"].put("aa")
    d.inports["b"].put("bb")

    NaiveScheduler().run_actor(d)

    out = d.outports["out"].pop()

    assert (len(out) == 2)
    assert (out["a"] == "aa")
    assert (out["b"] == "bb")


if __name__ == '__main__':
    nose.run(argv=[__file__, '-vv'])
