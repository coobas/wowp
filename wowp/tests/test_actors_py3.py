from __future__ import absolute_import, division, print_function, unicode_literals

import random

from wowp.actors import FuncActor, Switch, ShellRunner, DictionaryMerge, LoopWhile
from wowp.schedulers import NaiveScheduler
from wowp.components import Actor
import nose
from nose.plugins.skip import SkipTest
import six


def test_FuncActor_return_annotation():

    def func(x, y) -> ('a', 'b'):
        return x + 1, y + 2

    x, y = 2, 3.1

    f_actors = (FuncActor(func, outports=('a', 'b')),
                FuncActor(func))

    for fa in f_actors:
        fa.inports.x.put(x)
        fa.inports.y.put(y)

        NaiveScheduler().run_actor(fa)

        a, b = func(x, y)

        assert (fa.outports.a.pop() == a)
        assert (fa.outports.b.pop() == b)


def test_ignore_PEP_484_annotations():

    def func(x, y) -> int:
        return int(x + y + 2)

    x, y = 2, 3

    fa = FuncActor(func)
    fa.inports.x.put(x)
    fa.inports.y.put(y)

    NaiveScheduler().run_actor(fa)

    res = func(x, y)

    assert (fa.outports.out.pop() == res)

    # test with a given outport name
    fa = FuncActor(func, outports=('o', ))
    fa.inports.x.put(x)
    fa.inports.y.put(y)

    NaiveScheduler().run_actor(fa)

    res = func(x, y)

    assert (fa.outports.o.pop() == res)


if __name__ == '__main__':
    nose.run(argv=[__file__, '-vv'])
