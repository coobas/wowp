from wowp.actors import FuncActor
from wowp.schedulers import NaiveScheduler
import nose


def test_two_connected():
    """Two connected function actors.

    --func1(.)--func2(.)--
    """

    def func1(x):
        return x * 2

    def func2(a):
        return a + 3

    actor1 = FuncActor(func1, outports=('a', ))
    actor2 = FuncActor(func2, outports=('y', ))

    actor2.inports['a'] += actor1.outports['a']

    for in_value in range(10):
        actor1.inports['x'].put(in_value)
        NaiveScheduler().run_actor(actor1)
        assert func2(func1(in_value)) == actor2.outports['y'].pop()


def test_two_to_one_connected():
    """Two converging lines.

    --func1(.)--+
                |
                +-func2(.,.)--
                |
    --func1(.)--+

    """

    def func1(x):
        return x * 2

    def func2(x, y):
        return x + y

    in_actor1 = FuncActor(func1, outports=('a', ))
    in_actor2 = FuncActor(func1, outports=('a', ))

    out_actor = FuncActor(func2, outports=('a', ))

    out_actor.inports['x'] += in_actor1.outports['a']
    out_actor.inports['y'] += in_actor2.outports['a']

    in_value1 = 1
    in_value2 = 2

    in_actor2.inports['x'].put(in_value2)
    in_actor1.inports['x'].put(in_value1)

    NaiveScheduler().run_actor(in_actor1)
    NaiveScheduler().run_actor(in_actor2)

    assert func2(func1(in_value1), func1(in_value2)) == out_actor.outports['a'].pop()


def test_three_in_line():
    """Three linearly connected function actors.

    --func(.)--func(.)--func(.)--
    """

    def func(x):
        return x * 2

    actor1 = FuncActor(func, outports=('x', ))
    actor2 = FuncActor(func, outports=('x', ))
    actor3 = FuncActor(func, outports=('x', ))

    actor2.inports['x'] += actor1.outports['x']
    actor3.inports['x'] += actor2.outports['x']

    in_value = 4
    actor1.inports['x'].put(in_value)

    NaiveScheduler().run_actor(actor1)

    assert (func(func(func(in_value)))) == actor3.outports['x'].pop()


if __name__ == '__main__':
    nose.run(argv=[__file__, '-vv'])
