from __future__ import absolute_import, division, print_function, unicode_literals

from wowp.util import ConstructorWrapper
from wowp.actors import FuncActor
from wowp.actors.mapreduce import Map  # , Reduce
from wowp.schedulers import LinearizedScheduler
from nose.tools import assert_sequence_equal
import six
if six.PY2:
    from itertools import izip_longest as zip_longest
else:
    from itertools import zip_longest
import math
from string import ascii_uppercase


def test_map_run():
    func = lambda x: x * 2
    inp = range(5)
    map_act = Map(FuncActor, args=(func, ), kwargs={'inports': ('inp', )}, scheduler=LinearizedScheduler())
    map_act.inports.inp.put(inp)
    result = map_act.run()
    assert all(a == b for a, b in zip_longest(result['out'], map(func, inp)))


def test_map_linear():
    ra = FuncActor(range)
    func = math.sin
    map_act = Map(FuncActor, args=(func, ))
    map_act.inports['inp'] += ra.outports['out']

    wf = ra.get_workflow()
    sch = LinearizedScheduler()

    inp = 5
    sch.run_workflow(wf, inp=inp)
    result = wf.outports.out.pop_all()
    assert len(result) == 1
    assert all((a, b) for a, b in zip_longest(result[0], map(func, range(inp))))


def test_map_shell():
    from wowp.actors import ShellRunner
    cmd = "echo {number}"

    sch = LinearizedScheduler()
    map_actor = Map(ShellRunner,
                    args=(cmd, ),
                    kwargs=dict(shell=True,
                                format_inp='kwargs',
                                single_out=True),
                    scheduler=sch)
    # TODO Map needs a single output
    # possible solution -> specify output (and input) port for Map
    n = 5
    inp = [{'number': i} for i in range(n)]
    res = map_actor(inp=inp)
    assert all((a == b
                for a, b in zip(
                    (int(d['stdout'].strip()) for d in res['out']), range(n))))

# def test_map():
#     map_actor = Map(FuncActor, args=(ascii_uppercase, ))
#     test_str = 'abcdEFG'
#     res = map_actor(test_str)
#     assert res['out'] == test_str.upper()


def swap(a, b):
    return b, a

def test_map_multiport():
    actor_class = ConstructorWrapper(FuncActor, swap, inports=('a', 'b'), outports=('a', 'b'))
    amap = Map(actor_class, scheduler=LinearizedScheduler())

    inputs = dict(a=[1, 2], b=[10, 20])
    res = amap(**inputs)
    print(res)
    assert_sequence_equal(res['a'], inputs['b'])


if __name__ == '__main__':
    import nose
    nose.run(argv=[__file__, '-vv'])
