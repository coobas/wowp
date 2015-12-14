import nose
from wowp.actors import FuncActor
from wowp.actors.mapreduce import Map  # , Reduce
from wowp.schedulers import LinearizedScheduler
from itertools import zip_longest
import math
from string import ascii_uppercase


def test_map_run():
    func = lambda x: x * 2
    inp = range(5)
    map_act = Map(FuncActor, args=(func,), scheduler=LinearizedScheduler())
    map_act.inports.inp.put(inp)
    result = map_act.run()
    assert all(a == b for a, b in zip_longest(result['out'], map(func, inp)))


def test_map_linear():
    ra = FuncActor(range)
    func = math.sin
    map_act = Map(FuncActor, args=(func,))
    map_act.inports['inp'] += ra.outports['out']

    wf = ra.get_workflow()
    sch = LinearizedScheduler()

    inp = 5
    sch.run_workflow(wf, inp=inp)
    result = wf.outports.out.pop_all()
    assert len(result) == 1
    assert all((a, b) for a, b in zip_longest(result[0], map(func, range(inp))))


# def test_map():
#     map_actor = Map(FuncActor, args=(ascii_uppercase, ))
#     test_str = 'abcdEFG'
#     res = map_actor(test_str)
#     assert res['out'] == test_str.upper()

if __name__ == '__main__':
    nose.run(argv=[__file__, '-vv'])
