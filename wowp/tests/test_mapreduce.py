from wowp.actors import FuncActor
from wowp.actors.mapreduce import Map  # , Reduce
from wowp.schedulers import LinearizedScheduler
import six
if six.PY2:
    from itertools import izip_longest as zip_longest
else:
    from itertools import zip_longest
from string import ascii_uppercase


def test_map_run():
    func = lambda x: x * 2
    inp = range(5)
    map_act = Map(FuncActor, args=(func,), scheduler=LinearizedScheduler())
    map_act.inports.inp.put(inp)
    result = map_act.run()
    assert all(a == b for a, b in zip_longest(result, map(func, inp)))


# def test_map():
#     map_actor = Map(FuncActor, args=(ascii_uppercase, ))
#     test_str = 'abcdEFG'
#     res = map_actor(test_str)
#     assert res['out'] == test_str.upper()

if __name__ == "__main__":
    test_map_run()
