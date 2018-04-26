# run this using
# mpiexec [MPI EXEC PARAMETERS] python -m mpi4py.futures wowp_mpi_run_wf.py
# running on localhost with 4 processes
# mpiexec -n 4 python -m mpi4py.futures wowp_mpi_run_wf.py

import json
import datetime

from wowp.actors import AnnotateInp, FuncActor, LoopWhile, DictionaryMerge
from wowp.schedulers import FuturesScheduler


def test_LinearizedScheduler_loop1000(scheduler):

    def condition(x):
        return x < 1000

    def func(x):
        return x + 1

    fa = FuncActor(func, outports=('x', ))
    lw = LoopWhile("a_loop", condition)

    fa.inports['x'] += lw.outports['loop']
    lw.inports['loop'] += fa.outports['x']

    scheduler.put_value(lw.inports['init'], 0)
    scheduler.execute()

    result = lw.outports['exit'].pop()
    assert (result == 1000)


def run_tree_512_test(scheduler):
    def sum(a, b):
        return a + b

    def ident(a):
        return a

    def _split_and_sum(act, depth):
        if depth == 0:
            return act
        else:
            child1 = FuncActor(ident, outports=('a', ))
            child2 = FuncActor(ident, outports=('a', ))
            act.outports['a'].connect(child1.inports['a'])
            act.outports['a'].connect(child2.inports['a'])
            children = [_split_and_sum(child, depth - 1) for child in (child1, child2)]
            summer = FuncActor(sum, outports=('a', ))
            summer.inports['a'].connect(children[0].outports['a'])
            summer.inports['b'].connect(children[1].outports['a'])
            return summer

    first = FuncActor(ident, outports=('a', ))

    power = 8
    last = _split_and_sum(first, power)

    scheduler.put_value(first.inports['a'], 1)
    scheduler.execute()

    assert (2 ** power == last.outports['a'].pop())


if __name__ == '__main__':

    mpischeduler = FuturesScheduler(executor='mpi')

    # simple workflow with annotations
    actor1 = AnnotateInp(max_sleep=0.5, name='actor1')
    actor2a = AnnotateInp(max_sleep=0.5, name='actor2a')
    actor2b = AnnotateInp(max_sleep=0.5, name='actor2b')
    merge = DictionaryMerge(inport_names=['a', 'b'])
    actor2a.inports['inp'] += actor1.outports['out']
    actor2b.inports['inp'] += actor1.outports['out']
    merge.inports['a'] += actor2a.outports['out']
    merge.inports['b'] += actor2b.outports['out']

    wf = actor1.get_workflow()
    wf_res = wf(inp='input', scheduler=mpischeduler)

    def default(obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        else:
            return obj

    print(json.dumps(wf_res['out'][0], indent=2, default=default))

    # run two tests from the test suite
    test_LinearizedScheduler_loop1000(mpischeduler)
    run_tree_512_test(mpischeduler)
