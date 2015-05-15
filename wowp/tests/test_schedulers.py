from wowp.actors import FuncActor, LoopWhile
from wowp.schedulers import LinearizedScheduler

import nose

def test_LinearizedScheduler_loop1000():
    scheduler = LinearizedScheduler()

    condition = lambda x: x < 1000
    def func(x) -> ('x'):
        return x + 1
    fa = FuncActor(func)
    lw = LoopWhile("a_loop", condition)

    fa.inports['x'] += lw.outports['loop_out']
    lw.inports['loop_in'] += fa.outports['x']

    scheduler.put_value(lw.inports['loop_in'], 0)
    scheduler.execute()

    result = lw.outports['final'].pop()

    assert(result == 1000)

if __name__ == '__main__':
    nose.run(argv=[__file__, '-vv'])