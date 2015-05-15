from wowp.actors import FuncActor, LoopWhile
from wowp.schedulers import LinearizedScheduler, RandomScheduler

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


def test_RandomScheduler_tree512():
    def sum(a, b) -> ('a'):
        return a + b
    def ident(a) -> ('a'):
        return a

    def _split_and_sum(act, depth):
        if depth == 0:
            return act
        else:
            child1 = FuncActor(ident)
            child2 = FuncActor(ident)
            act.outports['a'].connect(child1.inports['a'])
            act.outports['a'].connect(child2.inports['a'])
            children = [_split_and_sum(child, depth - 1) for child in (child1, child2)]
            summer = FuncActor(sum)
            summer.inports['a'].connect(children[0].outports['a'])
            summer.inports['b'].connect(children[1].outports['a'])
            return summer        

    scheduler = RandomScheduler()

    first = FuncActor(ident)
    
    power = 8
    last = _split_and_sum(first, power)

    scheduler.put_value(first.inports['a'], 1)
    scheduler.execute()

    assert(2 ** power == last.outports['a'].pop())

if __name__ == '__main__':
    nose.run(argv=[__file__, '-vv'])