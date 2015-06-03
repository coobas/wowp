from wowp.actors import FuncActor, LoopWhile
from wowp.schedulers import LinearizedScheduler, RandomScheduler, ThreadedScheduler

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

def _run_tree_512_test(scheduler):
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

    first = FuncActor(ident)

    power = 8
    last = _split_and_sum(first, power)

    scheduler.put_value(first.inports['a'], 1)
    scheduler.execute()

    assert(2 ** power == last.outports['a'].pop())

def test_RandomScheduler_tree512():
    scheduler = RandomScheduler()
    _run_tree_512_test(scheduler)

def test_ThreadedScheduler_tree512():
    scheduler = ThreadedScheduler(max_threads=8)
    _run_tree_512_test(scheduler)

def test_LinearizedScheduler_tree512():
    scheduler = LinearizedScheduler()
    _run_tree_512_test(scheduler)

def test_ThrededScheduler_creates_threads_and_executes_all():
    """Test whether more threads are being used by the scheduler"""
    thread_ids = set()
    jobs_executed = 0

    def func(x) -> ('x'):
        nonlocal jobs_executed
        import threading
        thread_ids.add(threading.current_thread().ident)
        jobs_executed += 1
        return x

    branch_count = 40    # times 2
    branch_length = 10
    thread_count = 8

    scheduler = ThreadedScheduler(max_threads=thread_count)

    for i in range(branch_count):
        actors = []

        for j in range(branch_length):
            actor = FuncActor(func)
            if j == 0:
                scheduler.put_value(actor.inports['x'], 0)
            else:
                actor.inports["x"].connect(actors[j-1].outports["x"])
            actors.append(actor)
    scheduler.execute()

    # How many threads were used
    assert thread_count >= len(thread_ids)
    assert 1 < len(thread_ids)
    assert jobs_executed == branch_count * branch_length

if __name__ == '__main__':
    nose.run(argv=[__file__, '-vv'])