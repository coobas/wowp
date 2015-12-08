from __future__ import absolute_import, division, print_function, unicode_literals
from wowp.actors import FuncActor, Switch
from wowp.schedulers import LinearizedScheduler, ThreadedScheduler
import nose


def test_LinearizedScheduler_loop1000():

    def condition(x):
        return x < 1000

    def func(x):
        return x + 1

    scheduler = LinearizedScheduler()

    fa = FuncActor(func, outports=('x', ))
    lw = Switch("a_loop", condition)

    fa.inports['x'] += lw.outports['loop_out']
    lw.inports['loop_in'] += fa.outports['x']

    scheduler.put_value(lw.inports['loop_in'], 0)
    scheduler.execute()

    result = lw.outports['final'].pop()
    assert (result == 1000)


def _run_tree_512_test(scheduler):
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


def test_all_schedulers():
    for scheduler in (ThreadedScheduler(max_threads=8),
                      LinearizedScheduler()):
        for case in (_run_tree_512_test,
                     _run_linearity_test):
            yield case, scheduler


def test_ThrededScheduler_creates_threads_and_executes_all():
    """Test whether more threads are being used by the scheduler"""
    thread_ids = set()
    jobs_executed = {'count': 0}

    def func(x):
        # nonlocal jobs_executed
        import threading
        thread_ids.add(threading.current_thread().ident)
        jobs_executed['count'] += 1
        return x

    branch_count = 40  # times 2
    branch_length = 10
    thread_count = 8

    scheduler = ThreadedScheduler(max_threads=thread_count)

    for i in range(branch_count):
        actors = []

        for j in range(branch_length):
            actor = FuncActor(func, outports=('x', ))
            if j == 0:
                scheduler.put_value(actor.inports['x'], 0)
            else:
                actor.inports["x"].connect(actors[j - 1].outports["x"])
            actors.append(actor)
    scheduler.execute()

    # How many threads were used
    assert thread_count >= len(thread_ids)
    assert 1 < len(thread_ids)
    assert jobs_executed['count'] == branch_count * branch_length


def _run_linearity_test(scheduler):
    import random
    from time import sleep

    max_ = 100

    original_sequence = list(range(max_))

    def orig(x):
        # print("In act1:", x)
        return x

    def app_fn(x):
        # print("In act2:", x)
        s = random.randint(1, 30) / 1000.0
        sleep(s)
        return x

    orig_actor = FuncActor(orig, outports=('x', ))
    orig_actor.scheduler = scheduler
    new_actor = FuncActor(app_fn, outports=('x', ))
    orig_actor.outports["x"].connect(new_actor.inports["x"])

    for i in range(max_):
        scheduler.put_value(orig_actor.inports["x"], i)

    scheduler.execute()
    new_sequence = list(new_actor.outports['x'].pop_all())

    print("Received: ", new_sequence)
    print("Expected: ", original_sequence)

    assert new_sequence == original_sequence


if __name__ == '__main__':
    nose.run(argv=[__file__, '-vv'])
