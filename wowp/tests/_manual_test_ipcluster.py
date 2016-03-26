from __future__ import absolute_import, division, print_function, unicode_literals
from wowp.actors import FuncActor
from wowp.schedulers import IPyClusterScheduler, FuturesScheduler, LinearizedScheduler
from wowp.logger import logger


def _run_tree_512_test(scheduler):
    def sum(a, b):
        return a + b

    def ident(a):
        return a

    def _split_and_sum(act, depth):
        if depth == 0:
            return act
        else:
            child1 = FuncActor(ident, outports=('a',))
            child2 = FuncActor(ident, outports=('a',))
            act.outports['a'].connect(child1.inports['a'])
            act.outports['a'].connect(child2.inports['a'])
            children = [_split_and_sum(child, depth - 1) for child in (child1, child2)]
            summer = FuncActor(sum, outports=('a',))
            summer.inports['a'].connect(children[0].outports['a'])
            summer.inports['b'].connect(children[1].outports['a'])
            return summer

    first = FuncActor(ident, outports=('a',))

    power = 8
    last = _split_and_sum(first, power)

    scheduler.put_value(first.inports['a'], 1)
    scheduler.execute()

    assert (2 ** power == last.outports['a'].pop())


def orig(x):
    # print("In act1:", x)
    return x


def app_fn(x):
    import random
    from time import sleep

    # print("In act2:", x)
    s = random.randint(1, 30) / 1000.0
    sleep(s)
    return x


def _run_linearity_test(scheduler, size=100, ntimes=1):
    max_ = size

    original_sequence = list(range(max_))

    orig_actor = FuncActor(orig, outports=('x',))
    orig_actor.scheduler = scheduler
    new_actor = FuncActor(app_fn, outports=('x',))
    orig_actor.outports["x"].connect(new_actor.inports["x"])

    for t in range(ntimes):
        for i in range(max_):
            scheduler.put_value(orig_actor.inports["x"], i)

        scheduler.execute()
        new_sequence = list(new_actor.outports['x'].pop_all())

        try:
            assert new_sequence == original_sequence
        except AssertionError:
            print("Received: ", new_sequence)
            print("Expected: ", original_sequence)
            raise


if __name__ == '__main__':
    tests = (
        _run_tree_512_test,
        lambda scheduler: _run_linearity_test(scheduler, size=100, ntimes=5),
    )
    logger.level = 20
    for case in (tests):
        print('testing {}'.format(case))
        for scheduler in (LinearizedScheduler(),
                          IPyClusterScheduler(),
                          FuturesScheduler('distributed', executor_kwargs=dict(uris='192.168.111.23:8786')),
                          FuturesScheduler('ipyparallel', timeout=1),):
            # FuturesScheduler('multiprocessing'), ):
            print('using {}'.format(type(scheduler)))
            case(scheduler)
