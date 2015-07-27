from wowp.actors import FuncActor, LoopWhile
from wowp.schedulers import LinearizedScheduler, ThreadedScheduler, IPyClusterScheduler
import random
from time import sleep


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


def orig(x) -> ('x'):
    # print("In act1:", x)
    return x

def app_fn(x) -> ('x'):
    # print("In act2:", x)
    s = random.randint(1, 30) / 1000.0
    sleep(s)
    return x

def _run_linearity_test(scheduler):

    max_ = 100

    original_sequence = list(range(max_))

    orig_actor = FuncActor(orig)
    orig_actor.scheduler = scheduler
    new_actor = FuncActor(app_fn)
    orig_actor.outports["x"].connect(new_actor.inports["x"])

    for i in range(max_):
        scheduler.put_value(orig_actor.inports["x"], i)

    scheduler.execute()
    new_sequence = list(new_actor.outports['x'].pop_all())

    print("Received: ", new_sequence)
    print("Expected: ", original_sequence)

    assert new_sequence == original_sequence


if __name__ == '__main__':
    for case in (_run_tree_512_test, _run_linearity_test, ):
        print('testing {}'.format(case))
        scheduler = IPyClusterScheduler()
        case(scheduler)
