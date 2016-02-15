from __future__ import absolute_import, division, print_function, unicode_literals
from wowp.actors.experimental import Splitter, Chain
from wowp.schedulers import NaiveScheduler
from wowp.actors import FuncActor
from wowp.util import ConstructorWrapper


def test_splitter():
    splitter = Splitter(multiplicity=2, inport_name="x")
    assert len(splitter.outports) == 2

    scheduler = NaiveScheduler()
    for i in range(0, 10):
        scheduler.put_value(splitter.inports.x, i)
    scheduler.execute()

    x1_all = list(splitter.outports["x_1"].pop_all())
    x2_all = list(splitter.outports["x_2"].pop_all())

    print("x1:", x1_all)
    print("x2:", x2_all)

    assert [0, 2, 4, 6, 8] == x1_all
    assert [1, 3, 5, 7, 9] == x2_all

def double_me(x):
    return x * 2

def test_chain():
    func_generator = ConstructorWrapper(FuncActor, double_me)
    chain = Chain("func_chain", [func_generator, func_generator])
    wf = chain.get_workflow()
    res = wf(input = 4)
    assert res["output"].pop() == 16
    res = wf(input = 2)
    assert res["output"].pop() == 8
    res = wf(input = "a")
    assert res["output"].pop() == "aaaa"