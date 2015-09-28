from wowp.actors.experimental import Splitter
from wowp.schedulers import NaiveScheduler


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
