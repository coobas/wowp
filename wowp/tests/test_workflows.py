from wowp.actors import FuncActor
from wowp.schedulers import LinearizedScheduler, ThreadedScheduler, NaiveScheduler


def _run_workflow(scheduler):
    import math
    sin = FuncActor(math.sin)
    asin = FuncActor(math.asin)

    asin.inports['inp'] += sin.outports['out']

    wf = sin.get_workflow()

    x = math.pi / 2
    res = scheduler.run_workflow(wf, inp=x)
    assert res['out'].pop() == math.asin(math.sin(x))


def _call_workflow(scheduler):
    import math
    sin = FuncActor(math.sin)
    asin = FuncActor(math.asin)

    asin.inports['inp'] += sin.outports['out']

    wf = sin.get_workflow()

    x = math.pi / 2
    res = wf(scheduler=scheduler, inp=x)
    assert res['out'].pop() == math.asin(math.sin(x))


def test_all_schedulers():
    for scheduler in (ThreadedScheduler(max_threads=8),
                      LinearizedScheduler(),
                      NaiveScheduler()):
        for case in (_call_workflow,
                     _run_workflow):
            yield case, scheduler
