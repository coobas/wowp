from wowp.actors import FuncActor
from wowp.schedulers import LinearizedScheduler, ThreadedScheduler, NaiveScheduler
import nose


def _run_workflow(scheduler, wf_scheduler):

    if (isinstance(scheduler, ThreadedScheduler) or
    isinstance(wf_scheduler, ThreadedScheduler)):
        # skip temporarily
        raise nose.SkipTest

    import math
    sin = FuncActor(math.sin)
    asin = FuncActor(math.asin)

    asin.inports['inp'] += sin.outports['out']

    wf = sin.get_workflow()
    wf.scheduler = wf_scheduler

    x = math.pi / 2
    scheduler.run_workflow(wf, inp=x)
    res = {port.name: port.pop_all() for port in wf.outports}
    assert res['out'].pop() == math.asin(math.sin(x))


def _call_workflow(scheduler, wf_scheduler):

    if (isinstance(scheduler, ThreadedScheduler) or
    isinstance(wf_scheduler, ThreadedScheduler)):
        # skip temporarily
        raise nose.SkipTest

    import math
    sin = FuncActor(math.sin)
    asin = FuncActor(math.asin)

    asin.inports['inp'] += sin.outports['out']

    wf = sin.get_workflow()
    wf.scheduler = wf_scheduler

    x = math.pi / 2
    res = wf(scheduler=scheduler, inp=x)
    assert res['out'].pop() == math.asin(math.sin(x))


def test_all_schedulers():
    for scheduler in (ThreadedScheduler(max_threads=8),
                      LinearizedScheduler(),
                      NaiveScheduler()):
        for wf_scheduler in (ThreadedScheduler(max_threads=8),
                          LinearizedScheduler(),
                          NaiveScheduler()):
            for case in (_call_workflow,
                         _run_workflow):
                yield case, scheduler, wf_scheduler
