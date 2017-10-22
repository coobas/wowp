from __future__ import absolute_import, division, print_function, unicode_literals
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


def _test_workflow_chain(scheduler, wf_scheduler):
    from wowp.actors import FuncActor
    from wowp.actors.mapreduce import PassWID
    import math
    import random

    sin = FuncActor(math.sin)
    asin = FuncActor(math.asin)
    # first workflow
    asin.inports['inp'] += sin.outports['out']
    wf1 = sin.get_workflow()
    # second workflow
    passwid = PassWID()
    wf2 = passwid.get_workflow()
    # connect the two workflows
    wf2.inports['inp'] += wf1.outports['out']

    wf1.scheduler = wf_scheduler
    wf2.scheduler = wf_scheduler

    inp = random.random()
    scheduler.run_workflow(wf1, inp=inp)
    res1 = wf1.outports['out'].pop_all()
    res2 = wf2.outports['out'].pop_all()
    # wf1 wmpty output
    assert not res1
    # wf2 has output
    assert res2
    assert len(res2) == 1
    assert math.isclose(res2[0]['inp'], inp)


def test_all_schedulers():
    for scheduler in (ThreadedScheduler(max_threads=8),
                      LinearizedScheduler(),
                      NaiveScheduler()):
        for wf_scheduler in (ThreadedScheduler(max_threads=8),
                             LinearizedScheduler(),
                             NaiveScheduler()):
            for case in (_call_workflow,
                         _run_workflow,
                         _test_workflow_chain):
                yield case, scheduler, wf_scheduler
