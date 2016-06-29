from wowp.actors import FuncActor
from wowp.util import ConstructorWrapper
from wowp.actors.mapreduce import Map


def sleep_n_info(inp, maxsecs=2):
    from time import sleep, time
    from os import getpid
    from platform import node
    import random

    start = time()
    secs = maxsecs * random.random()
    print("sleep({})".format(secs))
    sleep(secs)

    res = {'pid': getpid(),
           'sleep': secs,
           'node': node(),
           'start': start,
           'inp': inp,
           'end': time()}

    return res


TestActor = ConstructorWrapper(FuncActor,
                               sleep_n_info,
                               inports=('inp', ),
                               outports=('out', ))

wowp_map = Map(TestActor)

WORKFLOW = wowp_map.get_workflow()
