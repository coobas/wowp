from wowp.actors import FuncActor
from wowp.schedulers import FuturesScheduler
from wowp.util import ConstructorWrapper
from wowp.actors.mapreduce import Map
import pickle
import sys
import os
import datetime


def instat_log_file(filename, msg, mode="w"):
    now = str(datetime.datetime.now())
    with open(filename, mode) as f_out:
        f_out.write(now + '\n' + msg)


print('$@^%$@ test_job (^@*&%')
print(sys.executable)
profile_dirs = os.getenv('WOWP_IPY_PROFILE_DIRS', '')
profile_dirs = profile_dirs.split(os.pathsep)
print('test_job profile_dirs: {}'.format(profile_dirs))

instat_log_file('test_job_start', 'test_job profile_dirs: {}'.format(profile_dirs))

# from ipyparallel import Client
# Client(*(), **{u'profile_dir': '/net/uja/scratch/tmp/_profile_testid_0'})

# from ipyparallel import Client
# cli = Client(*(), **{u'profile_dir': '/net/uja/scratch/tmp/_profile_testid_0'})
# print(cli)

# print('yes')
# sys.exit(0)



def sleep_n_info(inp, maxsecs=2):
    from time import sleep, time
    from os import getpid
    from platform import node
    import random

    start = time()
    secs = maxsecs * random.random()
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

print('... scheduler')
f_log = open('test_job_file.log', 'a')
f_log.write(str(datetime.datetime.now()) + "... scheduler ...\n")
instat_log_file('test_job_before_scheduler', "scheduler: {}".format(profile_dirs), mode="w")
scheduler = FuturesScheduler('ipyparallel', min_engines=45, timeout=1000,
                             executor_kwargs={'profile_dirs': profile_dirs})

instat_log_file('test_job_after_scheduler', "... scheduler here ...", mode="w")
wow_map = Map(TestActor, scheduler=scheduler)

f_log.write(str(datetime.datetime.now()) + "=-=# running #=-\n")

res = wow_map(inp=list(range(48 * 8)))
instat_log_file('test_job_res', str(res), mode="w")

print(res)
pickle.dump(res, open('res_pickle', 'w'))

print('--end--')
