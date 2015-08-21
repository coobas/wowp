from wowp.actors import FuncActor, Switch, ShellRunner
from wowp.schedulers import NaiveScheduler
import nose


def test_FuncActor_return_annotation():
    def func(x, y) -> ('a', 'b'):
        return x + 1, y + 2
    x, y = 2, 3.1

    fa = FuncActor(func)
    fa.inports.x.put(x)
    fa.inports.y.put(y)

    NaiveScheduler().run_actor(fa)

    a, b = func(x, y)

    assert(fa.outports.a.pop() == a)
    assert(fa.outports.b.pop() == b)


def test_FuncActor_call():
    def func(x, y) -> ('a', 'b'):
        return x + 1, y + 2
    x, y = 2, 3.1

    fa = FuncActor(func)

    assert func(x, y) == fa(x, y)


def test_LoopWhileActor():
    def condition(x):
        return x < 10

    def func(x) -> ('x'):
        return x + 1
    fa = FuncActor(func)
    lw = Switch("a_loop", condition)

    fa.inports['x'] += lw.outports['loop_out']
    lw.inports['loop_in'] += fa.outports['x']

    lw.inports['loop_in'].put(0)

    NaiveScheduler().run_actor(lw)

    result = lw.outports['final'].pop()

    assert(result == 10)


def test_LoopWhileActorWithInner():
    def condition(x):
        return x < 10

    def func(x) -> ('x'):
        return x + 1
    fa = FuncActor(func)
    lw = Switch("a_loop", condition, inner_actor=fa)

    lw.inports['loop_in'].put(0)

    NaiveScheduler().run_actor(lw)
    result = lw.outports['final'].pop()
    assert(result == 10)


def test_Shellrunner():
    import platform
    if platform.system() == 'Windows':
        runner = ShellRunner("echo", shell=True)
    else:
        runner = ShellRunner("echo", shell=False)
    runner.inports['inp'].put("test")

    NaiveScheduler().run_actor(runner)

    rvalue = runner.outports['ret'].pop()
    stdout = runner.outports['stdout'].pop()
    stderr = runner.outports['stderr'].pop()

    print("Return value: ", rvalue)
    print("Std out: ", stdout.strip())
    print("Std err: ", stderr.strip())

    assert(rvalue == 0)
    assert(stdout.strip() == "test")
    assert(stderr.strip() == "")

if __name__ == '__main__':
    nose.run(argv=[__file__, '-vv'])
