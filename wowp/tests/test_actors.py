from wowp.actors import FuncActor, LoopWhile
import nose


def test_FuncActor_return_annotation():
    def func(x, y) -> ('a', 'b'):
        return x+1, y+2
    x, y = 2, 3.1

    fa = FuncActor(func)
    fa.inports.x.put(x)
    fa.inports.y.put(y)

    a, b = func(x, y)

    assert(fa.outports.a.pop() == a)
    assert(fa.outports.b.pop() == b)


def test_FuncActor_call():
    def func(x, y) -> ('a', 'b'):
        return x+1, y+2
    x, y = 2, 3.1

    fa = FuncActor(func)

    assert func(x, y) == fa(x, y)

def test_LoopWhileActor():
    condition = lambda x: x < 10
    def func(x) -> ('x'):
        return x + 1
    fa = FuncActor(func)
    lw = LoopWhile("a_loop", condition)

    fa.inports['x'] += lw.outports['loop_out']
    lw.inports['loop_in'] += fa.outports['x']

    lw.inports['loop_in'].put(0)
    result = lw.outports['final'].pop()

    assert(result == 10)

def test_LoopWhileActorWithInner():
    condition = lambda x: x < 10
    def func(x) -> ('x'):
        return x + 1    
    fa = FuncActor(func)
    lw = LoopWhile("a_loop", condition, inner_actor=fa)

    lw.inports['loop_in'].put(0)
    result = lw.outports['final'].pop()
    assert(result == 10)

if __name__ == '__main__':
    nose.run(argv=[__file__, '-vv'])
