from wowp.actors import FuncActor
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


if __name__ == '__main__':
    nose.run(argv=[__file__, '-vv'])
