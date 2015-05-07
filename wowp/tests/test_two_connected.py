from wowp import Actor
from wowp.actors import FuncActor
import nose


def test_two_connected():
	def func1(x) -> ('a'):
		return x * 2

	def func2(a) -> ('y'):
		return a + 3

	actor1 = FuncActor(func1)
	actor2 = FuncActor(func2)

	actor2.inports['a'] += actor1.outports['a']

	for in_value in range(10):
		actor1.inports['x'].put(in_value)
		assert func2(func1(in_value)) == actor2.outports['y'].pop()

if __name__ == '__main__':
    nose.run(argv=[__file__, '-vv'])
